import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import logging
from constants import STATUS_COMPLETED, STATUS_IN_PROCESS, STATUS_CHECKED, EXPIRED_ANNOTATION_THRESHOLD
from typing import List, Optional, Dict, Tuple
from contextlib import contextmanager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


load_dotenv()

class DatabaseConfig:
    def __init__(self):
        self.user = os.getenv("user")
        self.password = os.getenv("password")
        self.host = os.getenv("host")
        self.port = os.getenv("port")
        self.dbname = os.getenv("dbname")

        if not all([self.user, self.password, self.host, self.port, self.dbname]):
            raise ValueError("Missing required database configuration. Check environment variables.")
    
    @property
    def connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

class DatabaseManager:
    def __init__(self):
        self.config = DatabaseConfig()
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(
                f"{self.config.connection_string}?options=-csearch_path=public",
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10
            )
        return self._engine
    
    @contextmanager
    def transaction(self):
        """Simplified transaction context manager"""
        with self.engine.begin() as conn:
            yield conn

class AnnotationManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def fetch_unannotated_doc(self):
        try:
            with self.db_manager.transaction() as conn:
                cleanup_query = text("""
                    SET TIME ZONE 'UTC-5';
                    DELETE FROM public.annotations 
                    WHERE status = :status 
                        AND time::timestamp < NOW() - :threshold * INTERVAL '1 minute'
                    """)
                conn.execute(cleanup_query, {
                    'status': STATUS_IN_PROCESS, 
                    'threshold': EXPIRED_ANNOTATION_THRESHOLD})
                
                # Fetch unannotated document
                select_query = text("""
                    WITH next_doc AS (
                        SELECT doc_id, body
                        FROM (
                            SELECT d.doc_id, d.body
                            FROM public.documents d
                            WHERE NOT EXISTS (
                                SELECT 1 FROM public.annotations a WHERE d.doc_id = a.doc_id
                            )
                            ORDER BY d.doc_id
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        ) subquery
                    )
                    INSERT INTO public.annotations (doc_id, status, time)
                    SELECT doc_id, :status, :time
                    FROM next_doc
                    RETURNING doc_id, (SELECT body FROM next_doc)
                """)
                
                result = conn.execute(select_query, {
                        "status": STATUS_IN_PROCESS, 
                        "time": datetime.now().isoformat()})

                return result.fetchone()
        
        except Exception as e:
            logger.error(f"Error fetching unannotated document: {str(e)}")
            raise

    def fetch_annotated_doc(self):
        try:
            with self.db_manager.transaction() as conn:
                select_query = text("""
                    SELECT a.doc_id, d.body
                    FROM public.annotations a
                    JOIN public.documents d ON d.doc_id = a.doc_id
                    WHERE a.status = :status
                    LIMIT 1
                """)
                result = conn.execute(select_query, {"status": STATUS_COMPLETED})
                return result.fetchone()
        except Exception as e:
            logger.error(f"Error fetching annotated document: {str(e)}")
            raise

    def save_annotation(self, doc_id, username):
        with self.db_manager.transaction() as conn:

            update_query = text("""
                UPDATE public.annotations
                SET status = :status,
                    username = :username, 
                    save_time = :save_time
                WHERE doc_id = :doc_id 
                    AND status = :old_status
            """)

            conn.execute(update_query, {
                "status": STATUS_COMPLETED,
                "username": username, 
                "save_time": datetime.now().isoformat(),
                "doc_id": doc_id, 
                "old_status": STATUS_IN_PROCESS
            })
    
    def save_annotated_doc(self, doc_id, username):
        try:
            with self.db_manager.transaction() as conn:
                update_query = text("""
                    UPDATE public.annotations
                    SET status = :status,
                        username = :username, 
                        save_time = :save_time
                    WHERE doc_id = :doc_id
                """)
                conn.execute(update_query, {
                    "doc_id": doc_id,
                    "status": STATUS_CHECKED,
                    "save_time": datetime.now().isoformat(),
                    "username": username
                })
        except Exception as e:
            logger.error(f"Error saving annotated document: {str(e)}")
            raise
        
    def mark_document_in_process(self, doc_id, username):
        """Mark document as being processed"""
        with self.db_manager.transaction() as conn:
            # Check if annotation already exists
            check_query = text("""
                SELECT id FROM public.annotations
                WHERE doc_id = :doc_id AND username = :username
            """)
            
            existing = conn.execute(check_query, {
                "doc_id": doc_id,
                "username": username
            }).scalar()
            
            if not existing:
                # Create new annotation record as in_process
                insert_query = text("""
                    INSERT INTO public.annotations (doc_id, username, status, save_time)
                    VALUES (:doc_id, :username, 'in_process', :save_time)
                """)
                
                conn.execute(insert_query, {
                    "doc_id": doc_id,
                    "username": username,
                    "save_time": datetime.now().isoformat()
                })
                
class DataManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def fetch_lab_data(self, doc_id: str, header_table: str, values_table: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Fetch both header and values data for a lab test"""
        with self.db_manager.transaction() as conn:
            # Fetch header data
            header_query = text(f"""
                SELECT * FROM public.{header_table} 
                WHERE doc_id = :doc_id AND
                annotation_source = :annotation_source
            """)
            header_df = pd.read_sql_query(header_query, conn, params={"doc_id": doc_id, "annotation_source": "gpt"})

            # Fetch values data if header exists
            values_df = pd.DataFrame()
            if not header_df.empty:
                test_id = header_df['id'].iloc[0]
                test_id = int(test_id)
                values_query = text(f"""
                    SELECT * FROM public.{values_table}
                    WHERE test_id = :test_id
                """)
                values_df = pd.read_sql_query(values_query, conn, params={"test_id": test_id})

            return header_df, values_df
    
    def check_db_structure(self):
        with self.db_manager.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            tables = cursor.fetchall()
            print("Existing tables:", tables)
            
            for table in tables:
                cursor.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = %s
                    """, (table[0],))
                columns = cursor.fetchall()
                print(f"\nColumns in {table[0]}:")
                for column in columns:
                    print(column)

if __name__ == "__main__":
    check_db_structure()