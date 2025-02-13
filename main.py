import streamlit as st
import pandas as pd
from typing import Optional, Tuple, List
from sqlalchemy import text, create_engine
import logging
from db_utils import (
    AnnotationManager, 
    DatabaseManager, 
    DataManager,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LabAnalysisUI:
    """Main UI class for Lab Analysis Application"""
    
    ANALYSIS_TABLES = {
        "alcohol_test": "alcohol_test_values",
        "biochem_blood_test": "biochem_blood_test_values",
        "blood_electrolytes": "blood_electrolytes_values",
        "blood_sugar": "blood_sugar_values",
        "blood_test": "blood_test_values",
        "blood_type": "blood_type_values",
        "cerebrospinal_fluid_analysis": "cerebrospinal_fluid_analysis_values",
        "coagulogram": "coagulogram_values",
        "fecal_analysis": "fecal_analysis_values",
        "hematocrit": "hematocrit_values",
        "hiv_test": "hiv_test_values",
        "malaria_test": "malaria_test_values",
        "sputum_analysis": "sputum_analysis_values",
        "syphilis_test": "syphilis_test_values",
        "urinalysis": "urinalysis_values",
    }

    HIDDEN_COLUMNS = ['id', 'doc_id', 'annotation_source']

    def __init__(self):        
        self.db_manager = DatabaseManager()
        self.data_manager = DataManager(self.db_manager)
        self.annotation_manager = AnnotationManager(self.db_manager)
        self._initialize_session_state()
        self._load_table_schemas()

    def _initialize_session_state(self):
        defaults = {
            'current_doc_id': None,
            'username': "",
            **{f"table_data_{table}": None for table in self.ANALYSIS_TABLES}
        }

        for key, default in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default
        
    def cleanup_session_state(self):
        st.session_state.current_doc_id = None
        for table_key in [f"table_data_{table}" for table in self.ANALYSIS_TABLES]:
            st.session_state[table_key] = None

    def _load_table_schemas(self):
        with self.db_manager.transaction() as conn:
            schemas = {}
            for header_table, values_table in self.ANALYSIS_TABLES.items():
                header_query = text(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = :table_name
                    AND column_name NOT IN ('id', 'doc_id', 'annotation_source')
                    ORDER BY ordinal_position;
                """)

                header_result = conn.execute(header_query, {"table_name": header_table})
                schemas[header_table] = {
                    row[0]: {
                        'type': row[1],
                        'nullable': row[2] == "YES",
                        'default': row[3]
                    } for row in header_result
                }

                # Get values table schema
                values_query = text("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = :table_name
                    AND column_name NOT IN ('id', 'test_id')
                    ORDER BY ordinal_position;
                """)
                
                values_result = conn.execute(values_query, {"table_name": values_table})
                schemas[values_table] = {
                    row[0]: {
                        'type': row[1],
                        'nullable': row[2] == "YES",
                        'default': row[3]
                    } for row in values_result
                }
            
            st.session_state.table_schemas = schemas
    


    def get_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the combined schema"""
        return pd.DataFrame(columns=["test_date", "analysis_order", "parameter_name", "value", "unit"])

    def prepare_combined_data(self, header_df: pd.DataFrame, values_df: pd.DataFrame) -> pd.DataFrame:
        """Combine header and values data into a single DataFrame"""
        if header_df.empty:
            return self.get_empty_dataframe()
            
        # If we have header data but no values, create empty values
        if values_df.empty:
            combined_df = self.get_empty_dataframe()
            if not header_df.empty:
                combined_df['test_date'] = header_df['test_date'].iloc[0]
                combined_df['analysis_order'] = header_df['analysis_order'].iloc[0]
            return combined_df

        # Combine header and values data
        combined_df = values_df.copy()
        combined_df['test_date'] = header_df['test_date'].iloc[0]
        combined_df['analysis_order'] = header_df['analysis_order'].iloc[0]
        
        # Reorder columns
        return combined_df[["test_date", "analysis_order", "parameter_name", "value", "unit"]]

    def split_combined_data(self, combined_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Split combined DataFrame back into header and values DataFrames"""
        if combined_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Extract header data
        header_df = pd.DataFrame({
            'test_date': [combined_df['test_date'].iloc[0]],
            'analysis_order': [combined_df['analysis_order'].iloc[0]]
        })

        # Extract values data
        values_df = combined_df[['parameter_name', 'value', 'unit']].copy()
        
        return header_df, values_df
    
    def save_table_data(self, header_table: str, values_table: str, combined_df: pd.DataFrame, doc_id: str):
        """Save the combined data, ensuring ground truth is saved first and then updates are allowed."""
        
        header_df, values_df = self.split_combined_data(combined_df)

        with self.db_manager.transaction() as conn:
            # Check for an existing "ground truth" annotation
            check_gt_query = text(f"""
                SELECT id FROM {header_table} 
                WHERE doc_id = :doc_id AND annotation_source = 'ground_truth'
            """)
            existing_gt_header = conn.execute(check_gt_query, {"doc_id": doc_id}).scalar()

            # Check for an existing "GPT" annotation
            check_gpt_query = text(f"""
                SELECT id FROM {header_table} 
                WHERE doc_id = :doc_id AND annotation_source = 'gpt'
            """)
            existing_gpt_header = conn.execute(check_gpt_query, {"doc_id": doc_id}).scalar()

            if not existing_gt_header:
                # No ground truth exists, so insert new GT annotation
                header_df_to_save = header_df.copy()
                header_df_to_save['doc_id'] = doc_id
                header_df_to_save['annotation_source'] = "ground_truth"

                header_columns = ['doc_id', 'test_date', 'analysis_order', 'annotation_source']
                insert_header_query = f"""
                    INSERT INTO {header_table} ({', '.join(header_columns)})
                    VALUES ({', '.join([f':{col}' for col in header_columns])})
                    RETURNING id
                """
                result = conn.execute(text(insert_header_query), header_df_to_save.to_dict('records'))
                test_id = int(result.scalar())  # Get newly inserted test ID
            else:
                # Ground truth exists, update instead of inserting
                test_id = existing_gt_header
                update_header_query = text(f"""
                    UPDATE {header_table}
                    SET test_date = :test_date, analysis_order = :analysis_order
                    WHERE id = :test_id
                """)
                conn.execute(update_header_query, {
                    "test_date": header_df['test_date'].iloc[0],
                    "analysis_order": int(header_df['analysis_order'].iloc[0]),
                    "test_id": test_id
                })

            # Ensure that values are always updated for GT annotation
            delete_values_query = text(f"DELETE FROM {values_table} WHERE test_id = :test_id")
            conn.execute(delete_values_query, {"test_id": test_id})

            # Insert new values
            if not values_df.empty:
                values_df_to_save = values_df.copy()
                values_df_to_save['test_id'] = test_id  # Associate with the correct test ID

                values_columns = ['test_id', 'parameter_name', 'value', 'unit']
                insert_values_query = f"""
                    INSERT INTO {values_table} ({', '.join(values_columns)})
                    VALUES ({', '.join([f':{col}' for col in values_columns])})
                """
                conn.execute(text(insert_values_query), values_df_to_save.to_dict('records'))


    def setup_page(self):
        """Configure the page layout and style"""
        st.set_page_config(
            page_title="Lab Analysis Annotation",
            page_icon="🔬",
            layout="wide"
        )
        st.title("Lab Analysis Annotation Interface")
        
        # Add custom CSS
        st.markdown("""
            <style>
            .stButton>button {
                width: 100%;
            }
            .success-message {
                color: #0f5132;
                background-color: #d1e7dd;
                padding: 1rem;
                border-radius: 0.25rem;
            }
            </style>
            """, unsafe_allow_html=True)

    def check_authentication(self):
        username = st.text_input("Username", value=st.session_state.username)

        if username:
            st.session_state.username = username
            return True
        
        st.warning("Please enter a username to start annotating.")
        return False
    
    def fetch_document(self) -> Optional[Tuple[str, str]]:

        if st.session_state.current_doc_id is not None:
            return st.session_state.current_doc_id, st.session_state.document_text

        try:
            document = self.annotation_manager.fetch_unannotated_doc()
            if not document:
                st.warning("📝 No documents available for annotation.")
                return None
            
            doc_id, document_body = document
            st.session_state.current_doc_id = doc_id
            return doc_id, document_body
            
        except Exception as e:
            logger.error(f"Error fetching document: {str(e)}")
            st.error("Failed to fetch document. Please try again later.")
            return None

    def display_document(self, document_body: str):
        """Display the document text with formatting"""
        with st.expander("Raw Lab Text", expanded=True):
            st.sidebar.text_area(
                "Raw Text",
                value=document_body,
                height=500,
                disabled=True,
                key="document_text"
            )

    @st.fragment
    def handle_table_operations(self, header_table: str, values_table: str, doc_id: str):
        """Handle operations for a single analysis table pair"""
        with st.expander(f"{header_table.replace('_', ' ').title()}", expanded=True):
            try:
                table_key = f"table_data_{header_table}"

                # Initialize or fetch table data
                if st.session_state[table_key] is None:
                    header_df, values_df = self.data_manager.fetch_lab_data(doc_id, header_table, values_table)
                    combined_df = self.prepare_combined_data(header_df, values_df)
                    st.session_state[table_key] = combined_df

                # Display combined editor
                edited_df = st.data_editor(
                    st.session_state[table_key],
                    key=f"editor_{header_table}",
                    use_container_width=True,
                    num_rows="dynamic",
                    column_config={
                        "test_date": st.column_config.TextColumn(
                            "Test Date",
                            help="Date when the test was performed",
                        ),
                        "analysis_order": st.column_config.NumberColumn(
                            "Order",
                            help="Analysis order number",
                            min_value=1,
                        ),
                        "parameter_name": st.column_config.TextColumn(
                            "Parameter",
                            help="Name of the test parameter",
                        ),
                        "value": st.column_config.TextColumn(
                            "Value",
                            help="Measured value",
                        ),
                        "unit": st.column_config.TextColumn(
                            "Unit",
                            help="Measurement unit",
                        ),
                    }
                )

                if st.button("💾 Save Changes", key=f"save_{header_table}"):
                    try:
                        st.session_state[table_key] = edited_df
                        self.save_table_data(
                            header_table,
                            values_table, 
                            edited_df,
                            doc_id
                        )
                        st.success("Changes saved successfully!")
                    except Exception as e:
                        logger.error(f"Error saving changes: {str(e)}")
                        st.error("Failed to save changes. Please try again.")

            except Exception as e:
                logger.error(f"Error handling table {header_table}: {str(e)}")
                st.error(f"Error processing {header_table}. Please try again.")

    def show_navigation_controls(self, doc_id):
        """Display navigation and control buttons"""
        st.markdown("---")
        cols = st.columns([1, 1])
        
        with cols[0]:
            if st.button("Save Annotation"):
                self.cleanup_session_state()
                st.success("Annotation saved successfully!")
                st.session_state.current_doc_id = None
                st.rerun()

        with cols[1]:
            if st.button("⏭️ Next Document", use_container_width=True):
                self.cleanup_session_state()
                st.success("Moving to next document...")
                st.session_state.current_doc_id = None
                st.rerun()

    def run(self):
        """Main application loop"""
        self.setup_page()

        if not self.check_authentication():
            return

        document = self.fetch_document()
        if not document:
            return
            
        doc_id, document_body = document
        self.display_document(document_body)
        
        for header_table, values_table in self.ANALYSIS_TABLES.items():
            self.handle_table_operations(header_table, values_table, doc_id)
            
        self.show_navigation_controls(doc_id)

def main():
    try:
        app = LabAnalysisUI()
        app.run()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error("An unexpected error occurred. Please refresh the page.")

if __name__ == "__main__":
    main()