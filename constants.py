# Predefined options for dropdown menus

FORM_OPTIONS = [
    'tablet', 'solution', 'injection', 'capsule', 'syrup', 'ampule', 'drops',
    'suspension', 'powder', 'cream', 'ointment', 'patch', 'gas', 'vial', 'NA'
]

ROUTE_OPTIONS = [
    'oral', 'intravenous', 'intramuscular', 'subcutaneous',
    'topical', 'inhalation', 'rectal', 'sublingual', 'NA'
]

FREQUENCY_OPTIONS = [
    '1r/day', '2r/day', '3r/day', '4r/day', 
    'every 4 hours', 'every 6 hours', 'every 8 hours', 
    'as needed', 'at bedtime',
    'before meals', 'after meals', 'with meals',
    'before breakfast', 'after breakfast',
    'before lunch', 'after lunch',
    'before dinner', 'after dinner', 'NA'
]

DOSAGE_UNITS = ['mg', 'g', 'mcg', 'mL', 'L', 'IU', 'mmol', 'mEq', '%', 'NA']

DB_PATH = 'stroke_drugs.db'
STATUS_IN_PROCESS = 'in process'
STATUS_COMPLETED = 'completed'
STATUS_CHECKED = 'checked'
EXPIRED_ANNOTATION_THRESHOLD = 30