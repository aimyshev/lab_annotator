import streamlit as st
import pandas as pd
import re

# Input text (you can replace it with your data source)
raw_text = """
ОАК от 09.01.14. НВ 137 г/л, эр. 4,2*10 г/л, л. 18,6 *10, СОЭ 15 мм/ч, п/я 7, с/я 80, мон 1, лимф 12..
ОАК от 13.01.14. НВ 133 г/л, эр. 4,0*10 г/л, л. 10,4*10, СОЭ 26 мм/ч, п/я 9, с/я 58, мон 0, лимф 27.
ОАМ 10.01.14 г. Цвет с/ж, отн.плотность 1020, белок- 0,099, сахар- полож, эпит.1-2 в п/зр, лейк -2-3 в п/з, эр 1-2.
ОАМ 23.01.14 г. Цвет с/ж, отн.плотность 1020, белок- 0,033, сахар- отр., эпит.1-2 в п/зр, лейк -3-4 в п/з.
"""

# Patterns for different analysis types
oak_pattern = r"ОАК от (\d{2}\.\d{2}\.\d{2}).*?НВ (\d+) г/л, эр\. ([\d,]+).*?СОЭ (\d+) мм/ч.*?п/я (\d+).*?с/я (\d+).*?мон (\d+).*?лимф (\d+)"
oam_pattern = r"ОАМ (\d{2}\.\d{2}\.\d{2}) г\. Цвет (.*?), отн\.плотность (\d+), белок- ([\d,.]+), сахар- (\w+), эпит\.(\d+-\d+) .*?лейк -(\d+-\d+) .*?эр (\d+-\d+)"

# Extract data
oak_data = re.findall(oak_pattern, raw_text)
oam_data = re.findall(oam_pattern, raw_text)

# Convert to DataFrames
oak_df = pd.DataFrame(
    oak_data,
    columns=["Date", "НВ (г/л)", "Эр. (10¹²/л)", "СОЭ (мм/ч)", "П/Я (%)", "С/Я (%)", "Мон (%)", "Лимф (%)"]
)

oam_df = pd.DataFrame(
    oam_data,
    columns=["Date", "Color", "Density", "Protein (г/л)", "Sugar", "Epithelial Cells", "Leukocytes", "Erythrocytes"]
)

# Streamlit interface
st.title("Medical Data Extraction")

# Display OAK data
st.header("ОАК (Общий Анализ Крови)")
if not oak_df.empty:
    st.dataframe(oak_df)
else:
    st.write("No ОАК data found.")

# Display OAM data
st.header("ОАМ (Общий Анализ Мочи)")
if not oam_df.empty:
    st.dataframe(oam_df)
else:
    st.write("No ОАМ data found.")
