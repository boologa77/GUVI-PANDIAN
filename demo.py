import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import streamlit as st

# Establishing connection to the MSSQL database using SQLAlchemy
def connect_to_db():
    server = 'localhost'
    database = 'OMG_DB'
    connection_string = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string)
    return engine

engine = connect_to_db()

# SQL Queries
queries = {
    "total_population_per_district": "SELECT District, SUM(TRY_CAST(Population AS FLOAT)) as total_population FROM processeddata GROUP BY District",
    "literate_males_females_per_district": "SELECT District, SUM(TRY_CAST(Literate_Male AS FLOAT)) as literate_males, SUM(TRY_CAST(Literate_Female AS FLOAT)) as literate_females FROM processeddata GROUP BY District",
    "percentage_of_workers_per_district": "SELECT District, (SUM(TRY_CAST(Workers AS FLOAT)) * 100.0 / SUM(TRY_CAST(Population AS FLOAT))) as worker_percentage FROM processeddata GROUP BY District",
    "households_with_LPG_PNG_per_district": "SELECT District, SUM(TRY_CAST(LPG_or_PNG_Households AS FLOAT)) as households_with_LPG_PNG FROM processeddata GROUP BY District",
    "religious_composition_per_district": "SELECT District, [State/UT], Hindus, Muslims, Christians, Sikhs, Buddhists, Jains, Others_Religions, Religion_Not_Stated FROM processeddata",
    "households_with_internet_per_district": "SELECT District, SUM(TRY_CAST(Households_with_Internet AS FLOAT)) as households_with_internet FROM processeddata GROUP BY District",
    "educational_attainment_per_district": "SELECT District, Below_Primary_Education, Primary_Education, Middle_Education, Secondary_Education, Higher_Education, Graduate_Education, Other_Education FROM processeddata",
    "households_with_transportation_modes_per_district": "SELECT District, SUM(TRY_CAST(Households_with_Bicycle AS FLOAT)) as bicycles, SUM(TRY_CAST(Households_with_Car_Jeep_Van AS FLOAT)) as cars, SUM(TRY_CAST(Households_with_Radio_Transistor AS FLOAT)) as radios, SUM(TRY_CAST(Households_with_Television AS FLOAT)) as televisions FROM processeddata GROUP BY District",
    "condition_of_census_houses_per_district": "SELECT District, SUM(TRY_CAST(Condition_of_occupied_census_houses_Dilapidated_Households AS FLOAT)) as dilapidated, SUM(TRY_CAST(Households_with_separate_kitchen_Cooking_inside_house AS FLOAT)) as separate_kitchen, SUM(TRY_CAST(Having_bathing_facility_Total_Households AS FLOAT)) as bathing_facility, SUM(TRY_CAST(Having_latrine_facility_within_the_premises_Total_Households AS FLOAT)) as latrine_facility FROM processeddata GROUP BY District",
    "household_size_distribution_per_district": "SELECT District, SUM(TRY_CAST(Household_size_1_person_Households AS FLOAT)) as size_1, SUM(TRY_CAST(Household_size_2_persons_Households AS FLOAT)) as size_2, SUM(TRY_CAST(Household_size_3_to_5_persons_Households AS FLOAT)) as size_3_5, SUM(TRY_CAST(Household_size_6_8_persons_Households AS FLOAT)) as size_6_8, SUM(TRY_CAST(Household_size_9_persons_and_above_Households AS FLOAT)) as size_9_above FROM processeddata GROUP BY District",
    "total_households_per_state": "SELECT [State/UT], SUM(TRY_CAST(Households AS FLOAT)) as total_households FROM processeddata GROUP BY [State/UT]",
    "households_with_latrine_per_state": "SELECT [State/UT], SUM(TRY_CAST(Having_latrine_facility_within_the_premises_Total_Households AS FLOAT)) as households_with_latrine FROM processeddata GROUP BY [State/UT]",
    "average_household_size_per_state": "SELECT [State/UT], AVG(TRY_CAST(Household_size_1_to_2_persons AS FLOAT)) as avg_household_size FROM processeddata GROUP BY [State/UT]",
    "owned_vs_rented_households_per_state": "SELECT [State/UT], SUM(TRY_CAST(Ownership_Owned_Households AS FLOAT)) as owned, SUM(TRY_CAST(Ownership_Rented_Households AS FLOAT)) as rented FROM processeddata GROUP BY [State/UT]",
    "distribution_of_latrine_types_per_state": "SELECT [State/UT], SUM(TRY_CAST(Type_of_latrine_facility_Pit_latrine_Households AS FLOAT)) as pit_latrine, SUM(TRY_CAST(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households AS FLOAT)) as flush_latrine, SUM(TRY_CAST(Type_of_latrine_facility_Other_latrine_Households AS FLOAT)) as other_latrine FROM processeddata GROUP BY [State/UT]",
    "households_with_drinking_water_per_state": "SELECT [State/UT], SUM(TRY_CAST(Location_of_drinking_water_source_Near_the_premises_Households AS FLOAT)) as households_with_drinking_water FROM processeddata GROUP BY [State/UT]",
    "average_household_income_per_state": "SELECT [State/UT], AVG(TRY_CAST(Power_Parity_Rs_45000_150000 AS FLOAT)) as avg_income FROM processeddata GROUP BY [State/UT]",
    "married_couples_with_household_sizes_per_state": "SELECT [State/UT], SUM(TRY_CAST(Married_couples_1_Households AS FLOAT)) as married_1, SUM(TRY_CAST(Married_couples_2_Households AS FLOAT)) as married_2, SUM(TRY_CAST(Married_couples_3_Households AS FLOAT)) as married_3, SUM(TRY_CAST(Married_couples_3_or_more_Households AS FLOAT)) as married_3_or_more FROM processeddata GROUP BY [State/UT]",
    "households_below_poverty_line_per_state": "SELECT [State/UT], COUNT(*) as households_below_poverty_line FROM processeddata WHERE Power_Parity_Less_than_Rs_45000 = 1 GROUP BY [State/UT]",
    "overall_literacy_rate_per_state": "SELECT [State/UT], (SUM(TRY_CAST(Literate AS FLOAT)) * 100.0 / SUM(TRY_CAST(Population AS FLOAT))) as literacy_rate FROM processeddata GROUP BY [State/UT]"
}

# Fetch data from MS SQL Server
data = {key: pd.read_sql_query(text(query), engine) for key, query in queries.items()}

# Streamlit App
st.title('Census Data')

for key, df in data.items():
    st.subheader(key.replace('_', ' ').title())
    
    # Display the DataFrame as a table
    st.dataframe(df)
