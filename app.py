import streamlit as st
import pandas as pd
from snowflake.snowpark import Session  # ✅ Correct import
from datetime import datetime

# ✅ Ensure `st.set_page_config()` is the first Streamlit command
st.set_page_config(
    page_title="Editable Portfolio Performance Data",
    page_icon="📊",
    layout="wide"
)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Editable Portfolio Performance Data</h1>", unsafe_allow_html=True)

# ✅ Snowflake connection parameters from Streamlit secrets
try:
    connection_parameters = {
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        "database": st.secrets["SNOWFLAKE_DATABASE"],
        "schema": st.secrets["SNOWFLAKE_SCHEMA"],
    }

    # ✅ Create a Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    st.success("✅ Successfully connected to Snowflake!")

except Exception as e:
    st.error(f"❌ Failed to connect to Snowflake: {e}")
    st.stop()  # Stop execution if connection fails

# Function to fetch data from Snowflake
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"❌ Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch override reference data
def fetch_override_ref_data(selected_module=None, selected_table=None):
    try:
        df = session.table("Override_Ref").to_pandas()
        df.columns = [col.upper() for col in df.columns]

        if selected_module:
            module_num = int(selected_module.split('-')[1])
            df = df[df['MODULE'] == module_num]
            
            if selected_table:
                df = df[df['SOURCE_TABLE'] == selected_table]
                
        return df
    except Exception as e:
        st.error(f"❌ Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Function to update a row in the source table
def update_source_table_row(source_table, as_of_date, portfolio, portfolio_segment, category, description, market_value):
    try:
        update_sql = f"""
            UPDATE {source_table}
            SET
                MARKET_VALUE = '{market_value}',
                DESCRIPTION = '{description}'
            WHERE
                AS_OF_DATE = '{as_of_date}' AND
                PORTFOLIO = '{portfolio}' AND
                PORTFOLIO_SEGMENT = '{portfolio_segment}' AND
                CATEGORY = '{category}'
        """
        session.sql(update_sql).collect()
        st.success(f"✅ Successfully updated row in {source_table}")

    except Exception as e:
        st.error(f"❌ Error updating row in {source_table}: {e}")

# Function to insert a row in the target table with RECORD_FLAG = 'O'
def insert_into_target_table(target_table, as_of_date, portfolio, portfolio_segment, category, description, market_value):
    try:
        formatted_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_sql = f"""
            INSERT INTO {target_table} (AS_OF_DATE, PORTFOLIO, PORTFOLIO_SEGMENT, CATEGORY, DESCRIPTION, MARKET_VALUE, AS_AT_DATE, RECORD_FLAG)
            VALUES ('{as_of_date}', '{portfolio}', '{portfolio_segment}', '{category}', '{description}', '{market_value}', '{formatted_ts}', 'O')
        """
        session.sql(insert_sql).collect()
        st.success(f"✅ Successfully inserted data into {target_table} with RECORD_FLAG = 'O'")

    except Exception as e:
        st.error(f"❌ Error inserting values into {target_table}: {e}")

# Function to update and insert data
def update_and_insert(source_table, target_table_name, edited_df, original_df):
    try:
        edited_rows = edited_df[(edited_df != original_df).any(axis=1)]
        for index, row in edited_rows.iterrows():
            as_of_date = row["AS_OF_DATE"].strftime("%Y-%m-%d")
            portfolio = row["PORTFOLIO"]
            portfolio_segment = row["PORTFOLIO_SEGMENT"]
            category = row["CATEGORY"]
            description = row["DESCRIPTION"]
            market_value = row["MARKET_VALUE"]

            update_source_table_row(source_table, as_of_date, portfolio, portfolio_segment, category, description, market_value)
            insert_into_target_table(target_table_name, as_of_date, portfolio, portfolio_segment, category, description, market_value)

        st.success("✅ Updated the data successfully!")
    except Exception as e:
        st.error(f"❌ Error updating and inserting data: {e}")

# Main app
override_ref_df = fetch_data("Override_Ref")
available_modules = [f"Module-{int(module)}" for module in override_ref_df['MODULE'].unique()] if not override_ref_df.empty else []
if not available_modules:
    st.warning("⚠️ No modules found in Override_Ref table.")

selected_module = st.selectbox("Select Module", available_modules)

module_tables_df = fetch_override_ref_data(selected_module)
available_tables = module_tables_df['SOURCE_TABLE'].unique() if not module_tables_df.empty else []
selected_table = st.selectbox("Select Table", available_tables)

table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table] if not module_tables_df.empty else pd.DataFrame()
if not table_info_df.empty:
    target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
    
    tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

    with tab1:
        st.subheader(f"Source Data from {selected_table}")
        source_df = fetch_data(selected_table)
        if not source_df.empty:
            edited_df = st.data_editor(source_df, num_rows="dynamic", use_container_width=True)
            if st.button("Submit Updates", type="primary"):
                update_and_insert(selected_table, target_table_name, edited_df, source_df)
        else:
            st.info(f"ℹ️ No data available in {selected_table}.")

    with tab2:
        st.subheader(f"Overridden Values from {target_table_name}")
        overridden_df = fetch_data(target_table_name)
        overridden_df = overridden_df[overridden_df['RECORD_FLAG'] == 'O'] if not overridden_df.empty else pd.DataFrame()
        if not overridden_df.empty:
            st.dataframe(overridden_df, use_container_width=True)
        else:
            st.info(f"ℹ️ No overridden values with RECORD_FLAG = 'O' in {target_table_name}.")
else:
    st.error(f"❌ No target table configured for {selected_table} in Override_Ref.")

# Footer
st.markdown("---")
st.caption("Portfolio Performance Override System • Last updated: March 12, 2025")
