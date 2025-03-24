import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="wide",  # Use wide layout for better spacing
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
    <style>
        .css-18e3th9 {background-color: #F0F2F6;} /* Light grey background */
        .css-1kyxreq {border-radius: 12px; padding: 20px;}
        .css-1b36jdy {text-align: center;}
        .stButton>button {background-color: #1E88E5; color: white; border-radius: 5px; height: 40px;}
        .stSelectbox>label {font-size: 16px;}
        .stDataFrame {border: 1px solid #dddddd; border-radius: 8px;}
        .success-message {
            color: green;
            font-weight: bold;
            padding: 10px;
            background-color: #d4edda;
            border: 1px solid #c3e6cb;
            border-radius: 5px;
            margin-bottom: 10px;
        }
    </style>
""", unsafe_allow_html=True)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Snowflake Connection Status using Streamlit secrets
try:
    connection_parameters = {
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        "database": st.secrets["SNOWFLAKE_DATABASE"],
        "schema": st.secrets["SNOWFLAKE_SCHEMA"],
    }

    # Create a Snowpark session using the connection parameters
    session = Session.builder.configs(connection_parameters).create()

    st.markdown('<div class="success-message">✅ Successfully connected to Snowflake!</div>', unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ Error connecting to Snowflake: {e}")
    st.stop()

# Function to fetch data based on the table name
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch override ref data based on the selected module
def fetch_override_ref_data(selected_module=None):
    try:
        df = session.table("Override_Ref").to_pandas()
        df.columns = [col.upper() for col in df.columns]

        if selected_module:
            module_num = int(selected_module.split('-')[1])
            df = df[df['MODULE_NUM'] == module_num]
        return df
    except Exception as e:
        st.error(f"Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Function to insert into override table
def insert_into_override_table(target_table, row_data, old_value, new_value):
    try:
        as_of_date = row_data['AS_OF_DATE']
        asset_class = row_data['ASSET_CLASS']
        segment = row_data['SEGMENT']
        segment_name = row_data['SEGMENT_NAME']
        strategy = row_data['STRATEGY']
        strategy_name = row_data['STRATEGY_NAME']
        portfolio = row_data['PORTFOLIO']
        portfolio_name = row_data['PORTFOLIO_NAME']
        holding_fund_ids = row_data['HOLDING_FUND_IDS']
        unitized_owner_ind = row_data['UNITIZED_OWNER_IND']

        insert_sql = f"""
            INSERT INTO {target_table} (AS_OF_DATE, ASSET_CLASS, SEGMENT, SEGMENT_NAME, STRATEGY, STRATEGY_NAME, PORTFOLIO, PORTFOLIO_NAME, HOLDING_FUND_IDS, MARKET_VALUE_OLD, MARKET_VALUE_NEW, UNITIZED_OWNER_IND, AS_AT_DATE, RECORD_FLAG)
            VALUES ('{as_of_date}', '{asset_class}', '{segment}', '{segment_name}', '{strategy}', '{strategy_name}', '{portfolio}', '{portfolio_name}', '{holding_fund_ids}', {old_value}, {new_value}, {unitized_owner_ind}, CURRENT_TIMESTAMP(), 'O')
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {target_table}: {e}")

# Function to insert new row in source table
def insert_into_source_table(source_table, row_data, new_value, editable_column):
    try:
        row_data_copy = row_data.copy()

        if editable_column.upper() in row_data_copy:
            del row_data_copy[editable_column.upper()]

        if 'RECORD_FLAG' in row_data_copy:
            del row_data_copy['RECORD_FLAG']

        if 'AS_AT_DATE' in row_data_copy:
            del row_data_copy['AS_AT_DATE']

        columns = ", ".join(row_data_copy.keys())
        formatted_values = []

        for col, val in row_data_copy.items():
            if isinstance(val, str):
                formatted_values.append(f"'{val}'")
            elif val is None or pd.isna(val):
                formatted_values.append("NULL")
            elif isinstance(val, (int, float)):
                formatted_values.append(str(val))
            elif isinstance(val, pd.Timestamp):
                formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif isinstance(val, datetime):
                formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            else:
                formatted_values.append(f"'{str(val)}'")

        values = ", ".join(formatted_values)

        insert_sql = f"""
            INSERT INTO {source_table} ({columns}, {editable_column}, record_flag, as_at_date)
            VALUES ({values}, '{new_value}', 'A', CURRENT_TIMESTAMP())
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {source_table}: {e}")
        
# Function to update record flag in source table
def update_source_table_record_flag(source_table, primary_key_values):
    try:
        where_clause_parts = []
        for col, val in primary_key_values.items():
            if val is None:
                where_clause_parts.append(f"{col} IS NULL")
            else:
                where_clause_parts.append(f"{col} = '{val}'")
        where_clause = " AND ".join(where_clause_parts)

        update_sql = f"""
            UPDATE {source_table}
            SET record_flag = 'D',
                as_at_date = CURRENT_TIMESTAMP()
            WHERE {where_clause} AND record_flag = 'A'
        """
        session.sql(update_sql).collect()
    except Exception as e:
        st.error(f"Error updating record flag in {source_table}: {e}")

# Main app
override_ref_df = fetch_data("Override_Ref")
if not override_ref_df.empty:
    module_numbers = sorted(override_ref_df['MODULE_NUM'].unique())
    available_modules = [f"Module-{int(module)}" for module in module_numbers]
else:
    available_modules = []
    st.warning("No modules found in Override_Ref table.")

selected_module = st.selectbox("Select Module", available_modules, key="module_selector")

# Display selected module
st.markdown(f"Module: {selected_module}")

module_tables_df = fetch_override_ref_data(selected_module)

if not module_tables_df.empty:
    available_tables = module_tables_df['SOURCE_TABLE'].unique()

    selected_table = st.selectbox("Select Table", available_tables, key="table_selector")

    table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

    if not table_info_df.empty:
        target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
        editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0]

        primary_key_cols = ['AS_OF_DATE', 'ASSET_CLASS', 'SEGMENT', 'STRATEGY', 'PORTFOLIO', 'UNITIZED_OWNER_IND', 'HOLDING_FUND_IDS']

        tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

        with tab1:
            st.subheader(f"Source Data from {selected_table}")
            source_df = fetch_data(selected_table)
            if not source_df.empty:
                source_df = source_df[source_df['RECORD_FLAG'] == 'A'].copy()

                # Make the dataframe editable only for the selected editable column
                edited_df = source_df.copy()

                def highlight_editable_column(df, column_name):
                    styled_df = pd.DataFrame('', index=df.index, columns=df.columns)
                    styled_df[column_name] = 'background-color: #FFFFE0'
                    return styled_df

                # Display the editable column, read-only
                st.markdown(f"Editable Column: {editable_column}")

                # Apply style to highlight the editable column
                styled_df = edited_df.style.apply(highlight_editable_column, column_name=editable_column, axis=None)

                edited_df = st.data_editor(
                    styled_df,
                    key=f"data_editor_{selected_table}_{editable_column}",
                    num_rows="dynamic",
                    use_container_width=True,
                    disabled=[col for col in edited_df.columns if col != editable_column and col not in primary_key_cols]
                )

                # Submit updates
                if st.button("Submit Updates"):
                    try:
                        changed_rows = edited_df[edited_df[editable_column] != source_df[editable_column]]
                        if not changed_rows.empty:
                            for index, row in changed_rows.iterrows():
                                primary_key_values = {col: row[col] for col in primary_key_cols}
                                new_value = row[editable_column]
                                old_value = source_df.loc[index, editable_column]

                                insert_into_override_table(target_table_name, source_df.loc[index].to_dict(), old_value, new_value)
                                insert_into_source_table(selected_table, source_df.loc[index].to_dict(), new_value, editable_column)
                                update_source_table_record_flag(selected_table, primary_key_values)

                            st.success("👍 Data updated successfully!")
                        else:
                            st.info("No changes were made.")
                    except Exception as e:
                        st.error(f"Error during update/insert: {e}")
            else:
                st.info(f"No data available in {selected_table}.")

        with tab2:
            st.subheader(f"Overridden Values from {target_table_name}")
            override_df = fetch_data(target_table_name)
            if not override_df.empty:
                st.dataframe(override_df, use_container_width=True)
            else:
                st.info(f"No overridden data available in {target_table_name}.")
    else:
        st.warning("No table information found in Override_Ref for the selected table.")
else:
    st.warning("No tables found for the selected module in Override_Ref table.")

# Footer
st.markdown("Portfolio Performance Override System • Last updated: March 24, 2025")
