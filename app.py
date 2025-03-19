import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="centered"
)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Retrieve Snowflake credentials from Streamlit secrets
try:
    connection_parameters = {
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        "database": st.secrets["SNOWFLAKE_DATABASE"],
        "schema": st.secrets["SNOWFLAKE_SCHEMA"],
    }

    # Create a Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    st.success("Successfully connected to Snowflake!")

except Exception as e:
    st.error(f"Failed to connect to Snowflake: {e}")
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

        # Filter based on the selected module if provided
        if selected_module:
            df = df[df['MODULE'] == int(selected_module)]  # Use integer module number
        return df
    except Exception as e:
        st.error(f"Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Function to update a row in the source table
def update_source_table_row(source_table, primary_key_values, editable_column, new_value):
    try:
        # Construct the UPDATE statement dynamically based on primary key columns
        where_clause = " AND ".join([f"{col} = '{val}'" for col, val in primary_key_values.items()])
        update_sql = f"""
            UPDATE {source_table}
            SET {editable_column} = '{new_value}'
            WHERE {where_clause}
        """

        # Execute the UPDATE statement
        session.sql(update_sql).collect()
        st.success(f"Successfully updated row in {source_table} where {where_clause}")

    except Exception as e:
        st.error(f"Error updating row in {source_table}: {e}")

# Function to insert a row in the target table with RECORD_FLAG = 'O'
def insert_into_target_table(target_table, row_data, editable_column, new_value):
    try:
        # Construct the INSERT statement dynamically
        columns = ", ".join(row_data.keys())
        values = ", ".join([f"'{val}'" for val in row_data.values()])

        insert_sql = f"""
            INSERT INTO {target_table} ({columns}, AS_AT_DATE, RECORD_FLAG)
            VALUES ({values}, CURRENT_TIMESTAMP(), 'O')
        """

        session.sql(insert_sql).collect()
        st.success(f"Successfully inserted data to table {target_table} with RECORD_FLAG = 'O'")

    except Exception as e:
        st.error(f"Error inserting values into {target_table}: {e}")

# Main app logic
# Retrieve the query parameters using the correct method
query_params = st.query_params

# Check if the 'module' parameter exists in the URL
if 'module' in query_params:
    selected_module = query_params['module'][0]  # Get the module from the URL
else:
    selected_module = None  # No module is selected

# Error handling: If no module is selected, display an error message and stop.
if selected_module is None:
    st.error("Please select a module from the Power BI report.")
    st.stop()

try:
    selected_module = int(selected_module)  # Convert to integer
except ValueError:
    st.error("Invalid module number. Please ensure the module number is an integer.")
    st.stop()

# Fetch the module tables based on the selected module
module_tables_df = fetch_override_ref_data(selected_module)

# Check if module data exists for the selected module
if module_tables_df.empty:
    st.error(f"Module {selected_module} does not exist in Override_Ref. No data to display.")
    st.stop()

module_name = module_tables_df['MODULE_NAME'].iloc[0]
st.markdown(f"<h3 style='text-align: center;'>Module: {module_name}</h3>", unsafe_allow_html=True)

# Select table within the module
available_tables = module_tables_df['SOURCE_TABLE'].unique()
selected_table = st.selectbox("Select Table", available_tables)

# Filter the data to the selected table
table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

if not table_info_df.empty:
    target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
    
    # **Enhancement: Allow editable column selection**
    editable_columns = table_info_df['EDITABLE_COLUMN'].unique()
    selected_column = st.selectbox("Select Editable Column", editable_columns)
    st.markdown(f"### **Editable Column:** {selected_column.upper()}")

    selected_column_upper = selected_column.upper()

    # Fetch primary key columns dynamically from the source table
    primary_key_cols = {
        'fact_portfolio_perf': ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY'],
        'fact_income': ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY'],
        'fact_msme': ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY'],
        'fact_orders': ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY'],
        'fact_customers': ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY']
    }.get(selected_table, [])

    if not primary_key_cols:
        st.error("Primary key columns not defined for this table. Please update the code.")
        st.stop()

    # Split the data into two tabs
    tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

    with tab1:
        st.subheader(f"Source Data from {selected_table}")

        # Fetch data at the beginning
        source_df = fetch_data(selected_table)
        if not source_df.empty:
            # Make the dataframe editable using st.data_editor
            edited_df = source_df.copy()

            # Apply a background color to the editable column
            def highlight_editable_column(df, column_name):
                # Create a style for the entire dataframe
                styled_df = pd.DataFrame('', index=df.index, columns=df.columns)
                # Apply yellow background only to the selected column
                styled_df[column_name] = 'background-color: #FFFFE0'
                return styled_df

            # Disable editing for all columns except the selected editable column
            disabled_cols = [col for col in edited_df.columns if col != selected_column_upper]

            styled_df = edited_df.style.apply(highlight_editable_column, column_name=selected_column_upper, axis=None)

            edited_df = st.data_editor(
                edited_df,  # Pass the original dataframe for editing
                key=f"data_editor_{selected_table}_{selected_column}",
                num_rows="dynamic",
                use_container_width=True,
                disabled=disabled_cols
            )

            # Submit button to update the source table and insert into the target table
            if st.button("Submit Updates"):
                try:
                    # Identify rows that have been edited
                    changed_rows = edited_df[edited_df[selected_column_upper] != source_df[selected_column_upper]]

                    if not changed_rows.empty:
                        # Capture current timestamp when updates are submitted
                        last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        for index, row in changed_rows.iterrows():
                            # Extract primary key values
                            primary_key_values = {col: row[col] for col in primary_key_cols}

                            # Get new value for the selected column
                            new_value = row[selected_column_upper]

                            # Update source table
                            update_source_table_row(selected_table, primary_key_values, selected_column, new_value)

                            # Prepare row data for insertion into target table
                            row_data = row.to_dict()

                            # Insert into target table
                            insert_into_target_table(target_table_name, row_data, selected_column, new_value)

                        st.success(f"Data updated successfully at {last_updated}!")

                        # Update the caption to show the last updated timestamp
                        st.session_state.last_updated = last_updated  # Store last update timestamp in session state
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

# **Enhancement: Block Module Selection**
# For demonstration, let's block module selection if it's not in a predefined list
blocked_modules = [1, 3]  # Example blocked modules
if selected_module in blocked_modules:
    st.error(f"Module {selected_module} is currently blocked.")
    st.stop()

# Footer (Dynamic with last updated time)
if 'last_updated' in st.session_state:
    st.markdown("---")
    st.caption(f"Portfolio Performance Override System - Last updated: {st.session_state.last_updated}")
else:
    st.markdown("---")
    st.caption("Portfolio Performance Override System - Last updated: Not yet updated")
