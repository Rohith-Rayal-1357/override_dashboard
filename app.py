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

    # ✅ Create a Snowpark session
    session = Session.builder.configs(connection_parameters).create()
    st.success("✅ Successfully connected to Snowflake!")

except Exception as e:
    st.error(f"❌ Failed to connect to Snowflake: {e}")
    st.stop()

# Function to fetch data from a table
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch override reference data based on selected module
def fetch_override_ref_data(selected_module=None):
    try:
        df = session.table("Override_Ref").to_pandas()
        df.columns = [col.upper() for col in df.columns]

        # Filter by selected module if provided
        if selected_module:
            df = df[df['MODULE'] == int(selected_module)]
        return df
    except Exception as e:
        st.error(f"Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Construct the dynamic INSERT SQL
def construct_insert_sql(source_table, target_table, join_keys):
    # Create the JOIN condition dynamically based on join_keys
    join_condition = " AND ".join([f"src.{key} = tgt.{key}" for key in join_keys])

    insert_sql = f"""
        INSERT INTO {source_table}
        (asofdate, segment, category, amount, record_flag, insert_ts)
        SELECT tgt.asofdate, tgt.segment, tgt.category, tgt.amount_new, 'A', CURRENT_TIMESTAMP(0)
        FROM {target_table} tgt
        JOIN {source_table} src
            ON {join_condition}
        WHERE src.amount = tgt.amount_old
        AND src.record_flag = 'A';
    """
    return insert_sql

# Construct the dynamic UPDATE SQL
def construct_update_sql(source_table, target_table, join_keys):
    # Create the JOIN condition dynamically based on join_keys
    join_condition = " AND ".join([f"src.{key} = tgt.{key}" for key in join_keys])

    update_sql = f"""
        UPDATE {source_table} src
        SET record_flag = 'D'
        FROM {target_table} tgt
        WHERE {join_condition}
        AND src.amount = tgt.amount_old
        AND src.record_flag = 'A';
    """
    return update_sql

# Function to execute dynamic SQL queries
def run_dynamic_sql(insert_sql, update_sql):
    try:
        # Execute INSERT SQL
        session.sql(insert_sql).collect()
        st.success("Data inserted successfully!")

        # Execute UPDATE SQL
        session.sql(update_sql).collect()
        st.success("Data updated successfully!")
        
    except Exception as e:
        st.error(f"Error executing SQL queries: {e}")

# Main function for Streamlit app
def main():
    # Get module from URL query params (if any)
    query_params = st.query_params
    module_number = query_params.get("module", None)

    # Fetch Override_Ref data for the selected module
    module_tables_df = fetch_override_ref_data(module_number)

    # Display the module name if available
    if module_number and not module_tables_df.empty:
        module_name = module_tables_df['MODULE_NAME'].iloc[0]
        st.markdown(f"""
            <div style="background-color: #E0F7FA; padding: 10px; border-radius: 5px; text-align: center; font-size: 16px;">
                <strong>Module:</strong> {module_name}
            </div>
        """, unsafe_allow_html=True)
    else:
        st.info("Please select a module from Power BI.")
        st.stop()

    # If data is found for the selected module, process further
    if not module_tables_df.empty:
        available_tables = module_tables_df['SOURCE_TABLE'].unique()

        # Let the user select a table
        selected_table = st.selectbox("Select Table", available_tables)

        # Get table details for the selected table
        table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

        if not table_info_df.empty:
            target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
            editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0]
            join_keys = table_info_df['JOIN_KEYS'].iloc[0].strip('{}').split(',')

            # Display the editable column in a disabled selectbox
            st.selectbox("Editable Column", [editable_column], disabled=True, key="editable_column_selectbox")
            st.markdown(f"**Editable Column:** {editable_column}")

            # Fetch source data for the selected table
            source_df = fetch_data(selected_table)
            if not source_df.empty:
                # Filter the 'A' records
                source_df = source_df[source_df['RECORD_FLAG'] == 'A'].copy()

                # Make the dataframe editable
                edited_df = source_df.copy()
                edited_df = edited_df.rename(columns={editable_column.upper(): f"{editable_column.upper()} ✏️"})

                # Apply a background color to the editable column
                def highlight_editable_column(df, column_name):
                    styled_df = pd.DataFrame('', index=df.index, columns=df.columns)
                    styled_df[column_name] = 'background-color: #FFFFE0'
                    return styled_df

                # Disable editing for other columns
                disabled_cols = [col for col in edited_df.columns if col != f"{editable_column.upper()} ✏️"]
                styled_df = edited_df.style.apply(highlight_editable_column, column_name=f"{editable_column.upper()} ✏️", axis=None)

                # Render the data editor
                edited_df = st.data_editor(
                    styled_df,
                    key=f"data_editor_{selected_table}_{editable_column}",
                    num_rows="dynamic",
                    use_container_width=True,
                    disabled=disabled_cols
                )

                # Submit button to execute SQL queries
                if st.button("Submit Updates"):
                    try:
                        # Identify rows that have been edited
                        changed_rows = edited_df[edited_df[f"{editable_column.upper()} ✏️"] != source_df[editable_column.upper()]]

                        if not changed_rows.empty:
                            for index, row in changed_rows.iterrows():
                                # Construct primary key values
                                primary_key_values = {col: row[col] for col in ['ASOFDATE', 'SEGMENT', 'CATEGORY']}

                                # Get the old and new values
                                new_value = row[f"{editable_column.upper()} ✏️"]
                                old_value = source_df.loc[index, editable_column.upper()]

                                # Construct the dynamic SQL queries
                                insert_sql = construct_insert_sql(selected_table, target_table_name, join_keys)
                                update_sql = construct_update_sql(selected_table, target_table_name, join_keys)

                                # Run the dynamic SQL queries
                                run_dynamic_sql(insert_sql, update_sql)

                                st.success("Data updated and inserted successfully!")

                        else:
                            st.info("No changes were made.")
                    except Exception as e:
                        st.error(f"Error during update/insert: {e}")

            else:
                st.info(f"No data available in {selected_table}.")
        else:
            st.warning("No table information found in Override_Ref for the selected table.")
    else:
        st.warning("No tables found for the selected module in Override_Ref.")

# Run the main function
if __name__ == "__main__":
    main()
