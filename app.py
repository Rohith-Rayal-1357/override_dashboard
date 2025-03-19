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
            df = df[df['MODULE'] == int(selected_module)]
        return df
    except Exception as e:
        st.error(f"Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Function to execute dynamic INSERT and UPDATE based on join_keys
def execute_insert_update(selected_table, target_table, join_keys, editable_column, old_value, new_value, row_data):
    try:
        # Convert join_keys to list and build ON conditions dynamically
        join_keys_list = eval(join_keys)  # Convert string to list {asofdate, segment, category}
        join_condition = " AND ".join([f"src.{key} = tgt.{key}" for key in join_keys_list])

        # Construct dynamic INSERT query using parameters from the edited row
        insert_columns = ", ".join(join_keys_list + [editable_column, 'record_flag', 'insert_timestamp']) #Corrected INSERT and added editable column
        insert_values = ", ".join([f"'{row_data[key]}'" for key in join_keys_list] + [f"'{new_value}'", "'A'", "CURRENT_TIMESTAMP(0)"]) #Corrected INSERT and added editable column

        insert_sql = f"""
            INSERT INTO {selected_table} ({insert_columns})
            SELECT {insert_values}
            WHERE NOT EXISTS (
                SELECT 1
                FROM {selected_table} existing
                WHERE {join_condition}
                AND existing.{editable_column} = '{new_value}'
                AND existing.record_flag = 'A'
            );
        """

        # Construct dynamic UPDATE query
        update_sql = f"""
            UPDATE {selected_table}
            SET record_flag = 'D'
            WHERE {join_condition}
            AND {editable_column} = '{old_value}'
            AND record_flag = 'A';
        """

        # Execute both INSERT and UPDATE queries
        session.sql(update_sql).collect()
        session.sql(insert_sql).collect()

        st.success(f"Successfully executed INSERT and UPDATE for table {selected_table}")

    except Exception as e:
        st.error(f"Error executing insert/update: {e}")

# Main app
def main():
    # Get module from URL
    query_params = st.query_params
    module_number = query_params.get("module", None)

    # Get tables for the selected module
    module_tables_df = fetch_override_ref_data(module_number)

    # Display Module Name in a styled box (light ice blue background)
    if module_number and not module_tables_df.empty:
        # Get the module name from the Override_Ref table
        module_name = module_tables_df['MODULE_NAME'].iloc[0]

        # Display the module name in a light ice blue box
        st.markdown(f"""
            <div style="background-color: #E0F7FA; padding: 10px; border-radius: 5px; text-align: center; font-size: 16px;">
                <strong>Module:</strong> {module_name}
            </div>
        """, unsafe_allow_html=True)
    else:
        st.info("Please select a module from Power BI.")
        st.stop()

    if not module_tables_df.empty:
        available_tables = module_tables_df['SOURCE_TABLE'].unique()  # Get source tables based on module... # Add select table box
        selected_table = st.selectbox("Select Table", available_tables)

        # Filter Override_Ref data based on the selected table
        table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

        if not table_info_df.empty:
            target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
            editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0]
            join_keys = table_info_df['JOIN_KEYS'].iloc[0]

            # Display the editable column in a disabled selectbox
            st.selectbox("Editable Column", [editable_column], disabled=True, key="editable_column_selectbox")
            st.markdown(f"**Editable Column:** {editable_column}")

            # Split the data into two tabs
            tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

            with tab1:
                st.subheader(f"Source Data from {selected_table}")

                # Fetch data at the beginning
                source_df = fetch_data(selected_table)
                if not source_df.empty:
                    # Retain only 'A' records
                    source_df = source_df[source_df['RECORD_FLAG'] == 'A'].copy()

                    # Make the dataframe editable using st.data_editor
                    edited_df = source_df.copy()

                    # Convert the editable column to string type BEFORE styling
                    editable_column_name = editable_column.upper()
                    edited_df[editable_column_name] = edited_df[editable_column_name].astype(str)

                    # Add pencil emoji AFTER you convert column to string, before you insert in the data editor
                    edited_df = edited_df.rename(columns={editable_column_name: f"{editable_column_name} ✏️"})

                    # Disable editing for all columns except the selected editable column
                    column_display_name = f"{editable_column_name} ✏️"
                    disabled_cols = [col for col in edited_df.columns if col != column_display_name]

                    # Apply a background color to the editable column
                    def highlight_editable_column(df, column_name):
                        styled_df = pd.DataFrame('', index=df.index, columns=df.columns)
                        styled_df[column_name] = 'background-color: #FFFFE0'  # Light yellow background
                        return styled_df

                    styled_df = edited_df.style.apply(highlight_editable_column, column_name=column_display_name, axis=None)

                    edited_df = st.data_editor(
                        styled_df,  # Pass the styled dataframe
                        key=f"data_editor_{selected_table}_{editable_column}",
                        num_rows="dynamic",
                        use_container_width=True,
                        disabled=disabled_cols
                    )

                    # Submit button to update the source table and insert to the target table
                    if st.button("Submit Updates"):
                        try:
                            # Identify rows that have been edited
                            # Use the column with the pencil emoji for comparison
                            edited_column_name_with_emoji = f"{editable_column_name} ✏️"
                            changed_rows = edited_df[edited_df[edited_column_name_with_emoji] != source_df[editable_column_name].astype(str)]

                            if not changed_rows.empty:
                                for index, row in changed_rows.iterrows():
                                    # Get the new and old value for the selected column
                                    new_value = row[edited_column_name_with_emoji]
                                    old_value = source_df.loc[index, editable_column_name]
                                    row_data = row.to_dict()

                                    # Perform Insert and Update dynamically
                                    execute_insert_update(selected_table, target_table_name, join_keys, editable_column_name, old_value, new_value, row_data) # added parameters

                                    # Capture the current timestamp and store it in session state
                                    current_timestamp = datetime.now().strftime('%B %d, %Y %H:%M:%S')
                                    st.session_state.last_update_time = current_timestamp

                                    st.success("Data updated successfully!")
                            else:
                                st.info("No changes were made.")

                        except Exception as e:
                            st.error(f"Error during update/insert: {e}")
                else:
                    st.info(f"No data available in {selected_table}.")
            with tab2:
                st.subheader(f"Overridden Values from {target_table_name}")

                # Fetch overridden data
                override_df = fetch_data(target_table_name)
                if not override_df.empty:
                    st.dataframe(override_df, use_container_width=True)
                else:
                    st.info(f"No overridden data available in {target_table_name}.")
        else:
            st.warning("No table information found in Override_Ref for the selected table.")
    else:
        st.warning("No tables found for the selected module in Override_Ref table.")

    # Display the last update timestamp in the footer
    if 'last_update_time' in st.session_state:
        last_update_time = st.session_state.last_update_time
        st.markdown("---")
        st.caption(f"Portfolio Performance Override System • Last updated: {last_update_time}")
    else:
        st.markdown("---")
        st.caption("Portfolio Performance Override System • Last updated: N/A")


# Run the main function
if __name__ == "__main__":
    main()
