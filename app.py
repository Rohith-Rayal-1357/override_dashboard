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

# Function to update record flag in source table
def update_source_table_record_flag(source_table, primary_key_values):
    try:
        where_clause = " AND ".join([f"{col} = '{val}'" for col, val in primary_key_values.items()])
        update_sql = f"""
            UPDATE {source_table}
            SET record_flag = 'D',
                insert_ts = CURRENT_TIMESTAMP()
            WHERE {where_clause}
        """
        session.sql(update_sql).collect()
    except Exception as e:
        st.error(f"Error updating record flag in {source_table}: {e}")

# Function to insert new row in source table
def insert_into_source_table(source_table, row_data, new_value, editable_column):
    try:
        # Create a copy of row_data to avoid modifying the original DataFrame
        row_data_copy = row_data.copy()
        
        # Remove the editable column from the copied dictionary
        if editable_column.upper() in row_data_copy:
            del row_data_copy[editable_column.upper()]

        # Remove the RECORD_FLAG column from the copied dictionary
        if 'RECORD_FLAG' in row_data_copy:
            del row_data_copy['RECORD_FLAG']

        # Remove the INSERT_TS column from the copied dictionary
        if 'INSERT_TS' in row_data_copy:
            del row_data_copy['INSERT_TS']
    
        columns = ", ".join(row_data_copy.keys())
        
        # Properly format the values based on their type
        formatted_values = []
        for col, val in row_data_copy.items():
            if isinstance(val, str):
                formatted_values.append(f"'{val}'")
            elif pd.isna(val):  # Handle potential NaN values, converting to NULL
                formatted_values.append("NULL")
            elif isinstance(val, (int, float)):
                formatted_values.append(str(val))
            elif isinstance(val, pd.Timestamp):  # Format Timestamp
                formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")  # Snowflake TIMESTAMP format
            elif isinstance(val, datetime):  # Format datetime object
                 formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            else:
                formatted_values.append(f"'{str(val)}'")  # Default to string if unknown type

        values = ", ".join(formatted_values)

        insert_sql = f"""
            INSERT INTO {source_table} ({columns}, {editable_column}, record_flag, insert_ts)
            VALUES ({values}, '{new_value}', 'A', CURRENT_TIMESTAMP())
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {source_table}: {e}")

# Function to insert into override table
def insert_into_override_table(target_table, asofdate, segment, category, src_ins_ts, amount_old, amount_new):
    try:
        insert_sql = f"""
            INSERT INTO {target_table} (asofdate, segment, category, src_ins_ts, amount_old, amount_new, insert_ts, record_flag)
            VALUES ('{asofdate}', '{segment}', '{category}', '{src_ins_ts}', {amount_old}, {amount_new}, CURRENT_TIMESTAMP(), 'O')
        """
        session.sql(insert_sql).collect()
    except Exception as e:
        st.error(f"Error inserting into {target_table}: {e}")

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
        available_tables = module_tables_df['SOURCE_TABLE'].unique() # Get source tables based on module

        # Add select table box
        selected_table = st.selectbox("Select Table", available_tables)
        
        # Filter Override_Ref data based on the selected table
        table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

        if not table_info_df.empty:
            target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
            editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0]
            editable_column_upper = editable_column.upper()

            # Display the editable column in a disabled selectbox
            st.selectbox("Editable Column", [editable_column], disabled=True, key="editable_column_selectbox")

            # Display the editable column label below the selectbox
            st.markdown(f"**Editable Column:** {editable_column_upper}")

            # Determine primary key columns dynamically based on selected_table
            if selected_table == 'portfolio_perf':
                primary_key_cols = ['ASOFDATE', 'SEGMENT', 'CATEGORY']
            else:
                st.error("Primary key columns not defined for this table. Please update the code.")
                st.stop()

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

                    # Modify column header to add pencil icon in the editable column
                    edited_df = edited_df.rename(columns={editable_column_upper: f"{editable_column_upper} ✏️"})

                    # Apply a background color to the editable column
                    def highlight_editable_column(df, column_name):
                        styled_df = pd.DataFrame('', index=df.index, columns=df.columns)
                        styled_df[column_name] = 'background-color: #FFFFE0'  # Light yellow background
                        return styled_df

                    # Disable editing for all columns except the selected editable column
                    disabled_cols = [col for col in edited_df.columns if col != f"{editable_column_upper} ✏️"]

                    styled_df = edited_df.style.apply(highlight_editable_column, column_name=f"{editable_column_upper} ✏️", axis=None)

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
                            changed_rows = edited_df[edited_df[f"{editable_column_upper} ✏️"] != source_df[editable_column_upper]]

                            if not changed_rows.empty:
                                for index, row in changed_rows.iterrows():
                                    # Extract primary key values
                                    primary_key_values = {col: row[col] for col in primary_key_cols}

                                    # Get new value for the selected column
                                    new_value = row[f"{editable_column_upper} ✏️"]
                                    old_value = source_df.loc[index, editable_column_upper]

                                    # Get the old insert timestamp
                                    src_ins_ts = str(source_df.loc[index, 'INSERT_TS'])

                                    # Before updating we need to extract current record values from source table.
                                    asofdate = row['ASOFDATE']
                                    segment = row['SEGMENT']
                                    category = row['CATEGORY']

                                    # 1. Mark the old record as 'D'
                                    update_source_table_record_flag(selected_table, primary_key_values)

                                    # 2. Insert the new record with 'A'
                                    insert_into_source_table(selected_table, source_df.loc[index].to_dict(), new_value, editable_column)

                                    # 3. Insert into override table
                                    insert_into_override_table(target_table_name, asofdate, segment, category, src_ins_ts, old_value, new_value)

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
