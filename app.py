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
        row_data_copy = row_data.copy()

        if editable_column.upper() in row_data_copy:
            del row_data_copy[editable_column.upper()]

        if 'RECORD_FLAG' in row_data_copy:
            del row_data_copy['RECORD_FLAG']

        if 'INSERT_TS' in row_data_copy:
            del row_data_copy['INSERT_TS']
    
        columns = ", ".join(row_data_copy.keys())
        
        formatted_values = []
        for col, val in row_data_copy.items():
            if isinstance(val, str):
                formatted_values.append(f"'{val}'")
            elif pd.isna(val):
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
    query_params = st.query_params
    module_number = query_params.get("module", None)

    module_tables_df = fetch_override_ref_data(module_number)

    if module_number and not module_tables_df.empty:
        module_name = module_tables_df['MODULE_NAME'].iloc[0]
        st.markdown(f"<h2 style='text-align: center;'>Module: {module_name}</h2>", unsafe_allow_html=True)
    else:
        st.info("Please select a module from Power BI.")
        st.stop()

    if not module_tables_df.empty:

        available_tables = module_tables_df['SOURCE_TABLE'].unique()

        selected_table = st.selectbox("Select Table", available_tables)
        
        table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

        if not table_info_df.empty:
            target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
            editable_column = table_info_df['EDITABLE_COLUMN'].iloc[0]
            editable_column_upper = editable_column.upper()

            st.selectbox("Editable Column", [editable_column], disabled=True, key="editable_column_selectbox")

            st.markdown(f"**Editable Column:** {editable_column_upper}")

            primary_key_cols = ['ASOFDATE', 'SEGMENT', 'CATEGORY']

            tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

            with tab1:
                st.subheader(f"Source Data from {selected_table}")

                source_df = fetch_data(selected_table)
                if not source_df.empty:
                    source_df = source_df[source_df['RECORD_FLAG'] == 'A'].copy()

                    edited_df = source_df.copy()

                    def highlight_editable_column(df, column_name):
                        styled_df = pd.DataFrame('', index=df.index, columns=df.columns)
                        styled_df[column_name] = 'background-color: #FFFFE0'
                        return styled_df

                    disabled_cols = [col for col in edited_df.columns if col != editable_column_upper]

                    styled_df = edited_df.style.apply(highlight_editable_column, column_name=editable_column_upper, axis=None)

                    edited_df = st.data_editor(
                        styled_df,
                        key=f"data_editor_{selected_table}_{editable_column}",
                        num_rows="dynamic",
                        use_container_width=True,
                        disabled=disabled_cols
                    )

                    if st.button("Submit Updates"):
                        try:
                            changed_rows = edited_df[edited_df[editable_column_upper] != source_df[editable_column_upper]]

                            if not changed_rows.empty:
                                for index, row in changed_rows.iterrows():
                                    primary_key_values = {col: row[col] for col in primary_key_cols}

                                    new_value = row[editable_column_upper]
                                    old_value = source_df.loc[index, editable_column_upper]

                                    src_ins_ts = str(source_df.loc[index, 'INSERT_TS'])

                                    asofdate = row['ASOFDATE']
                                    segment = row['SEGMENT']
                                    category = row['CATEGORY']

                                    update_source_table_record_flag(selected_table, primary_key_values)

                                    insert_into_source_table(selected_table, source_df.loc[index].to_dict(), new_value, editable_column)

                                    insert_into_override_table(target_table_name, asofdate, segment, category, src_ins_ts, old_value, new_value)

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

                override_df = fetch_data(target_table_name)
                if not override_df.empty:
                    st.dataframe(override_df, use_container_width=True)
                else:
                    st.info(f"No overridden data available in {target_table_name}.")

        else:
            st.warning("No table information found in Override_Ref for the selected table.")
    else:
        st.warning("No tables found for the selected module in Override_Ref table.")

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
