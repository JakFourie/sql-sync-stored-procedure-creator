# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import streamlit as st
from streamlit.logger import get_logger

LOGGER = get_logger(__name__)


def default_value(data_type):
    if "uniqueidentifier" in data_type:
        return "'00000000-0000-0000-0000-000000000000'"  # Valid nil UUID
    elif "int" in data_type or "decimal" in data_type:
        return "0"
    elif "bit" in data_type:
        return "0"
    else:
        return "''"


def generate_stored_procedure(target_table, source_table, columns):
    procedure_name = "stp_sync_{}".format(target_table.replace('[dbo].[', '').replace(']', ''))

    # Ensure there is at least one column provided
    if not columns:
        raise ValueError("No columns provided for the stored procedure.")

    first_column_name = columns[0]['name']  # Dynamically get the first column name for the join condition

    set_statements = ",\n        ".join(
        ["target.{0} = COALESCE(src.{0}, {1})".format(col['name'], default_value(col['type'])) if 'uniqueidentifier' not in col['type'] else "target.{0} = ISNULL(src.{0}, '00000000-0000-0000-0000-000000000000')".format(col['name']) for col in columns])
    where_conditions = " OR\n        ".join(
        ["COALESCE(target.{0}, {1}) <> COALESCE(src.{0}, {1}){2}".format(
            col['name'], 
            default_value(col['type']), 
            " COLLATE DATABASE_DEFAULT" if 'nvarchar' in col['type'] else "")
         for col in columns])

    script = [
        "USE [DW]",
        "GO",
        "/****** Object:  StoredProcedure [dbo].[{}] ******/".format(procedure_name),
        "SET ANSI_NULLS ON",
        "GO",
        "SET QUOTED_IDENTIFIER ON",
        "GO",
        "CREATE OR ALTER PROCEDURE [dbo].[{}]".format(procedure_name),
        "AS",
        "BEGIN",
        "    SET NOCOUNT ON;",
        "    -- Update existing records only if changes are detected",
        "    UPDATE target",
        "    SET ",
        "        {}".format(set_statements),
        "    FROM ",
        "        {} target".format(target_table),
        "    INNER JOIN ",
        "        {} src".format(source_table),
        "    ON ",
        "        target.{} = src.{}".format(first_column_name, first_column_name),  # Use the first column name for the join
        "    WHERE ",
        "        {};".format(where_conditions),
        "    -- Delete records that no longer exist in the source",
        "    DELETE target",
        "    FROM ",
        "        {}".format(target_table),
        "    WHERE NOT EXISTS (",
        "        SELECT 1",
        "        FROM {}".format(source_table),
        "        WHERE target.{} = src.{}".format(first_column_name, first_column_name),  # Use the first column name for deletion check
        "    );",
        "    -- Insert new records",
        "    INSERT INTO {} (".format(target_table),
        "        {}".format(",\n        ".join([col['name'] for col in columns])),
        "    )",
        "    SELECT ",
        "        {}".format(",\n        ".join(["ISNULL(src.{0}, '00000000-0000-0000-0000-000000000000')".format(col['name']) if 'uniqueidentifier' in col['type'] else "COALESCE(src.{0}, {1})".format(col['name'], default_value(col['type'])) for col in columns])),
        "    FROM ",
        "        {}".format(source_table),
        "    WHERE NOT EXISTS (",
        "        SELECT 1",
        "        FROM {} target".format(target_table),
        "        WHERE target.{} = src.{}".format(first_column_name, first_column_name),  # Use the first column name for insertion check
        "    );",
        "END",
        "GO"
    ]

    return "\n".join(script)



def run():
    st.set_page_config(
        page_title="MS SQL Sync Stored Procedure Generator",
        page_icon="↔️",
    )

    st.title('MS SQL Sync Stored Procedure Generator')
    st.text('Enter the source and target tables. Enter the field names and types.')
    st.text('Click generate.')
    st.text('Edit the provided stored procedure further if required.')

    # User inputs
    target_table = st.text_input('Target Table Name', '[dbo].[tbl_dw_Target]')
    source_table = st.text_input('Source Table Name', '[SRV-SQL].[DB].[dbo].[Source]')

    st.markdown("""
    **Note:** The first entry below is treated as the unique ID column. This column is used to match records between the source and target tables for updates, deletions, and insertions.
    """)

    # Dynamic columns
    columns = []
    columns_number = st.number_input('Number of Columns', min_value=0, max_value=20, step=1, value=1)
    for i in range(columns_number):
        with st.container():
            col1, col2 = st.columns(2)
            with col1:
                col_name = st.text_input(f'Column {i+1} Name', value="Column_Name", key=f"name_{i}")
            with col2:
                col_type = st.selectbox(f'Column {i+1} Type', options=["int", "uniqueidentifier", "nvarchar(50)", "date", "decimal(10, 3)", "bit"], index=2, key=f"type_{i}")
            columns.append({'name': col_name, 'type': col_type})

    if st.button('Generate Stored Procedure'):
        script = generate_stored_procedure(target_table, source_table, columns)
        st.text_area("Stored Procedure Script:", script, height=300)


if __name__ == "__main__":
    run()
