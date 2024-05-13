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
    if "int" in data_type or "decimal" in data_type:
        return "0"
    elif "bit" in data_type:
        return "0"  # Assuming 0 as default for bit
    elif "uniqueidentifier" in data_type:
        return "''"  # Assuming empty string for GUIDs (UUIDs)
    else:
        return "''"

def generate_stored_procedure(target_table, source_table, columns):
    procedure_name = f"stp_sync_{target_table.replace('[dbo].[', '').replace(']', '')}"
    
    set_statements = ",\n        ".join(
        [f"target.{col['name']} = COALESCE(src.{col['name']}, {default_value(col['type'])})" for col in columns])
    where_conditions = " OR\n        ".join(
        [f"COALESCE(target.{col['name']}, {default_value(col['type'])}) <> COALESCE(src.{col['name']}, {default_value(col['type'])})" for col in columns])
    insert_columns = ",\n        ".join([col['name'] for col in columns])
    select_columns = ",\n        ".join([f"COALESCE(src.{col['name']}, {default_value(col['type'])})" for col in columns])

    script = f"""
USE [DW_SSK]
GO

/****** Object:  StoredProcedure [dbo].[{procedure_name}] ******
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[{procedure_name}]
AS
BEGIN
    SET NOCOUNT ON;

    -- Update existing records only if changes are detected
    UPDATE target
    SET 
        {set_statements}
    FROM 
        {target_table} target
    INNER JOIN 
        {source_table} src
    ON 
        target.InstructionId = src.Id
    WHERE 
        {where_conditions};

    -- Delete records that no longer exist in the source
    DELETE target
    FROM 
        {target_table} target
    WHERE NOT EXISTS (
        SELECT 1
        FROM {source_table} src
        WHERE target.InstructionId = src.Id
    );

    -- Insert new records
    INSERT INTO {target_table} (
        {insert_columns}
    )
    SELECT 
        {select_columns}
    FROM 
        {source_table} src
    WHERE NOT EXISTS (
        SELECT 1
        FROM {target_table} target
        WHERE target.InstructionId = src.Id
    );
END
GO
"""

    return script

def run():
    st.set_page_config(
        page_title="MS SQL Stored Procedure Generator",
        page_icon="↔️",
    )

    st.title('MS SQL Stored Procedure Generator')
    st.text('Enter the source and target tables. Enter the field names and types.')
    st.text('Click generate.')
    st.text('Edit the provided stored procedure further if required.')

    # User inputs
    target_table = st.text_input('Target Table Name', '[dbo].[tbl_dw_Target]')
    source_table = st.text_input('Source Table Name', '[SRV-SQL].[DB].[dbo].[Source]')

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
