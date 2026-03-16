#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.sqlalchemy import (
    AzureContainer,
    CreateFileFormat,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    PARQUETFormatter,
)


def test_create_stage(sql_compiler):
    """
    This test compiles the SQL to create a named stage, by defining the stage naming
    information (namespace and name) and the physical storage information (here: an
    Azure container), and combining them in a CreateStage object
    NB: The test only validates that the correct SQL is generated. It does not
    execute the SQL (yet) against an actual Snowflake instance.
    """
    # define the stage name
    stage = ExternalStage(
        name="AZURE_STAGE",
        namespace="MY_DB.MY_SCHEMA",
    )
    # define the storage container
    container = AzureContainer(
        account="myaccount", container="my-container"
    ).credentials("saas_token")
    # define the stage object
    create_stage = CreateStage(stage=stage, container=container)

    # validate that the resulting SQL is as expected
    actual = sql_compiler(create_stage)
    expected = (
        "CREATE STAGE MY_DB.MY_SCHEMA.AZURE_STAGE "
        "URL='azure://myaccount.blob.core.windows.net/my-container' "
        "CREDENTIALS=(AZURE_SAS_TOKEN='saas_token')"
    )
    assert actual == expected

    create_stage_replace = CreateStage(
        stage=stage, container=container, replace_if_exists=True
    )

    # validate that the resulting SQL is as expected
    actual = sql_compiler(create_stage_replace)
    expected = (
        "CREATE OR REPLACE STAGE MY_DB.MY_SCHEMA.AZURE_STAGE "
        "URL='azure://myaccount.blob.core.windows.net/my-container' "
        "CREDENTIALS=(AZURE_SAS_TOKEN='saas_token')"
    )
    assert actual == expected

    create_stage = CreateStage(stage=stage, container=container, temporary=True)
    # validate that the resulting SQL is as expected
    actual = sql_compiler(create_stage)
    expected = (
        "CREATE TEMPORARY STAGE MY_DB.MY_SCHEMA.AZURE_STAGE "
        "URL='azure://myaccount.blob.core.windows.net/my-container' "
        "CREDENTIALS=(AZURE_SAS_TOKEN='saas_token')"
    )
    assert actual == expected


def test_create_csv_format(sql_compiler):
    """
    This test compiles the SQL to create a named CSV format. The format is defined
    using a name and a formatter object with the detailed formatting information.
    TODO: split name parameters into namespace and actual name

    NB: The test only validates that the correct SQL is generated. It does not
    execute the SQL (yet) against an actual Snowflake instance.
    """
    create_format = CreateFileFormat(
        format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
        formatter=CSVFormatter().field_delimiter(","),
    )
    actual = sql_compiler(create_format)
    expected = (
        "CREATE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT "
        "TYPE='csv' FIELD_DELIMITER = ','"
    )
    assert actual == expected

    create_format_replace = CreateFileFormat(
        format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
        formatter=CSVFormatter().field_delimiter(","),
        replace_if_exists=True,
    )
    actual = sql_compiler(create_format_replace)
    expected = (
        "CREATE OR REPLACE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT "
        "TYPE='csv' FIELD_DELIMITER = ','"
    )
    assert actual == expected


def test_create_parquet_format(sql_compiler):
    """
    This test compiles the SQL to create a named Parquet format. The format is defined
    using a name and a formatter object with the detailed formatting information.
    TODO: split name parameters into namespace and actual name

    NB: The test only validates that the correct SQL is generated. It does not
    execute the SQL (yet) against an actual Snowflake instance.

    """
    create_format = CreateFileFormat(
        format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
        formatter=PARQUETFormatter().compression("AUTO"),
    )
    actual = sql_compiler(create_format)
    expected = (
        "CREATE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT "
        "TYPE='parquet' COMPRESSION = 'AUTO'"
    )
    assert actual == expected

    create_format_replace = CreateFileFormat(
        format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
        formatter=PARQUETFormatter().compression("AUTO"),
        replace_if_exists=True,
    )
    actual = sql_compiler(create_format_replace)
    expected = (
        "CREATE OR REPLACE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT "
        "TYPE='parquet' COMPRESSION = 'AUTO'"
    )
    assert actual == expected
