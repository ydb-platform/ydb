from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name


def create_table_sql_request(table_name: str, columns: dict[str, dict[str]], pk_colums: dict[str, dict[str]], index_colums: dict[str, dict[str]], unique: str, sync: str) -> str:
    create_columns = []
    for prefix in columns.keys():
        if (prefix != "ttl_" or columns[prefix][0] != "") and len(columns[prefix]) != 0:
            create_columns.append(", ".join(
                f"{prefix}{cleanup_type_name(type_name)} {type_name}" for type_name in columns[prefix]))
    create_primary_key = []
    for prefix in pk_colums.keys():
        if len(pk_colums[prefix]) != 0:
            create_primary_key.append(", ".join(
                f"{prefix}{cleanup_type_name(type_name)}" for type_name in pk_colums[prefix]))
    create_index = []
    for prefix in index_colums.keys():
        if len(index_colums[prefix]) != 0:
            create_index.append(", ".join(
                f"INDEX idx_{prefix}{cleanup_type_name(type_name)} GLOBAL {unique} {sync} ON ({prefix}{cleanup_type_name(type_name)})" for type_name in index_colums[prefix]))
    sql_create = f"""
        CREATE TABLE `{table_name}` (
            {", ".join(create_columns)},
            PRIMARY KEY(
                {", ".join(create_primary_key)}
                ),
            {", ".join(create_index)}
            )
    """
    return sql_create


def create_ttl_sql_request(ttl: str, inteval: dict[str, str], time: str, table_name: str) -> str:
    create_ttl = []
    for pt in inteval.keys():
        create_ttl.append(
            f"""Interval("{pt}") {inteval[pt] if inteval[pt] == "" or inteval[pt] == "DELETE" else f"TO EXTERNAL DATA SOURCE {inteval[pt]}"}""")
    sql_ttl = f"""
         ALTER TABLE {table_name} SET ( TTL = 
         {", ".join(create_ttl)}
         ON {ttl} {f"AS {time}" if time != "" else ""} )
    """
    return sql_ttl
