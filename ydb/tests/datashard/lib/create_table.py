from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name


def create_table_sql_request(table_name: str, columns: dict[str, dict[str]], pk_columns: dict[str, dict[str]], index_columns: dict[str, dict[str]], unique: str, sync: str) -> str:
    create_columns = []
    for prefix in columns.keys():
        if (prefix != "ttl_" or columns[prefix][0] != "") and len(columns[prefix]) != 0:
            create_columns.append(", ".join(
                f"{prefix}{cleanup_type_name(type_name)} {type_name}" for type_name in columns[prefix]))
    create_primary_key = []
    for prefix in pk_columns.keys():
        if len(pk_columns[prefix]) != 0:
            create_primary_key.append(", ".join(
                f"{prefix}{cleanup_type_name(type_name)}" for type_name in pk_columns[prefix]))
    create_index = []
    for prefix in index_columns.keys():
        if len(index_columns[prefix]) != 0:
            create_index.append(", ".join(
                f"INDEX idx_{prefix}{cleanup_type_name(type_name)} GLOBAL {unique} {sync} ON ({prefix}{cleanup_type_name(type_name)})" for type_name in index_columns[prefix]))
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


def create_vector_index_sql_request(
    table_name: str,
    name_vector_index,
    embedding: str,
    prefix: str,
    function: str,
    distance: str,
    vector_type: str,
    sync,
    vector_dimension: int,
    levels: int,
    clusters: int,
    cover,
):
    create_vector_index = f"""
        ALTER TABLE {table_name}
        ADD INDEX {name_vector_index}
        GLOBAL {sync} USING vector_kmeans_tree
        ON ({f"{prefix}, " if prefix != "" else ""}{embedding}) {f"COVER ({", ".join(cover)})" if len(cover) != 0 else ""}
        WITH ({function}={distance}, vector_type="{vector_type}", vector_dimension={vector_dimension}, levels={levels}, clusters={clusters});
    """
    return create_vector_index
