from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name


def create_table(table_name: str, columns: dict[str, dict[str]], pk_colums: dict[str, dict[str]], index_colums: dict[str, dict[str]], unique: str, sync: str) -> str:
    sql_create = f"CREATE TABLE `{table_name}` ("
    for prefix in columns.keys():
        for type_name in columns[prefix]:
            if prefix != "ttl_" or type_name != "":
                sql_create += f"{prefix}{cleanup_type_name(type_name)} {type_name}, "
    sql_create += "PRIMARY KEY( "
    for prefix in pk_colums.keys():
        for type_name in pk_colums[prefix]:
            if prefix != "ttl_" or type_name != "":
                sql_create += f"{prefix}{cleanup_type_name(type_name)},"
    sql_create = sql_create[:-1]
    sql_create += ")"
    for prefix in index_colums.keys():
        for type_name in index_colums[prefix]:
            if prefix != "ttl_" or type_name != "":
                sql_create += f", INDEX idx_{prefix}{cleanup_type_name(type_name)} GLOBAL {unique} {sync} ON ({prefix}{cleanup_type_name(type_name)})"
    sql_create += ")"
    return sql_create


def create_ttl(ttl: str, inteval: dict[str, str], time: str, table_name: str) -> str:
    sql_ttl = f" ALTER TABLE {table_name} SET ( TTL = "
    lenght_interval = len(inteval)
    count = 1
    for pt in inteval.keys():
        sql_ttl += f"""Interval("{pt}") {inteval[pt] if inteval[pt] == "" or inteval[pt] == "DELETE" else f"TO EXTERNAL DATA SOURCE {inteval[pt]}"}
                {", " if count != lenght_interval else " "}"""
        count += 1
    sql_ttl += f""" ON {ttl} {f"AS {time}" if time != "" else ""} ); """
    return sql_ttl
