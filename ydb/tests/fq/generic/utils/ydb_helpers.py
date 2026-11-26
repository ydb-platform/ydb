import json


def create_external_data_source(
    client, name, source_type, location, database, login=None, password=None, protocol=None
):
    properties = {
        "SOURCE_TYPE": source_type,
        "LOCATION": location,
        "DATABASE_NAME": database,
        "USE_TLS": "FALSE",
        "AUTH_METHOD": "NONE" if login is None else "BASIC",
    }
    if login is not None:
        secret = f"{name}_password"
        client.query(f'CREATE SECRET `{secret}` WITH (value = {json.dumps(password)});')
        properties.update(LOGIN=login, PASSWORD_SECRET_PATH=secret)
    if protocol is not None:
        properties["PROTOCOL"] = protocol
    options = ",\n".join(f"{key} = {json.dumps(value)}" for key, value in properties.items())
    client.query(f"CREATE EXTERNAL DATA SOURCE `{name}` WITH ({options});")
