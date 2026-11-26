import json


def create_external_data_source(
    client, name, source_type, location, database, login=None, password=None, protocol=None, token=None
):
    properties = {
        "SOURCE_TYPE": source_type,
        "LOCATION": location,
        "DATABASE_NAME": database,
        "USE_TLS": "FALSE",
        "AUTH_METHOD": "TOKEN" if token is not None else "NONE" if login is None else "BASIC",
    }
    if token is not None:
        secret = f"{name}_token"
        client.query(f'CREATE SECRET `{secret}` WITH (value = {json.dumps(token)});')
        properties["TOKEN_SECRET_PATH"] = secret
    elif login is not None:
        secret = f"{name}_password"
        client.query(f'CREATE SECRET `{secret}` WITH (value = {json.dumps(password)});')
        properties.update(LOGIN=login, PASSWORD_SECRET_PATH=secret)
    if protocol is not None:
        properties["PROTOCOL"] = protocol
    options = ",\n".join(f"{key} = {json.dumps(value)}" for key, value in properties.items())
    client.query(f"CREATE EXTERNAL DATA SOURCE `{name}` WITH ({options});")
