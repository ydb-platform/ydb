import yatest.common


def get_pool(self, ydb_cli_path, endpoint, database, user, query):
    # It's funny, but the only way to find resource pool that was used is to use YDB CLI

    command = [
        ydb_cli_path,
        '-e', 'grpc://' + endpoint,
        '-d', database,
        "--user", user,
        '--no-password',
        "sql",
        "-s", query,
        '--stats', 'full',
        '--format', 'json-unicode'
    ]

    stdout = yatest.common.execute(command, wait=True).stdout.decode("utf-8")
    resource_pool_in_use = _find_resource_pool_id(stdout)
    return resource_pool_in_use


def _find_resource_pool_id(text):
    key = '"ResourcePoolId"'
    if key in text:
        start_idx = text.find(key) + len(key)
        # Skip spaces and colon
        while start_idx < len(text) and text[start_idx] in ' :':
            start_idx += 1
        # Skip opening double quote
        if start_idx < len(text) and text[start_idx] == '"':
            start_idx += 1
            end_idx = text.find('"', start_idx)
            if end_idx != -1:
                return text[start_idx:end_idx]
    return None
