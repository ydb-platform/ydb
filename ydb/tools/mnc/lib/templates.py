from . import deploy_ctx


dynamic_server_format = '''ydb_grpc_port="{grpc_port}"
ydb_grpcs_port="{grpcs_port}"
ydb_home="{deploy_path}/{process_name}"
ydb_ic_port="{ic_port}"
ydb_mon_address=""
ydb_mon_port="{mon_port}"
ydb_node_broker_port="{node_broker_port}"
ydb_syslog_service_tag="{process_name}"
ydb_tenant="{tenant}"
ydb_config="${{ydb_home}}/cfg"
ydb_auth_token_file="${{ydb_home}}/token/ydb.token"
ydb_key_file="${{ydb_config}}/key.txt"
ydb_config_dir="${{ydb_home}}/cfg_dir/"
ydb_pile_name="{pile_name}"

# TLS variables (filled when secure mode is enabled)
ydb_ca="{ca}"
ydb_cert="{cert}"
ydb_key="{key}"
mon_cert="{mon_cert}"

ydb_arg="${{ydb_arg}} server --yaml-config ${{ydb_config}}/config.yaml --tenant ${{ydb_tenant}}"
ydb_arg="${{ydb_arg}}${{ydb_mon_port:+ --mon-port ${{ydb_mon_port}}}}${{mon_cert:+ --mon-cert ${{mon_cert}}}}"
# Prefer secure grpcs port if provided
if [ ! -z "${{ydb_grpcs_port}}" ]; then
    ydb_arg="${{ydb_arg}}${{ydb_grpcs_port:+ --grpcs-port ${{ydb_grpcs_port}}}}"
else
    ydb_arg="${{ydb_arg}}${{ydb_grpc_port:+ --grpc-port ${{ydb_grpc_port}}}}"
fi
ydb_arg="${{ydb_arg}}${{ydb_ic_port:+ --ic-port ${{ydb_ic_port}}}}"
ydb_arg="${{ydb_arg}}${{ydb_node_broker_port:+ --node-broker-port ${{ydb_node_broker_port}}}}"
ydb_arg="${{ydb_arg}}${{ydb_syslog_service_tag:+ --syslog-service-tag ${{ydb_syslog_service_tag}}}}"
ydb_arg="${{ydb_arg}}${{ydb_pile_name:+ --bridge-pile-name ${{ydb_pile_name}}}}"

if [ -f "${{ydb_auth_token_file}}" ]; then
    ydb_arg="${{ydb_arg}}${{ydb_auth_token_file:+ --auth-token-file ${{ydb_auth_token_file}}}}"
fi

if [ -f "${{ydb_key_file}}" ]; then
    ydb_arg="${{ydb_arg}}${{ydb_key_file:+ --key-file ${{ydb_key_file}}}}"
else
    echo "Key file not found!"
fi

# Append TLS only when all three are provided
if [ ! -z "${{ydb_ca}}" ] && [ ! -z "${{ydb_cert}}" ] && [ ! -z "${{ydb_key}}" ]; then
    ydb_arg="${{ydb_arg}} --ca=${{ydb_ca}} --cert=${{ydb_cert}} --key=${{ydb_key}}"
fi
'''

dynamic_server = dynamic_server_format.format(
    grpc_port="%GRPC_PORT%",
    grpcs_port="%GRPCS_PORT%",
    ic_port="%IC_PORT%",
    mon_port="%MON_PORT%",
    deploy_path="%DEPLOY_PATH%",
    process_name="ydb_node_dynamic_%NODE_IDX%",
    tenant="%TENANT%",
    pile_name="%PILE_NAME%",
    node_broker_port="%NODE_BROKER_PORT%",
    ca="%CA%",
    cert="%CERT%",
    key="%KEY%",
    mon_cert="%MON_CERT%",
)

ydb_format = '''ydb_mon_address=""
ydb_grpc_port="{grpc_port}"
ydb_grpcs_port="{grpcs_port}"
ydb_ic_port="{ic_port}"
ydb_mon_port="{mon_port}"
ydb_home="{deploy_path}/{process_name}"
ydb_config="${{ydb_home}}/cfg"
ydb_auth_token_file="${{ydb_home}}/token/ydb.token"
ydb_key_file="${{ydb_config}}/key.txt"
ydb_config_dir="${{ydb_home}}/cfg_dir/"
# TLS variables (filled when secure mode is enabled)
ydb_ca="{ca}"
ydb_cert="{cert}"
ydb_key="{key}"
mon_cert="{mon_cert}"

ydb_arg="${{ydb_arg}} server --yaml-config ${{ydb_config}}/config.yaml --node static"
ydb_arg="${{ydb_arg}}${{ydb_mon_port:+ --mon-port ${{ydb_mon_port}}}}${{mon_cert:+ --mon-cert ${{mon_cert}}}}"
# Prefer secure grpcs port if provided
if [ ! -z "${{ydb_grpcs_port}}" ]; then
    ydb_arg="${{ydb_arg}}${{ydb_grpcs_port:+ --grpcs-port ${{ydb_grpcs_port}}}}"
else
    ydb_arg="${{ydb_arg}}${{ydb_grpc_port:+ --grpc-port ${{ydb_grpc_port}}}}"
fi
ydb_arg="${{ydb_arg}}${{ydb_ic_port:+ --ic-port ${{ydb_ic_port}}}}"

if [ ! -z "${{ydb_mon_address}}" ]; then
    ydb_arg="${{ydb_arg}}${{ydb_mon_address:+ --mon-address ${{ydb_mon_address}}}}"
else
    echo "Monitoring address is not defined."
fi

if [ -f "${{ydb_auth_token_file}}" ]; then
    ydb_arg="${{ydb_arg}}${{ydb_auth_token_file:+ --auth-token-file ${{ydb_auth_token_file}}}}"
fi

if [ -f "${{ydb_key_file}}" ]; then
    ydb_arg="${{ydb_arg}}${{ydb_key_file:+ --key-file ${{ydb_key_file}}}}"
else
    echo "Key file not found!"
fi
# Append TLS only when all three are provided
if [ ! -z "${{ydb_ca}}" ] && [ ! -z "${{ydb_cert}}" ] && [ ! -z "${{ydb_key}}" ]; then
    ydb_arg="${{ydb_arg}} --ca=${{ydb_ca}} --cert=${{ydb_cert}} --key=${{ydb_key}}"
fi
'''

log = '''BackendFileName: "%LOG_FILE_PATH%"
'''

ydb = ydb_format.format(
    grpc_port="%GRPC_PORT%",
    grpcs_port="%GRPCS_PORT%",
    ic_port="%IC_PORT%",
    mon_port="%MON_PORT%",
    deploy_path="%DEPLOY_PATH%",
    process_name="ydb_node_static_%NODE_IDX%",
    ca="%CA%",
    cert="%CERT%",
    key="%KEY%",
    mon_cert="%MON_CERT%",
)


def make_templates():
    with open(f'{deploy_ctx.work_directory}/replaced/dynamic_server.template', 'w') as file:
        print(dynamic_server, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/replaced/ydb.template', 'w') as file:
        print(ydb, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/replaced/log.template', 'w') as file:
        print(log, end='', file=file)
