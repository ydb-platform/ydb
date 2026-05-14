from . import deploy_ctx


dynamic_server_format = '''kikimr_grpc_port="{grpc_port}"
kikimr_grpcs_port="{grpcs_port}"
kikimr_home="{deploy_path}/{process_name}"
kikimr_ic_port="{ic_port}"
kikimr_mon_address=""
kikimr_mon_port="{mon_port}"
kikimr_node_broker_port="{node_broker_port}"
kikimr_syslog_service_tag="{process_name}"
kikimr_tenant="{tenant}"
kikimr_config="${{kikimr_home}}/cfg"
kikimr_auth_token_file="${{kikimr_home}}/token/kikimr.token"
kikimr_key_file="${{kikimr_config}}/key.txt"
kikimr_config_dir="${{kikimr_home}}/cfg_dir/"
kikimr_pile_name="{pile_name}"

# TLS variables (filled when secure mode is enabled)
kikimr_ca="{ca}"
kikimr_cert="{cert}"
kikimr_key="{key}"
mon_cert="{mon_cert}"

kikimr_arg="${{kikimr_arg}} server --yaml-config ${{kikimr_config}}/config.yaml --tenant ${{kikimr_tenant}}"
kikimr_arg="${{kikimr_arg}}${{kikimr_mon_port:+ --mon-port ${{kikimr_mon_port}}}}${{mon_cert:+ --mon-cert ${{mon_cert}}}}"
# Prefer secure grpcs port if provided
if [ ! -z "${{kikimr_grpcs_port}}" ]; then
    kikimr_arg="${{kikimr_arg}}${{kikimr_grpcs_port:+ --grpcs-port ${{kikimr_grpcs_port}}}}"
else
    kikimr_arg="${{kikimr_arg}}${{kikimr_grpc_port:+ --grpc-port ${{kikimr_grpc_port}}}}"
fi
kikimr_arg="${{kikimr_arg}}${{kikimr_ic_port:+ --ic-port ${{kikimr_ic_port}}}}"
kikimr_arg="${{kikimr_arg}}${{kikimr_node_broker_port:+ --node-broker-port ${{kikimr_node_broker_port}}}}"
kikimr_arg="${{kikimr_arg}}${{kikimr_syslog_service_tag:+ --syslog-service-tag ${{kikimr_syslog_service_tag}}}}"
kikimr_arg="${{kikimr_arg}}${{kikimr_pile_name:+ --bridge-pile-name ${{kikimr_pile_name}}}}"

if [ -f "${{kikimr_auth_token_file}}" ]; then
    kikimr_arg="${{kikimr_arg}}${{kikimr_auth_token_file:+ --auth-token-file ${{kikimr_auth_token_file}}}}"
fi

if [ -f "${{kikimr_key_file}}" ]; then
    kikimr_arg="${{kikimr_arg}}${{kikimr_key_file:+ --key-file ${{kikimr_key_file}}}}"
else
    echo "Key file not found!"
fi

# Append TLS only when all three are provided
if [ ! -z "${{kikimr_ca}}" ] && [ ! -z "${{kikimr_cert}}" ] && [ ! -z "${{kikimr_key}}" ]; then
    kikimr_arg="${{kikimr_arg}} --ca=${{kikimr_ca}} --cert=${{kikimr_cert}} --key=${{kikimr_key}}"
fi
'''

dynamic_server = dynamic_server_format.format(
    grpc_port="%GRPC_PORT%",
    grpcs_port="%GRPCS_PORT%",
    ic_port="%IC_PORT%",
    mon_port="%MON_PORT%",
    deploy_path="%DEPLOY_PATH%",
    process_name="test_kikimr_dynamic_%NODE_IDX%",
    tenant="%TENANT%",
    pile_name="%PILE_NAME%",
    node_broker_port="%NODE_BROKER_PORT%",
    ca="%CA%",
    cert="%CERT%",
    key="%KEY%",
    mon_cert="%MON_CERT%",
)

kikimr_format = '''kikimr_mon_address=""
kikimr_grpc_port="{grpc_port}"
kikimr_grpcs_port="{grpcs_port}"
kikimr_ic_port="{ic_port}"
kikimr_mon_port="{mon_port}"
kikimr_home="{deploy_path}/{process_name}"
kikimr_config="${{kikimr_home}}/cfg"
kikimr_auth_token_file="${{kikimr_home}}/token/kikimr.token"
kikimr_key_file="${{kikimr_config}}/key.txt"
kikimr_config_dir="${{kikimr_home}}/cfg_dir/"
# TLS variables (filled when secure mode is enabled)
kikimr_ca="{ca}"
kikimr_cert="{cert}"
kikimr_key="{key}"
mon_cert="{mon_cert}"

kikimr_arg="${{kikimr_arg}} server --yaml-config ${{kikimr_config}}/config.yaml --node static"
kikimr_arg="${{kikimr_arg}}${{kikimr_mon_port:+ --mon-port ${{kikimr_mon_port}}}}${{mon_cert:+ --mon-cert ${{mon_cert}}}}"
# Prefer secure grpcs port if provided
if [ ! -z "${{kikimr_grpcs_port}}" ]; then
    kikimr_arg="${{kikimr_arg}}${{kikimr_grpcs_port:+ --grpcs-port ${{kikimr_grpcs_port}}}}"
else
    kikimr_arg="${{kikimr_arg}}${{kikimr_grpc_port:+ --grpc-port ${{kikimr_grpc_port}}}}"
fi
kikimr_arg="${{kikimr_arg}}${{kikimr_ic_port:+ --ic-port ${{kikimr_ic_port}}}}"

if [ ! -z "${{kikimr_mon_address}}" ]; then
    kikimr_arg="${{kikimr_arg}}${{kikimr_mon_address:+ --mon-address ${{kikimr_mon_address}}}}"
else
    echo "Monitoring address is not defined."
fi

if [ -f "${{kikimr_auth_token_file}}" ]; then
    kikimr_arg="${{kikimr_arg}}${{kikimr_auth_token_file:+ --auth-token-file ${{kikimr_auth_token_file}}}}"
fi

if [ -f "${{kikimr_key_file}}" ]; then
    kikimr_arg="${{kikimr_arg}}${{kikimr_key_file:+ --key-file ${{kikimr_key_file}}}}"
else
    echo "Key file not found!"
fi
# Append TLS only when all three are provided
if [ ! -z "${{kikimr_ca}}" ] && [ ! -z "${{kikimr_cert}}" ] && [ ! -z "${{kikimr_key}}" ]; then
    kikimr_arg="${{kikimr_arg}} --ca=${{kikimr_ca}} --cert=${{kikimr_cert}} --key=${{kikimr_key}}"
fi
'''

log = '''BackendFileName: "%LOG_FILE_PATH%"
'''

kikimr = kikimr_format.format(
    grpc_port="%GRPC_PORT%",
    grpcs_port="%GRPCS_PORT%",
    ic_port="%IC_PORT%",
    mon_port="%MON_PORT%",
    deploy_path="%DEPLOY_PATH%",
    process_name="test_kikimr_static_%NODE_IDX%",
    ca="%CA%",
    cert="%CERT%",
    key="%KEY%",
    mon_cert="%MON_CERT%",
)

def make_templates():
    with open(f'{deploy_ctx.work_directory}/replaced/dynamic_server.template', 'w') as file:
        print(dynamic_server, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/replaced/kikimr.template', 'w') as file:
        print(kikimr, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/replaced/log.template', 'w') as file:
        print(log, end='', file=file)
