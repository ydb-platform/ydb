BASE_VARS = [
    ("kikimr_config", "${kikimr_home}/cfg"),
    ("kikimr_system_file", "${kikimr_config}/sys.txt"),
    ("kikimr_domain_file", "${kikimr_config}/domains.txt"),
    ("kikimr_naming_file", "${kikimr_config}/names.txt"),
    ("kikimr_blobstorage_file", "${kikimr_config}/bs.txt"),
    ("kikimr_cms_file", "${kikimr_config}/cms.txt"),
    ("kikimr_ic_file", "${kikimr_config}/ic.txt"),
    ("kikimr_grpc_file", "${kikimr_config}/grpc.txt"),
    ("kikimr_bootstrap_file", "${kikimr_config}/boot.txt"),
    ("kikimr_channels_file", "${kikimr_config}/channels.txt"),
    ("kikimr_logfile", "${kikimr_config}/log.txt"),
    ("kikimr_vdisksfile", "${kikimr_config}/vdisks.txt"),
    ("kikimr_kqp_file", "${kikimr_config}/kqp.txt"),
    ('kikimr_feature_flags_file', "${kikimr_config}/feature_flags.txt"),
    ("kikimr_netmode", "--tcp"),
    ("kikimr_mon_threads", "10"),
    ("kikimr_udfs_dir", "${kikimr_binaries_base_path}/libs"),
    ("kikimr_key_file", "${kikimr_config}/key.txt"),
    ("kikimr_auth_file", "${kikimr_config}/auth.txt"),
    ("kikimr_auth_token_file", "${kikimr_home}/token/kikimr.token"),
    ("kikimr_dyn_ns_file", "${kikimr_config}/dyn_ns.txt"),
    ("kikimr_tracing_file", "${kikimr_config}/tracing.txt"),
    ("kikimr_pdisk_key_file", "${kikimr_config}/pdisk_key.txt"),
]

NODE_ID_LOCAL_VAR = """
kikimr_node_id="static"
"""

CUSTOM_CONFIG_INJECTOR = """
#Custom config
[ -s /etc/default/kikimr.custom ] && . /etc/default/kikimr.custom
"""

CUSTOM_SYS_CONFIG_INJECTOR = """
if [ -z "$kikimr_system_file" ]; then
    kikimr_system_file="${kikimr_config}/sys.txt"
fi
"""

NEW_STYLE_CONFIG = """kikimr_auth_token_file="${kikimr_home}/token/kikimr.token"
kikimr_config="${kikimr_home}/cfg"
kikimr_auth_token_file="${kikimr_home}/token/kikimr.token"
kikimr_key_file="${kikimr_config}/key.txt"

kikimr_arg="${kikimr_arg} server --yaml-config ${kikimr_config}/config.yaml --node static"
kikimr_arg="${kikimr_arg}${kikimr_mon_port:+ --mon-port ${kikimr_mon_port}}"
kikimr_arg="${kikimr_arg}${kikimr_mon_threads:+ --mon-threads ${kikimr_mon_threads}}"
kikimr_arg="${kikimr_arg}${kikimr_grpc_port:+ --grpc-port ${kikimr_grpc_port}}"
kikimr_arg="${kikimr_arg}${kikimr_ic_port:+ --ic-port ${kikimr_ic_port}}"

if [ ! -z "${kikimr_mon_address}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_mon_address:+ --mon-address ${kikimr_mon_address}}"
else
    echo "Monitoring address is not defined."
fi

if [ -f "${kikimr_auth_token_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_auth_token_file:+ --auth-token-file ${kikimr_auth_token_file}}"
fi

if [ -f "${kikimr_key_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_key_file:+ --key-file ${kikimr_key_file}}"
else
    echo "Key file not found!"
fi
kikimr_arg="${kikimr_arg}${kikimr_ca:+ --ca=${kikimr_ca}}${kikimr_cert:+ --cert=${kikimr_cert}}${kikimr_key:+ --key=${kikimr_key}}"

"""

NEW_STYLE_DYNAMIC_CFG = """kikimr_grpc_port="${kikimr_grpc_port:? expected not empty var}"
kikimr_home="${kikimr_home:? expected not empty var}"
kikimr_ic_port="${kikimr_ic_port:? expected not empty var}"
kikimr_binaries_base_path="/Berkanavt/kikimr"
kikimr_mbus_port="${kikimr_mbus_port:? expected not empty var}"
kikimr_mon_address=""
kikimr_mon_port="${kikimr_mon_port:? expected not empty var}"
kikimr_node_broker_port="2135"
kikimr_syslog_service_tag="${kikimr_syslog_service_tag:? expected not empty var}"
kikimr_tenant="${kikimr_tenant:? expected not empty var}"
kikimr_config="${kikimr_home}/cfg"
kikimr_auth_token_file="${kikimr_home}/token/kikimr.token"
kikimr_key_file="${kikimr_config}/key.txt"

#Custom config
[ -s /etc/default/kikimr.custom ] && . /etc/default/kikimr.custom

kikimr_arg="${kikimr_arg} server --yaml-config ${kikimr_config}/config.yaml --tenant ${kikimr_tenant}"
kikimr_arg="${kikimr_arg}${kikimr_mon_port:+ --mon-port ${kikimr_mon_port}}"
kikimr_arg="${kikimr_arg}${kikimr_grpc_port:+ --grpc-port ${kikimr_grpc_port}}"
kikimr_arg="${kikimr_arg}${kikimr_ic_port:+ --ic-port ${kikimr_ic_port}}"
kikimr_arg="${kikimr_arg}${kikimr_node_broker_port:+ --node-broker-port ${kikimr_node_broker_port}}"
kikimr_arg="${kikimr_arg}${kikimr_syslog_service_tag:+ --syslog-service-tag ${kikimr_syslog_service_tag}}"

if [ -f "${kikimr_auth_token_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_auth_token_file:+ --auth-token-file ${kikimr_auth_token_file}}"
fi

if [ ! -z "${kikimr_grpcs_port}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_grpcs_port:+ --grpcs-port ${kikimr_grpcs_port}}"
else
    echo "GRPCs port is not defined."
fi

if [ -f "${kikimr_key_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_key_file:+ --key-file ${kikimr_key_file}}"
else
    echo "Key file not found!"
fi
"""

DEFAULT_ARGUMENTS_SET = """
kikimr_arg="${kikimr_arg} server"
kikimr_arg="${kikimr_arg}${kikimr_loglevel:+ --log-level ${kikimr_loglevel}} --syslog"
kikimr_arg="${kikimr_arg}${kikimr_netmode:+ ${kikimr_netmode}}"
kikimr_arg="${kikimr_arg}${kikimr_udfs_dir:+ --udfs-dir ${kikimr_udfs_dir}}"
kikimr_arg="${kikimr_arg}${kikimr_logfile:+ --log-file ${kikimr_logfile}}"
kikimr_arg="${kikimr_arg}${kikimr_vdisksfile:+ --vdisk-file ${kikimr_vdisksfile}}"
kikimr_arg="${kikimr_arg}${kikimr_system_file:+ --sys-file ${kikimr_system_file}}"
kikimr_arg="${kikimr_arg}${kikimr_domain_file:+ --domains-file ${kikimr_domain_file}}"
kikimr_arg="${kikimr_arg}${kikimr_naming_file:+ --naming-file ${kikimr_naming_file}}"
kikimr_arg="${kikimr_arg}${kikimr_blobstorage_file:+ --bs-file ${kikimr_blobstorage_file}}"
kikimr_arg="${kikimr_arg}${kikimr_cms_file:+ --cms-file ${kikimr_cms_file}}"
kikimr_arg="${kikimr_arg}${kikimr_ic_file:+ --ic-file ${kikimr_ic_file}}"
kikimr_arg="${kikimr_arg}${kikimr_grpc_file:+ --grpc-file ${kikimr_grpc_file}}"
kikimr_arg="${kikimr_arg}${kikimr_channels_file:+ --channels-file ${kikimr_channels_file}}"
kikimr_arg="${kikimr_arg}${kikimr_mon_port:+ --mon-port ${kikimr_mon_port}}"
kikimr_arg="${kikimr_arg}${kikimr_mon_threads:+ --mon-threads ${kikimr_mon_threads}}"
kikimr_arg="${kikimr_arg}${kikimr_bootstrap_file:+ --bootstrap-file ${kikimr_bootstrap_file}}"
kikimr_arg="${kikimr_arg}${kikimr_kqp_file:+ --kqp-file ${kikimr_kqp_file}}"
kikimr_arg="${kikimr_arg}${kikimr_feature_flags_file:+ --feature-flags-file ${kikimr_feature_flags_file}}"
kikimr_arg="${kikimr_arg}${kikimr_grpc_port:+ --grpc-port ${kikimr_grpc_port}}"
kikimr_arg="${kikimr_arg}${kikimr_ic_port:+ --ic-port ${kikimr_ic_port}}"
if [ ! -z "${kikimr_mon_address}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_mon_address:+ --mon-address ${kikimr_mon_address}}"
else
    echo "Monitoring address is not defined."
fi

if [ -f "${kikimr_key_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_key_file:+ --key-file ${kikimr_key_file}}"
else
    echo "Key file not found!"
fi

if [ -f "${kikimr_auth_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_auth_file:+ --auth-file ${kikimr_auth_file}}"
fi
if [ -f "${kikimr_auth_token_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_auth_token_file:+ --auth-token-file ${kikimr_auth_token_file}}"
fi

if [ -f "${kikimr_dyn_ns_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_dyn_ns_file:+ --dyn-nodes-file ${kikimr_dyn_ns_file}}"
fi

if [ -f "${kikimr_pdisk_key_file}" ]; then
    kikimr_arg="${kikimr_arg}${kikimr_pdisk_key_file:+ --pdisk-key-file ${kikimr_pdisk_key_file}}"
else
    echo "PDisk Key file not found!"
fi

kikimr_arg="${kikimr_arg}${kikimr_ca:+ --ca=${kikimr_ca}}${kikimr_cert:+ --cert=${kikimr_cert}}${kikimr_key:+ --key=${kikimr_key}}"
kikimr_arg="${kikimr_arg} ${kikimr_tracing_file}"
"""

NODE_ID_ARGUMENT = """kikimr_arg="${kikimr_arg}${kikimr_node_id:+ --node ${kikimr_node_id}}"
"""

NODE_BROKER_ARGUMENT = """kikimr_arg="${kikimr_arg}${kikimr_node_broker_port:+ --node-broker-port ${kikimr_node_broker_port}}"
"""

SYS_LOG_SERVICE_TAG = """kikimr_arg="${kikimr_arg}${kikimr_syslog_service_tag:+ --syslog-service-tag ${kikimr_syslog_service_tag}}"
"""

DEFAULT_KIKIMR_MBUS_INFLY = 10000
DEFAULT_KIKIMR_MBUS_INFLY_BYTES = 5000000000
DEFAULT_KIKIMR_MBUS_MAX_MESSAGE_SIZE = 140000000


def local_vars(
    tenant,
    node_broker_port=None,
    ic_port=19001,
    mon_port=8765,
    grpc_port=2135,
    mbus_port=2134,
    kikimr_home='/Berkanavt/kikimr',
    kikimr_binaries_base_path='/Berkanavt/kikimr',
    pq_enable=True,
    sqs_port=8771,
    sqs_enable=False,
    enable_cores=False,
    default_log_level=3,
    mon_address="",
    cert_params=None,
    rb_txt_enabled=False,
    metering_txt_enabled=False,
    audit_txt_enabled=False,
    yql_txt_enabled=False,
    fq_txt_enabled=False,
    new_style_kikimr_cfg=False,
    mbus_enabled=False,
):
    cur_vars = []
    if enable_cores:
        cur_vars.append(
            ("kikimr_coregen", "--core"),
        )

    if cert_params:
        for key, value in zip(['ca', 'cert', 'key'], cert_params):
            if value:
                cur_vars.append(('kikimr_' + key, value))

    if tenant:
        cur_vars.append(('kikimr_tenant', tenant))

    if node_broker_port:
        cur_vars.append(('kikimr_node_broker_port', node_broker_port))

    cur_vars.append(('kikimr_mon_address', mon_address))

    if default_log_level:
        cur_vars.extend(
            [
                ("kikimr_loglevel", str(default_log_level)),
            ]
        )

    cur_vars.extend(
        [
            ('kikimr_ic_port', ic_port),
            ('kikimr_mon_port', mon_port),
            ('kikimr_home', kikimr_home),
        ]
    )

    if kikimr_binaries_base_path:
        cur_vars.extend(
            [
                ('kikimr_binaries_base_path', kikimr_binaries_base_path),
            ]
        )

    if not new_style_kikimr_cfg:
        cur_vars.extend(BASE_VARS)

    if pq_enable:
        cur_vars.append(("kikimr_pq_file", "${kikimr_config}/pq.txt"))

    if sqs_enable:
        cur_vars.append(("kikimr_sqs_file", "${kikimr_config}/sqs.txt"))
        cur_vars.append(("kikimr_sqs_port", sqs_port))

    if rb_txt_enabled:
        cur_vars.append(("kikimr_rb_file", "${kikimr_config}/rb.txt"))

    if metering_txt_enabled:
        cur_vars.append(("kikimr_metering_file", "${kikimr_config}/metering.txt"))

    if audit_txt_enabled:
        cur_vars.append(("kikimr_audit_file", "${kikimr_config}/audit.txt"))

    if yql_txt_enabled:
        cur_vars.append(("kikimr_yql_file", "${kikimr_config}/yql.txt"))

    if fq_txt_enabled:
        cur_vars.append(("kikimr_yq_file", "${kikimr_config}/yq.txt"))

    if mbus_enabled:
        cur_vars.append(("kikimr_mbus_enable", "--mbus"))
        cur_vars.append(("kikimr_mbus_port", mbus_port))
        cur_vars.append(("kikimr_mbus_infly", DEFAULT_KIKIMR_MBUS_INFLY))
        cur_vars.append(("kikimr_mbus_infly_bytes", DEFAULT_KIKIMR_MBUS_INFLY_BYTES))
        cur_vars.append(("kikimr_mbus_max_message_size", DEFAULT_KIKIMR_MBUS_MAX_MESSAGE_SIZE))

    return '\n'.join(['%s="%s"' % (cur_var[0], cur_var[1]) for cur_var in cur_vars])


def tenant_argument(tenant=None):
    return '' if tenant is None else 'kikimr_arg="${kikimr_arg}${kikimr_tenant:+ --tenant ${kikimr_tenant}}"'


def pq_enable_argument(pq_enable=False):
    return '' if not pq_enable else 'kikimr_arg="${kikimr_arg}${kikimr_pq_file:+ --pq-file ${kikimr_pq_file}}"'


def sqs_arguments(sqs_enable=False):
    return (
        ''
        if not sqs_enable
        else '\n'.join(
            [
                'kikimr_arg="${kikimr_arg}${kikimr_sqs_file:+ --sqs-file ${kikimr_sqs_file}}"',
                'kikimr_arg="${kikimr_arg}${kikimr_sqs_port:+ --sqs-port ${kikimr_sqs_port}}"',
            ]
        )
    )


def rb_arguments(rb_txt_enabled=False):
    return (
        []
        if not rb_txt_enabled
        else [
            'kikimr_arg="${kikimr_arg}${kikimr_rb_file:+ --rb-file ${kikimr_rb_file}}"',
        ]
    )


def metering_arguments(metering_txt_enabled=False):
    return (
        []
        if not metering_txt_enabled
        else [
            'kikimr_arg="${kikimr_arg}${kikimr_metering_file:+ --metering-file ${kikimr_metering_file}}"',
        ]
    )


def audit_arguments(audit_txt_enabled=False):
    return (
        []
        if not audit_txt_enabled
        else [
            'kikimr_arg="${kikimr_arg}${kikimr_audit_file:+ --audit-file ${kikimr_audit_file}}"',
        ]
    )


def yql_arguments(yql_txt_enabled=False):
    return (
        []
        if not yql_txt_enabled
        else [
            'kikimr_arg="${kikimr_arg}${kikimr_yql_file:+ --yql-file ${kikimr_yql_file}}"',
        ]
    )


def mbus_arguments(enable_mbus=False):
    return (
        []
        if not enable_mbus
        else [
            'kikimr_arg="${kikimr_arg}${kikimr_mbus_enable:+ ${kikimr_mbus_enable}}"',
            'kikimr_arg="${kikimr_arg}${kikimr_mbus_port:+ --mbus-port ${kikimr_mbus_port}}"',
            'kikimr_arg="${kikimr_arg}${kikimr_mbus_enable:+ --mbus-worker-count=4}"',
            'kikimr_arg="${kikimr_arg}${kikimr_mbus_infly:+ --mbus-max-in-flight ${kikimr_mbus_infly}}"',
            'kikimr_arg="${kikimr_arg}${kikimr_mbus_infly_bytes:+ --mbus-max-in-flight-by-size ${kikimr_mbus_infly_bytes}}"',
            'kikimr_arg="${kikimr_arg}${kikimr_mbus_max_message_size:+ --mbus-max-message-size ${kikimr_mbus_max_message_size}}"',
        ]
    )


def dynamic_cfg_new_style(
    enable_cores=False,
):
    return "\n".join(
        [
            "kikimr_coregen=\"--core\"" if enable_cores else "",
            NEW_STYLE_DYNAMIC_CFG,
        ]
    )


def kikimr_cfg_for_static_node_new_style(
    ic_port=19001,
    mon_port=8765,
    grpc_port=2135,
    mon_address="",
    enable_cores=False,
    kikimr_home='/Berkanavt/kikimr',
    tenant=None,
    cert_params=None,
    new_style_kikimr_cfg=True,
    mbus_enabled=False,
    pq_enable=True,
):
    return "\n".join(
        [
            local_vars(
                tenant,
                ic_port=ic_port,
                mon_address=mon_address,
                mon_port=mon_port,
                grpc_port=grpc_port,
                kikimr_home=kikimr_home,
                enable_cores=enable_cores,
                cert_params=cert_params,
                default_log_level=None,
                kikimr_binaries_base_path=None,
                new_style_kikimr_cfg=new_style_kikimr_cfg,
                mbus_enabled=mbus_enabled,
                pq_enable=pq_enable,
            ),
            NEW_STYLE_CONFIG,
        ]
        + mbus_arguments(mbus_enabled)
    )


def kikimr_cfg_for_static_node(
    tenant=None,
    ic_port=19001,
    mon_port=8765,
    kikimr_home='/Berkanavt/kikimr',
    pq_enable=True,
    enable_cores=False,
    default_log_level=3,
    kikimr_binaries_base_path='/Berkanavt/kikimr',
    mon_address="",
    cert_params=None,
    rb_txt_enabled=False,
    metering_txt_enabled=False,
    audit_txt_enabled=False,
    yql_txt_enabled=False,
    fq_txt_enabled=False,
    mbus_enabled=False,
):
    return '\n'.join(
        [
            local_vars(
                tenant,
                ic_port=ic_port,
                mon_port=mon_port,
                kikimr_home=kikimr_home,
                kikimr_binaries_base_path=kikimr_binaries_base_path,
                pq_enable=pq_enable,
                enable_cores=enable_cores,
                default_log_level=default_log_level,
                mon_address=mon_address,
                cert_params=cert_params,
                rb_txt_enabled=rb_txt_enabled,
                metering_txt_enabled=metering_txt_enabled,
                audit_txt_enabled=audit_txt_enabled,
                yql_txt_enabled=yql_txt_enabled,
                fq_txt_enabled=fq_txt_enabled,
                mbus_enabled=mbus_enabled,
            ),
            NODE_ID_LOCAL_VAR,
            CUSTOM_CONFIG_INJECTOR,
            # arguments
            DEFAULT_ARGUMENTS_SET,
            NODE_ID_ARGUMENT,
            tenant_argument(tenant),
            pq_enable_argument(pq_enable),
        ]
        + rb_arguments(rb_txt_enabled)
        + metering_arguments(metering_txt_enabled)
        + audit_arguments(audit_txt_enabled)
        + yql_arguments(yql_txt_enabled)
        + mbus_arguments(mbus_enabled)
    )


def kikimr_cfg_for_dynamic_node(
    node_broker_port=2135,
    tenant=None,
    ic_port=19001,
    mon_port=8765,
    kikimr_home='/Berkanavt/kikimr',
    sqs_port=8771,
    sqs_enable=False,
    enable_cores=False,
    default_log_level=3,
    kikimr_binaries_base_path='/Berkanavt/kikimr',
    mon_address="",
    cert_params=None,
    rb_txt_enabled=False,
    metering_txt_enabled=False,
    audit_txt_enabled=False,
    yql_txt_enabled=False,
    fq_txt_enabled=False,
):
    return "\n".join(
        [
            local_vars(
                tenant,
                node_broker_port=node_broker_port,
                ic_port=ic_port,
                mon_port=mon_port,
                kikimr_home=kikimr_home,
                kikimr_binaries_base_path=kikimr_binaries_base_path,
                sqs_port=sqs_port,
                sqs_enable=sqs_enable,
                enable_cores=enable_cores,
                default_log_level=default_log_level,
                mon_address=mon_address,
                cert_params=cert_params,
                rb_txt_enabled=rb_txt_enabled,
                metering_txt_enabled=metering_txt_enabled,
                audit_txt_enabled=audit_txt_enabled,
                yql_txt_enabled=yql_txt_enabled,
                fq_txt_enabled=fq_txt_enabled,
            ),
            CUSTOM_CONFIG_INJECTOR,
            # arguments
            DEFAULT_ARGUMENTS_SET,
            NODE_BROKER_ARGUMENT,
            tenant_argument(tenant),
            sqs_arguments(sqs_enable),
        ]
        + rb_arguments(rb_txt_enabled)
        + metering_arguments(metering_txt_enabled)
        + audit_arguments(audit_txt_enabled)
    )


def expected_vars(**kwargs):
    kikimr_patched = []
    exclude_options = kwargs.pop('exclude_options', [])
    for name, val in sorted(kwargs.items()):
        if not name.startswith('kikimr_'):
            name = "kikimr_" + name
        if val is None:
            val = '${{{bash_var}:? expected not empty var}}'.format(bash_var=name)

        kikimr_patched.append((name, val))

    kikimr_patched.extend(BASE_VARS)

    return '\n'.join(['%s="%s"' % (var[0], var[1]) for var in kikimr_patched if var[0] not in exclude_options])


def kikimr_cfg_for_dynamic_slot(
    enable_cores=False,
    cert_params=None,
    rb_txt_enabled=False,
    metering_txt_enabled=False,
    audit_txt_enabled=False,
    yql_txt_enabled=False,
    fq_txt_enabled=False,
):
    return "\n".join(
        [
            'kikimr_coregen="--core"' if enable_cores else "",
            expected_vars(
                node_broker_port=2135,
                kikimr_binaries_base_path="/Berkanavt/kikimr",
                mon_address="",
                tenant=None,
                mbus_port=None,
                grpc_port=None,
                ic_port=None,
                mon_port=None,
                home=None,
                syslog_service_tag=None,
                exclude_options=['kikimr_system_file'],
            ),
        ]
        + ['kikimr_%s=%s' % (key, value) for key, value in zip(['ca', 'cert', 'key'], cert_params or []) if value]
        + [
            CUSTOM_CONFIG_INJECTOR,
            CUSTOM_SYS_CONFIG_INJECTOR,
            # arguments
            DEFAULT_ARGUMENTS_SET,
            NODE_BROKER_ARGUMENT,
            SYS_LOG_SERVICE_TAG,
            tenant_argument(True),
        ]
        + rb_arguments(rb_txt_enabled)
        + metering_arguments(metering_txt_enabled)
        + audit_arguments(audit_txt_enabled)
        + yql_arguments(yql_txt_enabled)
    )
