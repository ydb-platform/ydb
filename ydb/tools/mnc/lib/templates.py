from . import deploy_ctx


test_kikimr_dynamic_systemd = '''[Unit]
Description=kikimr daemon
StartLimitInterval=10
StartLimitBurst=15
AssertFileNotEmpty=/Berkanavt/test_kikimr_dynamic_%NODE_IDX%/cfg/dynamic_server_%NODE_IDX%.cfg
After=network.target

[Service]
Type=simple
User=kikimr
LimitNOFILE=45000
LimitCORE=0
LimitMEMLOCK=32212254720

SuccessExitStatus=
SuccessExitStatus=0

PermissionsStartOnly=true
CapabilityBoundingSet=CAP_SETFCAP CAP_SYS_RAWIO CAP_SYS_NICE
RuntimeDirectory=kikimr

StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=kikimr
SyslogFacility=daemon
SyslogLevel=err

ExecStartPre=/sbin/setcap CAP_SYS_RAWIO,CAP_SYS_NICE=ep /Berkanavt/kikimr/bin/kikimr
ExecStart=/bin/sh -c '. /Berkanavt/test_kikimr_dynamic_%NODE_IDX%/cfg/dynamic_server_%NODE_IDX%.cfg'''\
    ''' && if [ -f "/usr/lib/libbreakpad_init.so" ]; then export LD_PRELOAD=libbreakpad_init.so; export BREAKPAD_MINIDUMPS_PATH=/Berkanavt/minidumps/; fi;'''\
    ''' exec /Berkanavt/kikimr/bin/kikimr $kikimr_arg &> /Berkanavt/test_kikimr_dynamic_%NODE_IDX%/logs/kikimr.start'
ExecStartPost=/bin/sh -c 'echo $MAINPID > /run/kikimr/test_kikimr_dynamic_%NODE_IDX%.pid'

KillMode=mixed
TimeoutStopSec=300

Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
'''

test_kikimr_dynamic = '''description	"kikimr MULTITENANCY"
env tenant
env grpc
env mbus
env ic
env mon
env kikimr_main_dir=/Berkanavt/test_kikimr_dynamic_%NODE_IDX%
env tenant_main_dir=/Berkanavt/test_kikimr_dynamic_%NODE_IDX%/tenants
env user=kikimr

kill timeout 300

limit nofile 45000 45000
limit core unlimited unlimited
limit memlock 32212254720 32212254720

console log

script

    [ ! -d $kikimr_main_dir/cfg ] && logger -p daemon.err -t kikimr_$slot "No conf dir" && exit 1

    [ ! -s /Berkanavt/test_kikimr_dynamic_%NODE_IDX%/cfg/dynamic_server_%NODE_IDX%.cfg ] && logger -p daemon.err -t kikimr_$slot "No dynamic server config" && exit 1
    #import kikimr_arg
    [ -s ${tenant_main_dir}/location ] && location=" --data-center $(cat ${tenant_main_dir}/location)"
    [ -s /Berkanavt/kikimr_$slot/sys.txt ] && kikimr_system_file="$kikimr_main_dir/cfg/sys.txt"
    . /Berkanavt/test_kikimr_dynamic_%NODE_IDX%/cfg/dynamic_server_%NODE_IDX%.cfg

kikimr_user="${user}"
kikimr_dir="$kikimr_main_dir"
kikimr_bin_path="/Berkanavt/kikimr/bin"
kikimr_pid_path="/run/${user}"
#kikimr_coregen="--core"
kikimr_log="daemon.err"

install -o ${user} -d $kikimr_pid_path
install -o root -d $kikimr_dir
install -o syslog -d $kikimr_dir/logs
install -o ${user} -d $kikimr_dir/cache

if [ -f "/usr/lib/libbreakpad_init.so" ]; then
    export LD_PRELOAD=libbreakpad_init.so
    export BREAKPAD_MINIDUMPS_PATH=/Berkanavt/minidumps/
fi

exec /usr/bin/daemon --name=test_kikimr_dynamic_%NODE_IDX% \
                     --user=${kikimr_user} \
                     --pidfile=${kikimr_pid_path}/test_kikimr_dynamic_%NODE_IDX%.pid \
                     --unsafe \
                     ${kikimr_coregen} \
                     --respawn \
                     --delay=10 \
                     --output=/Berkanavt/test_kikimr_dynamic_%NODE_IDX%/logs/kikimr.start \
                     --errlog=/Berkanavt/test_kikimr_dynamic_%NODE_IDX%/logs/kikimr.start \
                     -- ${kikimr_bin_path}/kikimr ${kikimr_arg} ${location}

end script

post-start script
    /sbin/reload rsyslog
    if [ ! -z $tenant ] && [ ! -z $grpc ] && [ ! -z $mbus ] && [ ! -z $ic ] && [ ! -z $mon ]; then
        install -o root -d ${kikimr_main_dir}_${slot}
        printf "tenant=${tenant}\ngrpc=${grpc}\nmbus=${mbus}\nic=${ic}\nmon=${mon}\n" > ${kikimr_main_dir}_${slot}/slot_cfg
    fi
end script
'''

test_kikimr_systemd = '''[Unit]
Description=kikimr daemon
StartLimitInterval=10
StartLimitBurst=15
AssertFileNotEmpty=/Berkanavt/test_kikimr_static_%NODE_IDX%/cfg/kikimr-%NODE_IDX%.cfg
After=network.target

[Service]
Type=simple
User=kikimr
LimitNOFILE=45000
LimitCORE=0
LimitMEMLOCK=32212254720

SuccessExitStatus=
SuccessExitStatus=0

PermissionsStartOnly=true
CapabilityBoundingSet=CAP_SETFCAP CAP_SYS_RAWIO CAP_SYS_NICE
RuntimeDirectory=kikimr

StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=kikimr
SyslogFacility=daemon
SyslogLevel=err

ExecStartPre=/sbin/setcap CAP_SYS_RAWIO,CAP_SYS_NICE=ep /Berkanavt/kikimr/bin/kikimr
ExecStart=/bin/sh -c '. /Berkanavt/test_kikimr_static_%NODE_IDX%/cfg/kikimr-%NODE_IDX%.cfg'''\
    ''' && if [ -f "/usr/lib/libbreakpad_init.so" ]; then export LD_PRELOAD=libbreakpad_init.so; export BREAKPAD_MINIDUMPS_PATH=/Berkanavt/minidumps/; fi;'''\
    ''' exec /Berkanavt/kikimr/bin/kikimr $kikimr_arg &> /Berkanavt/test_kikimr_static_%NODE_IDX%/logs/kikimr.start'
ExecStartPost=/bin/sh -c 'echo $MAINPID > /run/kikimr/test_kikimr_static_%NODE_IDX%.pid'

KillMode=mixed
TimeoutStopSec=300

Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
'''

test_kikimr = '''description	"kikimr daemon"

start on runlevel [2345]
stop on runlevel [!2345]

kill timeout 300

limit nofile 45000 45000
limit core unlimited unlimited
limit memlock 32212254720 32212254720

script

        [ ! -e /Berkanavt/test_kikimr_static_%NODE_IDX%/cfg/kikimr-%NODE_IDX%.cfg ] && logger -p daemon.err -t kikimr "No conf file" && exit 1
        . /Berkanavt/test_kikimr_static_%NODE_IDX%/cfg/kikimr-%NODE_IDX%.cfg
        /sbin/setcap CAP_SYS_RAWIO,CAP_SYS_NICE=ep /Berkanavt/kikimr/bin/kikimr || true

kikimr_user=kikimr
kikimr_dir="/Berkanavt/test_kikimr_static_%NODE_IDX%"
kikimr_bin_path="/Berkanavt/kikimr/bin"
kikimr_pid_path="/run/kikimr"
#kikimr_coregen="--core"
kikimr_log="daemon.err"

install -o kikimr -d $kikimr_pid_path

if [ -f "/usr/lib/libbreakpad_init.so" ]; then
    export LD_PRELOAD=libbreakpad_init.so
    export BREAKPAD_MINIDUMPS_PATH=/Berkanavt/minidumps/
fi

exec /usr/bin/daemon --name=test_kikimr_static_%NODE_IDX% \
                     --user=${kikimr_user} \
                     --pidfile=${kikimr_pid_path}/test_kikimr_static_%NODE_IDX%.pid \
                     --unsafe \
                     ${kikimr_coregen} \
                     --respawn \
                     --delay=10 \
                     --output=/Berkanavt/test_kikimr_static_%NODE_IDX%/logs/kikimr.start \
                     --errlog=/Berkanavt/test_kikimr_static_%NODE_IDX%/logs/kikimr.start \
                     -- ${kikimr_bin_path}/kikimr ${kikimr_arg}

end script
'''

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

configure_domain = '''ConfigureRequest {
  Actions {
    AddConfigItem {
      ConfigItem {
        Config {
          ActorSystemConfig {
Executor {
  Type: BASIC
  Threads: 2
  SpinThreshold: 1
  Name: "System"
}
Executor {
  Type: BASIC
  Threads: 4
  SpinThreshold: 1
  Name: "User"
}
Executor {
  Type: BASIC
  Threads: 2
  SpinThreshold: 1
  Name: "Batch"
}
Executor {
  Type: IO
  Threads: 1
  Name: "IO"
}
Executor {
  Type: BASIC
  Threads: 1
  SpinThreshold: 10
  Name: "IC"
  TimePerMailboxMicroSecs: 100
}
Scheduler {
  Resolution: 64
  SpinThreshold: 0
  ProgressThreshold: 10000
}
SysExecutor: 0
UserExecutor: 1
IoExecutor: 3
BatchExecutor: 2
ServiceExecutor {
  ServiceName: "Interconnect"
  ExecutorId: 4
}
          }
        }
        MergeStrategy: 3
      }
    }
  }
}
'''


def make_templates():
    with open(f'{deploy_ctx.work_directory}/init/test_kikimr_dynamic_systemd.template', 'w') as file:
        print(test_kikimr_dynamic_systemd, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/init/test_kikimr_dynamic.template', 'w') as file:
        print(test_kikimr_dynamic, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/init/test_kikimr_systemd.template', 'w') as file:
        print(test_kikimr_systemd, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/init/test_kikimr.template', 'w') as file:
        print(test_kikimr, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/replaced/dynamic_server.template', 'w') as file:
        print(dynamic_server, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/replaced/kikimr.template', 'w') as file:
        print(kikimr, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/replaced/log.template', 'w') as file:
        print(log, end='', file=file)
    with open(f'{deploy_ctx.work_directory}/special_dynamic/Configure-domain.txt', 'w') as file:
        print(configure_domain, end='', file=file)
