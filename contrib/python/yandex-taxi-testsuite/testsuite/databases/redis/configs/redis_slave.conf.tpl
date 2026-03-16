daemonize yes
protected-mode no
port ${port}
tcp-backlog 511
bind ${host}
timeout 0
tcp-keepalive 0
loglevel notice
databases 16
save ""

slaveof ${host} ${master_port}
