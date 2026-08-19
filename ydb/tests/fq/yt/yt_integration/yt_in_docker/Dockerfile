# YT Saurus local stable image — same as used by run_local_cluster.sh.
# The stable image has full queue CLI support (pull-queue-consumer, etc.).
FROM ghcr.io/ytsaurus/local:stable

# Required by the stable image bootstrap scripts.
ENV YT_FORCE_IPV4=1
ENV YT_FORCE_IPV6=0
ENV YT_USE_HOSTS=0
ENV YTSERVER_ALL_PATH=/usr/bin/ytserver-all
ENV YT_LOCAL_ROOT_PATH=/tmp

EXPOSE 80
EXPOSE 8443

ENTRYPOINT ["yt_local", "start", \
  "--proxy-port", "80", \
  "--rpc-proxy-port", "8443", \
  "--rpc-proxy-count", "1", \
  "--queue-agent-count", "1", \
  "--fqdn", "localhost", \
  "--address-resolver-config", "{enable_ipv4=%true;enable_ipv6=%false;}", \
  "--native-client-supported", \
  "--id", "locasaurus", \
  "--sync"]
