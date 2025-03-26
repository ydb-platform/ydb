#!/usr/bin/env bash

# Steps to check this proxy with 'kind' cluster.
# 1. Get local ydb. https://ydb.tech/docs/en/quickstart
# 2. Build etcd_proxy.
# 3. Copy this script and etcd_proxy binary into local ydbd folder.
# 4. Create kind cluster and use extraMounts parameter in config to mount local ydbd folder with proxy into container.
# 5. In container replace etcd port in /etc/kubernetes/manifests/kube-apiserver.yaml from 2379 to 2479.
# 6. Execute 'cd ydbd && ./run_etcd.sh' in container.


./start.sh ram

./etcd_proxy --endpoint=localhost:2136 --database=/Root/test --ca=/etc/kubernetes/pki/etcd/ca.crt  --cert=/etc/kubernetes/pki/apiserver-etcd-client.crt --key=/etc/kubernetes/pki/apiserver-etcd-client.key --init --import-from=127.0.0.1:2379 --import-prefix=/registry/

./etcd_proxy --port=2479 --endpoint=localhost:2136 --database=/Root/test --ca=/etc/kubernetes/pki/etcd/ca.crt --cert=/etc/kubernetes/pki/etcd/server.crt --key=/etc/kubernetes/pki/etcd/server.key | tee etcd.log &
