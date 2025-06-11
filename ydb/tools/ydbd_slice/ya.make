RECURSE(
    bin
)

PY3_LIBRARY(ydbd_slice)

PY_SRCS(
    __init__.py
    cluster_description.py
    yaml_configurator.py
    config_client.py
    kube/__init__.py
    kube/api.py
    kube/cms.py
    kube/docker.py
    kube/dynconfig.py
    kube/generate.py
    kube/utils.py
    kube/handlers.py
    kube/kubectl.py
    kube/yaml.py
    nodes.py
    handlers.py
)

PEERDIR(
    ydb/tools/cfg
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/PyYAML
    contrib/python/ruamel.yaml
    contrib/python/kubernetes
    contrib/python/Jinja2
    contrib/python/grpcio
    contrib/python/tenacity
)

RESOURCE(
    kube/templates/common/namespace.yaml /ydbd_slice/templates/common/namespace.yaml
    kube/templates/common/database.yaml /ydbd_slice/templates/common/database.yaml
    kube/templates/common/dynconfig.yaml /ydbd_slice/templates/common/dynconfig.yaml
    kube/templates/common/obliterate.yaml /ydbd_slice/templates/common/obliterate.yaml
    kube/templates/8-node-block-4-2/nodeclaim.yaml /ydbd_slice/templates/8-node-block-4-2/nodeclaim.yaml
    kube/templates/8-node-block-4-2/storage.yaml /ydbd_slice/templates/8-node-block-4-2/storage.yaml
    kube/templates/legacy-cms-config-items/table-profile.txt /ydbd_slice/templates/legacy-cms-config-items/table-profile.txt
    kube/templates/legacy-cms-config-items/unified-agent.txt /ydbd_slice/templates/legacy-cms-config-items/unified-agent.txt

    baremetal/templates/block-4-2-8-nodes.yaml /ydbd_slice/baremetal/templates/block-4-2-8-nodes.yaml
    baremetal/templates/block-4-2-4-nodes.yaml /ydbd_slice/baremetal/templates/block-4-2-4-nodes.yaml
    baremetal/templates/block-4-2-2-nodes.yaml /ydbd_slice/baremetal/templates/block-4-2-2-nodes.yaml
    baremetal/templates/mirror-3-dc-9-nodes.yaml /ydbd_slice/baremetal/templates/mirror-3-dc-9-nodes.yaml
    baremetal/templates/mirror-3-dc-3-nodes.yaml /ydbd_slice/baremetal/templates/mirror-3-dc-3-nodes.yaml
    baremetal/templates/none-1-node.yaml /ydbd_slice/baremetal/templates/none-1-node.yaml
)

END()
