RECURSE(
    bin
)

PY3_LIBRARY(ydbd_slice)

PY_SRCS(
    __init__.py
    cluster_description.py
    kube/__init__.py
    kube/api.py
    kube/cms.py
    kube/docker.py
    kube/dynconfig.py
    kube/generate.py
    kube/handlers.py
    kube/kubectl.py
    kube/yaml.py
    nodes.py
    handlers.py
)

PEERDIR(
    ydb/tools/cfg
    ydb/public/sdk/python
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
    kube/templates/8-node-block-4-2/nodeclaim.yaml /ydbd_slice/templates/8-node-block-4-2/nodeclaim.yaml
    kube/templates/8-node-block-4-2/storage.yaml /ydbd_slice/templates/8-node-block-4-2/storage.yaml
    kube/templates/legacy-cms-config-items/table-profile.txt /ydbd_slice/templates/legacy-cms-config-items/table-profile.txt
    kube/templates/legacy-cms-config-items/unified-agent.txt /ydbd_slice/templates/legacy-cms-config-items/unified-agent.txt
)

END()
