# Configuring authentication and authorization of database nodes

Node authentication in the {{ ydb-short-name }} cluster verifies the authenticity of database nodes when making service calls to other nodes over the [gRPC](https://grpc.io/) protocol. Node authorization verifies and grants the necessary permissions when processing service calls, including operations for registering launched nodes in the cluster and accessing [configuration management](../configuration-management/index.md). Using node authentication and authorization is recommended for all {{ ydb-short-name }} clusters, as it helps avoid unauthorized access to data through the inclusion of attacker-controlled nodes in the cluster.

Before a [dynamic node](../../concepts/glossary.md#dynamic) of the database joins the cluster and starts exchanging data with other nodes over the internal [Interconnect](../../concepts/glossary.md#actor-system-interconnect) protocol, it must register with the already running [storage nodes](../../concepts/glossary.md#storage-node). To do this, the node being started establishes a gRPC connection with one of the storage nodes specified in the `--node-broker` option and performs service calls over this connection, including a registration request. It is precisely these gRPC calls that are protected by node authentication and authorization: without successful verification, the node does not get the right to register and cannot join the cluster.

## What you need to know before you begin

**Node identifiers:**

- **NodeId** — a unique numeric identifier of a node in the cluster (from 1 to 1048575).
- **SID** (Subject IDentifier) is a unique identifier of an access subject that determines which operations a node can perform (for example, `registerNode@cert` means "a node with registration rights").

**Purpose of authentication.** Ensure that the new database node is an authorized node. This is done using [certificates](../../reference/configuration/client_certificate_authorization.md).

## How node verification works

1. The database node being started opens a gRPC connection to one of the storage nodes specified in the `--node-broker` option. The connection is established over TLS, and the certificate of the launched node, specified in the `--grpc-cert` option, is used as the client certificate.
2. Storage node and database node perform [mTLS](../../concepts/glossary.md#mtls): each node verifies the other's certificate. The trust chain is checked (the certificate is signed by a known certificate authority) and the host name matches the value of the "Subject Name" field of the certificate.
3. The storage node checks the "Subject" field of the certificate for compliance with the requirements specified in the static configuration in the [client_certificate_authorization](../../reference/configuration/client_certificate_authorization.md) section. Thus, the storage node ensures that the certificate is issued specifically for the nodes of your cluster. For example, you can require that all node certificates have the organization (O) = "MyCompany", which will make it difficult for attackers to forge certificates.
4. If all checks are successful (the certificate is authentic, the trust chain is in order, and the Subject field meets the requirements), the storage node trusts the connection and assigns it an access subject identifier (SID) — a string like registerNode@cert, specified in the configuration in the member_groups parameter. The SID determines which operations the node can perform (in this case, register in the cluster).
5. The database node uses a trusted gRPC connection to "introduce itself" to the storage node. The database node sends a service request in which it passes the IP address and port through which connections from other cluster nodes will be accepted. The storage node remembers the received information — this is called registration in the cluster.
6. The storage node checks whether the SID assigned to the connection at the mTLS stage has the right to register nodes. To do this, the storage node looks at the configuration parameter `register_dynamic_node_allowed_sids` — this is a list of SIDs that are allowed to register new database nodes in the cluster. If the node's SID (for example, `registerNode@cert`) is in this list, registration is allowed. If not, the node is rejected as unauthorized.

   After successful verification, the storage node performs three actions:

   - Assigns a unique numeric identifier — **NodeId** (for example, NodeId=50001) — to the database node.
   - Writes the mapping: NodeId=50001 → IP address 192.168.1.100:19002 to the cluster node service registry.
   - This registry is used by other cluster nodes for routing (where to send a query to a node with NodeId=50001).
7. Now the database node is ready for full operation in the cluster. The database node connects to other nodes in the cluster via the Interconnect service protocol and transmits its assigned NodeId (for example, 50001) and its network address (host:port). Other cluster nodes use this information to route queries. If a node tries to connect with an unknown NodeId or IP address (which is not in the registry), other nodes reject it.

The node authentication mode during registration is set by the [node_registration_token](../../reference/configuration/auth_config.md#node-registration-token) parameter in the `auth_config` section. By default, authentication is disabled. This mode is only allowed in fully isolated test environments.

## Configuring authentication and authorization for database nodes

To enable node authentication and authorization, you need to prepare the certificate infrastructure and configure node startup using TLS certificates before changing the cluster configuration.

1. Make sure that [gRPC traffic encryption is configured](../../reference/configuration/tls.md#grpc) using TLS in the deployed {{ ydb-short-name }} cluster.
2. When issuing certificates for nodes, set uniform rules for filling in the "Subject" field so that you can distinguish cluster node certificates. For details, see [documentation on certificate verification rules](../../reference/configuration/client_certificate_authorization.md).

    {% note info %}

   An example of a [script for generating self-signed certificates](https://github.com/ydb-platform/ydb/blob/main/ydb/deploy/tls_cert_gen/) for {{ ydb-short-name }} nodes fills the "Subject" field with the value `O=YDB` for all nodes. The examples in this article are designed for exactly this variant of filling the "Subject" field.

    {% endnote %}
3. In the command-line parameters for [starting database nodes](../deployment-options/manual/initial-deployment/deployment-configuration-v1.md#start-dynnode), add options with paths to certificates and the key:

   | **Command-line option** | **Description** |
   | --- | --- |
   | `--grpc-ca` | Path to the certificate file of trusted certification authorities `ca.crt`. |
   | `--grpc-cert` | Path to the `node.crt` node certificate file. |
   | `--grpc-key` | Path to the secret key file of node `node.key`. |

   Example of launching a database node:


    ```bash
    /opt/ydb/bin/ydbd server --yaml-config  /opt/ydb/cfg/config.yaml --tenant /Root/testdb \
        --grpcs-port 2136 --grpc-ca /opt/ydb/certs/ca.crt \
        --grpc-cert /opt/ydb/certs/node.crt --grpc-key /opt/ydb/certs/node.key \
        --ic-port 19002 --ca /opt/ydb/certs/ca.crt \
        --mon-port 8766 --mon-cert /opt/ydb/certs/web.pem \
        --node-broker grpcs://<ydb1>:2135 \
        --node-broker grpcs://<ydb2>:2135 \
        --node-broker grpcs://<ydb3>:2135
    ```

## Enabling node authentication and authorization mode

To enable mandatory authorization of database nodes, update the [static cluster configuration](../../reference/configuration/index.md):

1. At the root level, add a block `client_certificate_authorization` with requirements for the "Subject" field of the certificates of connecting nodes.


    ```yaml
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["registerNode@cert"]
          subject_terms:
            - short_name: "O"
              values: ["YDB"]
    ```


   If necessary, add additional certificate checks according to the [`client_certificate_authorization` documentation](../../reference/configuration/client_certificate_authorization.md).

   If the certificate is successfully verified and the components of its "Subject" field match the requirements specified in `subject_terms`, the connection will be assigned the access subjects (SIDs) listed in `member_groups`. To distinguish such subjects from other groups and accounts, they must be named with the suffix `@cert`.
2. In the `security_config` block, add the `register_dynamic_node_allowed_sids` parameter with a list of SIDs that are allowed to register database nodes. Example:


    ```yaml
    domains_config:
      ...
      security_config:
        enforce_user_token_requirement: true
        ...
        register_dynamic_node_allowed_sids:
          - "root@builtin"
          - "registerNode@cert"
    ```


   Including `root@builtin` in this list is mandatory for technical reasons: some internal cluster processes use it regardless of the node authentication mode.

   For details on cluster authentication parameters, see the [`security_config` documentation](../../reference/configuration/security_config.md#security-access-levels).
3. In the `auth_config` block, set the `node_registration_token` parameter to an empty string:


    ```yaml
    auth_config:
      ...
      node_registration_token: ""
    ```


   For details about the `node_registration_token` parameter, see the [`auth_config` documentation](../../reference/configuration/auth_config.md#node-registration-token).
4. Update the static configuration on all cluster nodes manually or via the [Ansible playbook](../deployment-options/ansible/update-config.md).
5. Perform a staged restart of storage nodes [using ydbops](../../reference/ydbops/rolling-restart-scenario.md) or via an [Ansible playbook](../deployment-options/ansible/restart.md).
6. Perform a staged restart of the cluster database nodes using ydbops or an Ansible playbook.
