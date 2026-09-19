For example, with `sub: user1`, `groups: [admins, developers]` and the default domain, the following SIDs will be generated:

- user `user1@sso`;
- groups `admins@sso` and `developers@sso`.

{{ ydb-short-name }} uses the list of groups from the token without additional calls to the IdP and without expanding nested groups. Managing users and groups of an external IdP using commands `CREATE USER`, `ALTER USER`, `CREATE GROUP`, and `ALTER GROUP` is not allowed. Rights are assigned to the generated SIDs using the methods described in the section [{#T}](./authorization.md).

### Server configuration

Authentication via an external IdP is enabled in the [authentication configuration](../reference/configuration/auth_config.md#external-idp-auth-config) when the `external_idp_config` section is present.

## Client authentication by certificate {#client-certificate}

{{ ydb-short-name }} can authenticate a client based on the client certificate received during TLS connection establishment. The check is performed at the application protocol level (gRPC, etc.) when the server is already accepting requests over an open connection.

This method is suitable, for example, in corporate scenarios with centralized certificate issuance.

### How it works

1. The client establishes a TLS connection with the server {{ ydb-short-name }}, providing the client certificate (and trust chain).
2. When processing a request, the server extracts the certificate from the TLS context.
3. The server uses the certificate for authentication and validates it according to the rules of the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section.
4. Upon successful certificate validation, the client is assigned a security identifier [SID](../concepts/glossary.md#access-sid), which has all the [rights](../concepts/glossary.md#access-right) assigned to the corresponding identifier.

Certificate authentication is applied only to requests without an [authentication token](../concepts/glossary.md#auth-token). If the client provides an authentication token — for example, in the `Authorization` header for HTTP or through SDK/CLI mechanisms for IAM, login, and password — the token takes precedence. In this case, the certificate is transmitted at the TLS level but is not used for authentication.

### SID generation

{% note info %}

Client certificate validation during [device authentication](#device-auth) and user authentication by client certificate are different mechanisms. Device authentication restricts the network perimeter without generating a SID; user authentication by client certificate generates a SID and groups for [authorization](./authorization.md).

{% endnote %}

Successful certificate authentication creates a user SID with the suffix `@<domain>`, where `<domain>` is the [value of the parameter](../reference/configuration/auth_config.md#certificate-auth-config) `certificate_authentication_domain` in the `auth_config` section (default: `cert`). The name is formed from all attributes of the Subject field of the certificate in `Имя=Значение,...@<domain>` notation. The order of attributes corresponds to the order of fields in the certificate. Example:

```text
C=RU,ST=MSK,O=MyOrg,CN=account1.apps.example.net@cert
```

### Obtaining groups

If the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section contains `client_certificate_definitions` blocks, the certificate is accepted if it matches at least one of them. For each matching block, the client is included in the groups from `member_groups`. If `member_groups` is not specified, the default group is used — `default_group` (default value: `DefaultClientAuth@cert`).

### Server configuration

Certificate validation and group assignment rules are set in the [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) section of the cluster's static configuration. To enable client certificate requests during TLS handshake over gRPC, set the `request_client_certificate: true` parameter.

### Client configuration

For more details on configuring [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), see the section [TLS connection parameters](../reference/ydb-cli/connect.md#tls).

## Device authentication by certificate {#device-auth}

Device authentication is the validation of the [client certificate](../concepts/glossary.md#client-certificate) during TLS connection establishment; [SID](../concepts/glossary.md#access-sid) is not generated in this case. If a certificate is presented, the trust chain to the CA is checked; an untrusted certificate leads to connection rejection before application requests are processed. The requirement to present a certificate depends on the interface (see  section [Usage in {{ ydb-short-name }}](#device-auth-interfaces)).

### Why device authentication is needed {#device-auth-motivation}

Device authentication addresses the following tasks in {{ ydb-short-name }}:

1. Cluster isolation — restrict the set of hosts and applications that can establish TLS connections to {{ ydb-short-name }} nodes.

2. Protection against configuration errors — prevent connections to foreign clusters {{ ydb-short-name }}, for example, with an incorrect [node-broker](../devops/configuration-management/configuration-v1/node-authorization.md) parameter, a dynamic node will not connect to a foreign cluster, whereas with regular TLS such a connection could be established.

3. Complicating application-level attacks — a process on a foreign host without a suitable certificate does not get access to the cluster API, even if a network route to the port exists.

After passing device authentication, [authentication](./authentication.md) of a user or application may be required to access data. This can be done not only via a verified client certificate, but also using other authentication methods in {{ ydb-short-name }}, for example, by [login and password](./authentication.md#static-credentials).

### How it works {#device-auth-how-it-works}

1. The client establishes a TLS connection with the server {{ ydb-short-name }} and, if the connection interface requires it, presents a client certificate.
2. If a certificate is presented, the server validates it at the TLS level: trust chain to the configured certificate authority (CA), validity period, etc. p. Additional rules for matching certificate fields (e.g., `require_same_issuer`, Subject, and SAN) are applied during [client authentication by certificate](./authentication.md#client-certificate).
3. Upon successful validation, the connection is opened; upon failure, it is rejected.

### Usage in {{ ydb-short-name }} {#device-auth-interfaces}

Device authentication is optional and configured independently: the mechanism can be enabled on some ports and disabled on others.

- **Interconnect** — when TLS is enabled in the [interconnect_config](../reference/configuration/tls.md#interconnect) section, [Interconnect](../concepts/glossary.md#actor-system-interconnect) requires a client certificate.

- **Kafka API** — when mTLS is enabled, it requires a client certificate; only the trust chain to the CA is checked, and a connection without a certificate or with an untrusted certificate is not established. Server configuration is described in the [kafka_proxy_config](../reference/configuration/kafka_proxy_config.md) section, and client connection in the section [Device authentication via mTLS](../reference/kafka-api/auth.md#device-auth).

- **gRPC** and **YDB Monitoring** — you can enable a client certificate request for device authentication, and also separately enable mandatory validation (an untrusted certificate is always rejected). gRPC configuration is described in the [grpc_config](../reference/configuration/tls.md#grpc) and [client_certificate_authorization](../reference/configuration/client_certificate_authorization.md) sections, and client connection in the section [TLS connection parameters](../reference/ydb-cli/connect.md#tls); YDB Monitoring configuration is described in the [monitoring_config](../reference/configuration/monitoring_config.md#tls) section.

## Authentication using a third-party IAM provider {#iam}

* **Access Token** — a fixed token is set as a parameter for the client (SDK or CLI) and is passed in requests.
* **Refresh Token** — an [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a personal account is set as a parameter for the client (SDK or CLI), based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token passed in requests.
* **Service Account Key** — attributes of a service account and a signing key are set as parameters for the client (SDK or CLI), based on which the client periodically accesses the IAM API in the background to rotate (obtain the next) token passed in requests.
* **Metadata** — the client (SDK or CLI) periodically accesses a local service to rotate (obtain the next) token passed in requests.
* **OAuth 2.0 token exchange** - the client (SDK or CLI) exchanges a token of another type for an access token using the [OAuth 2.0 token exchange protocol](https://www.rfc-editor.org/rfc/rfc8693),, which is then passed in requests to the {{ ydb-short-name }} API.

Any holder of a valid token can gain access to perform operations, so the main task of the security system is to ensure token secrecy and prevent its compromise.

Authentication modes with token rotation **Refresh Token** and **Service Account Key** provide a higher level of security compared to the fixed-token mode **Access Token**, since only short-lived secrets are transmitted over the network to the server {{ ydb-short-name }}.

Maximum security and performance are achieved when using the **Metadata** mode, as it eliminates the need to handle secrets during application deployment and also allows contacting IAM and caching the token in advance, before application startup.

When choosing an authentication mode among those supported by the server and environment, the following recommendations should be followed:

* **Anonymous** is typically used on self-deployed local clusters {{ ydb-short-name }} that are not accessible over the network.
* **Access Token** is used when other modes are not supported on the server side or for configuration/debugging purposes. It does not require client interactions with IAM. However, if IAM supports an API for token rotation, fixed tokens issued by such IAM usually have a short lifetime, which forces regular manual renewal in IAM.
* **Refresh Token** can be used for one-off manual operations under a personal account, for example, related to data maintenance in the database, performing ad-hoc operations in CLI, or running applications from a workstation. Such a token can be obtained manually in IAM once for a long period and stored in an environment variable on a personal workstation for automatic use when launching CLI without additional authentication parameters.
* **Service Account Key** is primarily used for applications designed to run in environments that support the **Metadata** mode, when testing them outside such environments (e.g., on a workstation). It can also be used for applications outside such environments, acting as an analog of **Refresh Token** for service accounts. Unlike a personal account, the access objects and roles of a service account can be limited.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to be specified in parameters can be obtained from the IAM system associated with a specific installation of {{ ydb-short-name }}. In particular, for the service {{ ydb-short-name }} in {{ yandex-cloud }}, Yandex.Passport OAuth and service accounts {{ yandex-cloud }} are used. When using {{ ydb-short-name }} in corporate contexts, standard centralized authentication systems for the organization may be used.

When using modes that involve the client {{ ydb-short-name }} contacting IAM, an IAM URL providing the token issuance API can additionally be specified. By default, existing SDKs and CLIs attempt to access the IAM API {{ yandex-cloud }} hosted on `iam.api.cloud.yandex.net:443`.
