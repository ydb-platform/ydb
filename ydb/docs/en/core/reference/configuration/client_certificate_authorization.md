# client_certificate_authorization

The `client_certificate_authorization` section contains settings for requesting a [client certificate](../../concepts/glossary.md#client-certificate) during [device authentication](../../security/authentication.md#device-auth-interfaces) over gRPCs and defines rules for verifying client SSL certificates and generating [SID](../../concepts/glossary.md#access-sid) of users [when authenticating using a client certificate](../../security/authentication.md#client-certificate). The settings are specified in the [static configuration](./index.md) of the cluster. The nested block `client_certificate_definitions` defines requirements for filling in the "Subject" and "Subject Alternative Name" fields of client certificates, as well as a list of assigned SID groups.

Client certificates are described by the [X.509](https://en.wikipedia.org/wiki/X.509) standard. The "Subject" field of a certificate consists of several components (for example, `O` — organization, `OU` — department within the organization, `C` — country, `CN` — common name of the subject). Checks can be configured to match one or more components of the field against expected values.

The "Subject Alternative Name" field of the node certificate is a list of the node's network names or IP addresses. Checks can be configured to match the names specified in the certificate against the expected values.

## Syntax


```yaml
client_certificate_authorization:
  request_client_certificate: Bool
  client_certificate_required: Bool
  default_group: <default SID>
  client_certificate_definitions:
    - member_groups: <SID array>
      require_same_issuer: Bool
      subject_dns:
        suffixes: <array of allowed suffixes>
        values: <array of allowed values>
      subject_terms:
      - short_name: <Subject Name component name>
        suffixes: <array of allowed suffixes>
        values: <array of allowed values>
    - member_groups: <SID array>
    ...
```


| Key | Description |
| --- | --- |
| `request_client_certificate` | Requesting a client certificate during TLS-handshake for gRPCs.<br/>Allowed values:<br/><ul><li>`true` — the server requests a client certificate. If a certificate is presented, it is verified at the TLS level during [device authentication](../../security/authentication.md#device-auth-interfaces) (a connection with an untrusted certificate is not established); after successful verification, the server preferentially uses the [authentication token](../../concepts/glossary.md#auth-token) for client authentication, and if no token is present, authentication is performed using the [client certificate](../../security/authentication.md#client-certificate). If no certificate is presented, then with the `client_certificate_required=true` setting, the connection is not established.</li><li>`false` — the server does not request a client certificate during TLS-handshake; device authentication and client certificate authentication over gRPCs are not available with this setting.<br/>Default value: `false`.</li></ul> |
| `client_certificate_required` | Requiring a client certificate during TLS-handshake for gRPCs.<br/>Allowed values:<br/><ul><li>`true` — the server requires a client certificate: a connection without a certificate or with an untrusted certificate is not established. You can specify `true` only together with `request_client_certificate: true`.</li><li>`false` — a client certificate is not required; the behavior when requesting a certificate is determined by the `request_client_certificate` parameter.<br/>Default value: `false`.</li></ul> |
| SID assigned to all connections providing a trusted client certificate when no explicit settings are provided in the `default_group` section. | SID assigned to all connections with a trusted client certificate if no explicit settings are specified in the `client_certificate_definitions` section.<br/>Default value: `DefaultClientAuth@cert`. |
| `client_certificate_definitions` | Section defining the requirements for database node certificates. |
| `member_groups` | SIDs assigned to connections that conform to the requirements of the current configuration block. |
| `require_same_issuer` | Require that the value of the "Issuer" field (typically containing the Certification Authority name) is the same for both client (database node) and server (storage node) certificates. <br/>Allowed values:<br/><ul><li>`true` — The values must be the same (used by default if the parameter is omitted).</li><li>`false` — The values can be different (allowing client and server certificates to be issued by different Certification Authorities).</li></ul> |
| `subject_dns` | Allowed values for the "Subject Alternative Name" field, specified as either full values (using the `values` sub-key) or suffixes (using the `suffixes` sub-key). The check is successful if the actual value matches any full name or any suffix specified. |
| `subject_terms` | Requirements for the "Subject" field value. Contains the component name (in the `short_name` sub-key) and a list of full values (using the `values` sub-key) or suffixes (using the `suffixes` sub-key). The check is successful if the actual value of each component matches either an allowed full value or an allowed suffix. |

## Examples

The following configuration fragment requires that the "Subject" field of the client certificate contain the `O=YDB` and `CN=user1` components. For such a certificate, the user SID `O=YDB,CN=user1@cert` will be generated and the group `group@cert` will be assigned:


```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["group@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
      - short_name: "CN"
        values: ["user1"]
```


The `CN` component may specify the server's network name rather than the user name. This option is advisable to use when [registering dynamic nodes](../../devops/deployment-options/manual/node-authorization.md#vklyuchenie-rezhima-autentifikacii-i-avtorizacii-uzlov). The following configuration fragment requires that the "Subject" field of the node's client certificate contain the `O=YDB` and `CN=server1.internal.corp` components. For such a certificate, the SID `O=YDB,CN=server1.internal.corp@cert` will be generated and the group `registerNode@cert` will be assigned:


```yaml
client_certificate_authorization:
  request_client_certificate: true
  client_certificate_definitions:
    - member_groups: ["registerNode@cert"]
      subject_terms:
      - short_name: "O"
        values: ["YDB"]
      - short_name: "CN"
        values: ["server1.internal.corp"]
```
