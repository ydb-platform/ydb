When connecting to an [endpoint](../../../../concepts/connect.md#endpoint) via gRPCs (with encryption), the client verifies the server certificate against a [chain of trust](../../../../concepts/connect.md#tls-cert). If the server uses a root CA that is not in the standard set of trusted CLI certificates, the path to it is specified in the parameter:

- `--ca-file <filename>` : root PEM certificate file (CA)

If the [client_certificate_authorization](../../../configuration/client_certificate_authorization.md) section in the cluster configuration has the `request_client_certificate: true` parameter enabled, the server requests a client certificate during [device authentication](../../../../security/authentication.md#device-auth-interfaces). If [device authentication](../../../../security/authentication.md#device-auth) or [client certificate authentication](../../../../security/authentication.md#client-certificate) is required, the following are specified on the client:

- `--client-cert-file <filename>` : client certificate file (PEM or PKCS#12)
- `--client-cert-key-file <filename>` : client certificate private key file (specified if the certificate and key are in separate files)
- `--client-cert-key-password-file <filename>` : file with the private key password, if the key is encrypted; if the key is encrypted and the option is not specified, the password will be requested interactively

The options listed above are also used for [client certificate authentication](../../../../security/authentication.md#client-certificate): `request_client_certificate: true` must be enabled on the server side, and no other [authentication](#authentication) options must be set on the client. If `--client-cert-file` and another authentication mode are specified simultaneously, the certificate is transmitted during the TLS handshake, but an [authentication token](../../../../concepts/glossary.md#auth-token) is used for request identification, not the certificate.

After successful authentication, the client is assigned a security identifier [SID](../../../../concepts/glossary.md#access-sid), which has all the [rights](../../../../concepts/glossary.md#access-right) assigned to the corresponding identifier. The method of forming the SID depends on the [authentication](#authentication) mode. Schema and data rights are not granted automatically — see [authorization](../../../../security/authorization.md).
