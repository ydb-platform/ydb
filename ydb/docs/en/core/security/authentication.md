If the user is found, {{ ydb-short-name }} performs a repeated bind operation — this time on behalf of the found user, using their password.
4. The final result — successful or unsuccessful authentication — is determined by the result of the second bind (on behalf of the user).

Thus, {{ ydb-short-name }} does not store user passwords and fully relies on the LDAP authentication mechanism.

As a result of successful verification of the user's login and password in the LDAP directory, an [authentication token](../concepts/glossary.md#auth-token) {{ ydb-short-name }} is returned. This token is then used instead of the login and password. Using a token speeds up the authentication process and improves security.

{% note info %}

When using LDAP authentication, no user passwords are stored in {{ ydb-short-name }}.

{% endnote %}

#### Service account authentication {#ldap-service-account-auth}

A service account can be authenticated in two main ways:

* Using a login and password.
  In this case, the configuration must specify the login (`bind_dn`) and password (`bind_password`). These parameters will be used to connect to the LDAP server on behalf of the service account.

* Using mTLS (mutual TLS) via the SASL EXTERNAL mechanism.
  In this option, certificates are used for authentication instead of a login and password. This allows not storing the service account password in the configuration — it is enough to specify the certificate file (`use_tls.cert_file`) and private key file (`use_tls.key_file`), as well as enable a special flag (`extended_settings.enable_sasl_external_bind`). For detailed configuration information, see  the section [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config).

### Token verification {#token-validation}

After user authentication, an [authentication token](../concepts/glossary.md#auth-token) is generated, which is passed with requests to {{ ydb-short-name }}. When verifying the token, the node determines the user on whose behalf the request is made and the groups to which they belong. Depending on the authentication method, cryptographic verification of the token may be performed on the {{ ydb-short-name }} side, or an external authentication system may be contacted. For example, for a user from an LDAP directory, the token does not contain group information, so the node makes an additional request to the LDAP server to obtain the user's group list. Network calls increase the time for verifying the authentication token and processing the request, as well as the load on the external system. Therefore, {{ ydb-short-name }} [cache verification results](./caching-authentication-results.md).

Groups, like the user themselves, are subjects for performing operations on database schema objects. To differentiate access to various database resources, subjects can be assigned access rights. And in accordance with the list of assigned rights, subjects will be authorized to perform certain operations.

The process of obtaining a user's group list from the LDAP directory is similar to the actions performed during authentication. First, a *bind* operation is performed for the service user whose credentials are recorded in the `bind_dn` and `bind_password` parameters of the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section of the configuration file. After successful authentication, a search is performed for the user record for which the token was previously generated. The search is also performed in accordance with the `search_filter` parameter. If the user still exists, the result of the *search* operation will be a list of values of the attribute specified in the `requested_group_attribute` parameter. If this parameter is empty, the attribute for reverse group membership will be `memberOf`. The attribute `memberOf` stores the Distinguished Names (DN) of the groups to which the user belongs.

#### Obtaining groups

By default, {{ ydb-short-name }} searches only for those groups in which the user is directly a member. By enabling the `extended_settings.enable_nested_groups_search` flag in the [ldap_authentication](../reference/configuration/auth_config.md#ldap-auth-config) section, {{ ydb-short-name }} will attempt to obtain groups at all nesting levels, not just those in which the user is directly a member. If {{ ydb-short-name }} is configured to work with Active Directory, the Active Directory-specific matching rule [LDAP_MATCHING_RULE_IN_CHAIN](https://learn.microsoft.com/en-us/windows/win32/adsi/search-filter-syntax?redirectedfrom=MSDN). will be used to search for all nested groups. This rule allows obtaining all nested groups with a single query. For LDAP servers based on OpenLDAP, group search will be performed by recursively traversing the graph, which generally requires multiple queries. For both Active Directory and OpenLDAP, group search will be performed only for the subtree whose root is taken from the configuration parameter `base_dn`.

{% note info %}

 The client passes the already obtained JWT token as a Bearer token with each request.

### How it works

1. The client authenticates with an external IdP and receives a signed JWT token.
2. The client passes the token in a request to {{ ydb-short-name }} with type `Bearer`.
3. The node {{ ydb-short-name }} requests the OIDC Discovery document at `<issuer>/.well-known/openid-configuration`, where `<issuer>` is the configured provider URL and token issuer identifier. For example, for `issuer: https://idp.example.com` the request is sent to `https://idp.example.com/.well-known/openid-configuration`. The value of the field `issuer` in the document must exactly match the configured provider address.
4. From the field `jwks_uri` of the Discovery document, the URL is extracted, from which {{ ydb-short-name }} periodically obtains a set of public keys [JWKS](https://www.rfc-editor.org/rfc/rfc7517).
5. Based on the fields `alg` (algorithm, signing algorithm) and `kid` (key ID, key identifier) of the JWT header, the public key is selected. {{ ydb-short-name }} verifies the token signature, issuer, audience, and expiration times.
6. The user identifier and group list are extracted from the token fields. The configured authentication domain suffix is added to them, after which the resulting SIDs are used for [authorization](./authorization.md).

The Discovery URL, `issuer`, and `jwks_uri` must use the scheme `https://`. If the Discovery document or JWKS is temporarily unavailable, {{ ydb-short-name }} retries requests with an increasing interval. The obtained keys are cached and periodically refreshed to support key rotation on the IdP side. After the cache lifetime expires, outdated keys are removed; until JWKS is successfully refreshed, new token verifications fail with a temporary error.

### Token and key requirements

The JWT token must have a valid compact serialization format and contain the fields `alg` and `kid` in its header. Only asymmetric signing algorithms are supported:

- RSA PKCS#1: `RS256`, `RS384`, `RS512`;
- RSA-PSS: `PS256`, `PS384`, `PS512`;
- ECDSA: `ES256`, `ES384`, `ES512`.

Symmetric algorithms of the `HS*` family are not supported. The public key in JWKS must have matching `kty` (key type) and `kid`, and contain `x5c` (X.509 certificate chain). The public key is extracted from the first certificate `x5c`; JWKs containing only RSA or EC parameters without `x5c` are skipped.

The following fields are required for successful authentication:

- `alg` and `kid` in the JWT header;
- the claim `iss` (issuer, token issuer) matching the value of `issuer` in the configuration;
- a non-empty string user identifier. To obtain it, the claim specified by the parameter `subject_claim_name` is used. If this claim is missing or has a different type, the standard claim `sub` (subject, subject identifier) is used.

The remaining checked fields are optional:

- the claim `aud` (audience, token recipient) is checked if the parameter `audience` is set in the configuration; it is recommended to always set the expected audience so that tokens issued for other services are not accepted;
- when checking time-related claims such as `exp` (expiration time), `nbf` (not before), and `iat` (issued at), the allowed clock skew specified by [parameter `allowed_clock_skew`](../reference/configuration/auth_config.md#external-idp-auth-config) is taken into account. If `exp` is missing, {{ ydb-short-name }} sets the authentication result expiration to 10 minutes;
- the claim specified by the parameter `groups_claim_name` contains the user's group list. It must be an array; only string elements are extracted from the array. If the claim is missing or has a different type, the group list is considered empty.

### SID formation

The suffix `@<auth-domain>` is added to the user identifier and each group from the JWT. The value of `<auth-domain>` is set by the parameter `external_idp_authentication_domain`; by default, the value `sso` is used.

For example, with `sub: user1`, `groups: [admins, developers]`, and the default domain, the following SIDs will be formed:

- user `user1@sso`;
- groups `admins@sso` and `developers@sso`.

{{ ydb-short-name }} uses the group list from the token without additional calls to the IdP and without expanding nested groups. Users and groups of the external IdP cannot be managed using the commands `CREATE USER`, `ALTER USER`, `CREATE GROUP`, and `ALTER GROUP`. Permissions are assigned to the formed SIDs using the methods described in section [{#T}](./authorization.md).

### Server configuration

Authentication via an external IdP is enabled in the [authentication configuration](../reference/configuration/auth_config.md#external-idp-auth-config) when the section `external_idp_config` is present.

## Client certificate authentication {#client-certificate}

{{ ydb-short-name }} can authenticate a client based on the client certificate data obtained during TLS connection establishment. It can also be used for applications outside such environments, acting as an analog of **Refresh Token** for service accounts. Unlike a personal account, the access objects and roles of a service account can be restricted.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

A token for specifying in parameters can be obtained from the IAM system associated with a specific installation {{ ydb-short-name }}. In particular, for the service {{ ydb-short-name }} in {{ yandex-cloud }}, Yandex.Passport OAuth and service accounts {{ yandex-cloud }} are used. When using {{ ydb-short-name }} in corporate contexts, standard centralized authentication systems for the organization may be used.

When using modes that involve the client {{ ydb-short-name }} accessing IAM, an IAM URL providing a token issuance API can additionally be specified. By default, existing SDKs and CLIs attempt to access the IAM API {{ yandex-cloud }}, hosted at `iam.api.cloud.yandex.net:443`.
