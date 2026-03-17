#!/usr/bin/env python
"""Cert methods module."""
import os
import warnings

from hvac.api.vault_api_base import VaultApiBase
from hvac.utils import validate_pem_format
from hvac import exceptions, utils


class Cert(VaultApiBase):
    """Cert Auth Method (API).

    Reference: https://www.vaultproject.io/api/auth/cert/index.html
    """

    def create_ca_certificate_role(
        self,
        name,
        certificate="",
        certificate_file="",
        allowed_common_names="",
        allowed_dns_sans="",
        allowed_email_sans="",
        allowed_uri_sans="",
        allowed_organizational_units="",
        required_extensions="",
        display_name="",
        token_ttl=0,
        token_max_ttl=0,
        token_policies=[],
        token_bound_cidrs=[],
        token_explicit_max_ttl=0,
        token_no_default_policy=False,
        token_num_uses=0,
        token_period=0,
        token_type="",
        mount_point="cert",
    ):
        """Create CA Certificate Role.

        Sets a CA cert and associated parameters in a role name.

        Supported methods:
            POST:	/auth/<mount point>/certs/:name. Produces: 204 (empty body)

        :param name: The name of the certificate role.
        :type name: str
        :param certificate: The PEM-format CA certificate. Either certificate or certificate_file is required.
            NOTE: Passing a certificate file path with the certificate argument is deprecated and will be dropped in
            version 3.0.0
        :type certificate: str
        :param certificate_file: File path to the PEM-format CA certificate.  Either certificate_file or certificate is
            required.
        :type certificate_file: str
        :param allowed_common_names: Constrain the Common Names in the client certificate with a globbed pattern. Value
            is a comma-separated list of patterns. Authentication requires at least one Name matching at least one
            pattern. If not set, defaults to allowing all names.
        :type allowed_common_names: str | list
        :param allowed_dns_sans: Constrain the Alternative Names in the client certificate with a globbed pattern. Value
            is a comma-separated list of patterns. Authentication requires at least one DNS matching at least one pattern.
            If not set, defaults to allowing all dns.
        :type allowed_dns_sans: str | list
        :param allowed_email_sans: Constrain the Alternative Names in the client certificate with a globbed pattern.
            Value is a comma-separated list of patterns. Authentication requires at least one Email matching at least
            one pattern. If not set, defaults to allowing all emails.
        :type allowed_email_sans: str | list
        :param allowed_uri_sans: Constrain the Alternative Names in the client certificate with a globbed pattern.
            Value is a comma-separated list of URI patterns. Authentication requires at least one URI matching at least
            one pattern. If not set, defaults to allowing all URIs.
        :type allowed_uri_sans: str | list
        :param allowed_organizational_units: Constrain the Organizational Units (OU) in the client certificate with a
            globbed pattern. Value is a comma-separated list of OU patterns. Authentication requires at least one OU
            matching at least one pattern. If not set, defaults to allowing all OUs.
        :type allowed_organizational_units: str | list
        :param required_extensions: Require specific Custom Extension OIDs to exist and match the pattern. Value is a
            comma separated string or array of oid:value. Expects the extension value to be some type of ASN1 encoded
            string. All conditions must be met. Supports globbing on value.
        :type required_extensions: str | list
        :param display_name: The display_name to set on tokens issued when authenticating against this CA certificate.
            If not set, defaults to the name of the role.
        :type display_name: str | unicode
        :param token_ttl: The incremental lifetime for generated tokens. This current value of this will be referenced
            at renewal time.
        :type token_ttl: int | str
        :param token_max_ttl: The maximum lifetime for generated tokens. This current value of this will be referenced
            at renewal time.
        :type token_max_ttl: int | str
        :param token_policies: List of policies to encode onto generated tokens. Depending on the auth method, this list
            may be supplemented by user/group/other values.
        :type token_policies: list | str
        :param token_bound_cidrs: List of CIDR blocks; if set, specifies blocks of IP addresses which can authenticate
            successfully, and ties the resulting token to these blocks as well.
        :type token_bound_cidrs: list | str
        :param token_explicit_max_ttl: If set, will encode an explicit max TTL onto the token. This is a hard cap even
            if token_ttl and token_max_ttl would otherwise allow a renewal.
        :type token_explicit_max_ttl: int | str
        :param token_no_default_policy: If set, the default policy will not be set on generated tokens; otherwise it
            will be added to the policies set in token_policies.
        :type token_no_default_policy: bool
        :param token_num_uses: The maximum number of times a generated token may be used (within its lifetime); 0 means
            unlimited. If you require the token to have the ability to create child tokens, you will need to set this value to 0.
        :type token_num_uses: int
        :param token_period: The period, if any, to set on the token.
        :type token_period: int | str
        :param token_type: The type of token that should be generated. Can be service, batch, or default to use the
            mount's tuned default (which unless changed will be service tokens). For token store roles, there are two
            additional possibilities: default-service and default-batch which specify the type to return unless the
            client requests a different type at generation time.
        :type token_type: str
        :param mount_point:
        :type mount_point:
        """
        if certificate:
            try:
                utils.validate_pem_format("", certificate)
                cert = certificate
            except exceptions.ParamValidationError:
                with open(certificate) as f_cert:
                    warnings.warn(
                        "Passing a certificate file path to `certificate` is deprecated and will be removed in v3.0.0;"
                        "use `certificate_file` instead. (See https://github.com/hvac/hvac/issues/914)"
                    )
                    cert = f_cert.read()
        elif certificate_file:
            with open(certificate_file) as f_cert:
                cert = f_cert.read()
        else:
            raise exceptions.ParamValidationError(
                "`certificate` or `certificate_file` must be provided"
            )

        params = utils.remove_nones(
            {
                "name": name,
                "certificate": cert,
                "allowed_common_names": allowed_common_names,
                "allowed_dns_sans": allowed_dns_sans,
                "allowed_email_sans": allowed_email_sans,
                "allowed_uri_sans": allowed_uri_sans,
                "allowed_organizational_units": allowed_organizational_units,
                "required_extensions": required_extensions,
                "display_name": display_name,
                "token_ttl": token_ttl,
                "token_max_ttl": token_max_ttl,
                "token_policies": token_policies,
                "token_bound_cidrs": token_bound_cidrs,
                "token_explicit_max_ttl": token_explicit_max_ttl,
                "token_no_default_policy": token_no_default_policy,
                "token_num_uses": token_num_uses,
                "token_period": token_period,
                "token_type": token_type,
            }
        )

        api_path = "/v1/auth/{mount_point}/certs/{name}".format(
            mount_point=mount_point, name=name
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_ca_certificate_role(self, name, mount_point="cert"):
        """
        Gets information associated with the named role.

        Supported methods:
            GET: /auth/<mount point>/certs/{name}. Produces: 200 application/json

        :param name: The name of the certificate role
        :type name: str | unicode
        :param mount_point:
        :type mount_point:
        :return: The JSON response of the read_ca_certificate_role request.
        :rtype: dict
        """
        params = {
            "name": name,
        }
        api_path = "/v1/auth/{mount_point}/certs/{name}".format(
            mount_point=mount_point, name=name
        )
        return self._adapter.get(
            url=api_path,
            json=params,
        )

    def list_certificate_roles(self, mount_point="cert"):
        """
        Lists configured certificate names.

        Supported methods:
            LIST: /auth/<mount point>/certs. Produces: 200 application/json

        :param mount_point:
        :type mount_point:
        :return: The response of the list_certificate request.
        :rtype: requests.Response
        """
        api_path = f"/v1/auth/{mount_point}/certs"
        return self._adapter.list(url=api_path)

    def delete_certificate_role(self, name, mount_point="cert"):
        """
        List existing LDAP existing groups that have been created in this auth method.

        Supported methods:
            DELETE: /auth/{mount_point}/groups. Produces: 204 (empty body)

        :param name: The name of the certificate role.
        :type name: str | unicode
        :param mount_point:
        :type mount_point:
        """
        api_path = "/v1/auth/{mount_point}/certs/{name}".format(
            mount_point=mount_point, name=name
        )
        return self._adapter.delete(
            url=api_path,
        )

    def configure_tls_certificate(self, mount_point="cert", disable_binding=False):
        """
        Configure options for the method.

        Supported methods:
            POST: /auth/<mount point>/config. Produces: 204 (empty body)


        :param disable_binding: If set, during renewal, skips the matching of presented client identity with the client
            identity used during login.
        :type disable_binding: bool
        :param mount_point:
        :type mount_point:
        """
        params = {
            "disable_binding": disable_binding,
        }
        api_path = f"/v1/auth/{mount_point}/config"
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def login(
        self,
        name="",
        cacert=False,
        cert_pem="",
        key_pem="",
        mount_point="cert",
        use_token=True,
    ):
        """
        Log in and fetch a token. If there is a valid chain to a CA configured in the method and all role constraints
            are matched, a token will be issued. If the certificate has DNS SANs in it, each of those will be verified.
            If Common Name is required to be verified, then it should be a fully qualified DNS domain name and must be
            duplicated as a DNS SAN

        Supported methods:
            POST: /auth/<mount point>/login Produces: 200 application/json

        :param name: Authenticate against only the named certificate role, returning its policy list if successful. If
            not set, defaults to trying all certificate roles and returning any one that matches.
        :type name: str | unicode
        :param cacert: The value used here is for the Vault TLS Listener CA certificate, not the CA that issued the
            client authentication certificate. This can be omitted if the CA used to issue the Vault server certificate
            is trusted by the local system executing this command.
        :type cacert: str | bool
        :param cert_pem: Location of the cert.pem used to authenticate the host.
        :tupe cert_pem: str | unicode
        :param key_pem: Location of the public key.pem used to authenticate the host.
        :param key_pem: str | unicode
        :param mount_point:
        :type mount_point:
        :param use_token: If the returned token is stored in the client
        :param use_token: bool
        :return: The response of the login request.
        :rtype: requests.Response
        """
        params = {}
        if name != "":
            params["name"] = name
        api_path = f"/v1/auth/{mount_point}/login"

        # Must have cert checking or a CA cert. This is caught lower down but harder to grok
        if not cacert:
            # If a cacert is not provided try to drop down to the adapter and get the cert there.
            # If the cacert is not in the adapter already login will also.
            if not self._adapter._kwargs.get("verify"):
                raise self.CertificateAuthError(
                    "cacert must be True, a file_path, or valid CA Certificate."
                )
            else:
                cacert = self._adapter._kwargs.get("verify")
        else:
            validate_pem_format("verify", cacert)
        # if cert_pem is a string its ready to be used and either has the key with it or the key is provided as an arg
        try:
            if validate_pem_format("cert_pem", cert_pem):
                tls_update = True
        except exceptions.ParamValidationError:
            tls_update = {}
            if not (os.path.exists(cert_pem) or self._adapter._kwargs.get("cert")):
                raise FileNotFoundError("Can't find the certificate.")
            try:
                tls_parts = {"cert_pem": cert_pem, "key_pem": key_pem}
                for tls_part in tls_parts:
                    if tls_parts[tls_part] != "":
                        tls_update[tls_part] = tls_parts[tls_part]
            except ValueError:
                tls_update = True

        additional_request_kwargs = {}
        if tls_update:
            additional_request_kwargs = {
                "verify": cacert,
                # need to define dict as cert is a tuple
                "cert": tuple([cert_pem, key_pem]),
            }

        return self._adapter.login(
            url=api_path, use_token=use_token, json=params, **additional_request_kwargs
        )

    class CertificateAuthError(Exception):
        pass
