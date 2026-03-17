#!/usr/bin/python
""" AWS auth method module """
import logging
import json
from base64 import b64encode

from hvac import exceptions, aws_utils, utils
from hvac.api.vault_api_base import VaultApiBase
from hvac.constants.aws import ALLOWED_IAM_ALIAS_TYPES, ALLOWED_EC2_ALIAS_TYPES
from hvac.constants.aws import DEFAULT_MOUNT_POINT as AWS_DEFAULT_MOUNT_POINT

logger = logging.getLogger(__name__)


class Aws(VaultApiBase):
    """AWS Auth Method (API).

    Reference: https://www.vaultproject.io/api/auth/aws/index.html
    """

    def configure(
        self,
        max_retries=None,
        access_key=None,
        secret_key=None,
        endpoint=None,
        iam_endpoint=None,
        sts_endpoint=None,
        iam_server_id_header_value=None,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
        sts_region=None,
    ):
        """Configure the credentials required to perform API calls to AWS as well as custom endpoints to talk to AWS API.

        The instance identity document fetched from the PKCS#7 signature will provide the EC2 instance ID.
        The credentials configured using this endpoint will be used to query the status of the instances via
        DescribeInstances API. If static credentials are not provided using this endpoint, then the credentials will be
        retrieved from the environment variables AWS_ACCESS_KEY, AWS_SECRET_KEY and AWS_REGION respectively.
        If the credentials are still not found and if the method is configured on an EC2 instance with metadata querying
        capabilities, the credentials are fetched automatically

        Supported methods:
            POST: /auth/{mount_point}/config Produces: 204 (empty body)

        :param max_retries: Number of max retries the client should use for recoverable errors.
            The default (-1) falls back to the AWS SDK's default behavior
        :type max_retries: int
        :param access_key: AWS Access key with permissions to query AWS APIs. The permissions required depend on the
            specific configurations. If using the iam auth method without inferencing, then no credentials are
            necessary. If using the ec2 auth method or using the iam auth method with inferencing, then these
            credentials need access to ec2:DescribeInstances. If additionally a bound_iam_role is specified, then
            these credentials also need access to iam:GetInstanceProfile. If, however, an alternate sts configuration
            is set for the target account, then the credentials must be permissioned to call sts:AssumeRole on the
            configured role, and that role must have the permissions described here
        :type access_key: str | unicode
        :param secret_key: AWS Secret key with permissions to query AWS APIs
        :type secret_key: str | unicode
        :param endpoint: URL to override the default generated endpoint for making AWS EC2 API calls
        :type endpoint: str | unicode
        :param iam_endpoint: URL to override the default generated endpoint for making AWS IAM API calls
        :type iam_endpoint: str | unicode
        :param sts_endpoint: URL to override the default generated endpoint for making AWS STS API calls
        :type sts_endpoint: str | unicode
        :param iam_server_id_header_value: The value to require in the X-Vault-AWS-IAM-Server-ID header as part of
            GetCallerIdentity requests that are used in the iam auth method. If not set, then no value is required or
            validated. If set, clients must include an X-Vault-AWS-IAM-Server-ID header in the headers of login
            requests, and further this header must be among the signed headers validated by AWS. This is to protect
            against different types of replay attacks, for example a signed request sent to a dev server being resent
            to a production server
        :type iam_server_id_header_value: str | unicode
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str | unicode
        :param sts_region: Region to override the default region for making AWS STS API calls. Should only be set if
            sts_endpoint is set. If so, should be set to the region in which the custom sts_endpoint resides
        :type sts_region: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        params = utils.remove_nones(
            {
                "max_retries": max_retries,
                "access_key": access_key,
                "secret_key": secret_key,
                "endpoint": endpoint,
                "iam_endpoint": iam_endpoint,
                "sts_endpoint": sts_endpoint,
                "iam_server_id_header_value": iam_server_id_header_value,
                "sts_region": sts_region,
            }
        )
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/client", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_config(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Read previously configured AWS access credentials.

        Supported methods:
            GET: /auth/{mount_point}/config. Produces: 200 application/json

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str | unicode
        :return: The data key from the JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/client", mount_point=mount_point
        )
        response = self._adapter.get(
            url=api_path,
        )
        return response.get("data")

    def delete_config(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Delete previously configured AWS access credentials,

        Supported methods:
            DELETE: /auth/{mount_point}/config Produces: 204 (empty body)

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/client", mount_point=mount_point
        )
        return self._adapter.delete(url=api_path)

    def configure_identity_integration(
        self,
        iam_alias=None,
        ec2_alias=None,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
        iam_metadata=None,
        ec2_metadata=None,
    ):
        """Configure the way that Vault interacts with the Identity store.

        The default (as of Vault 1.0.3) is role_id for both values.

        Supported methods:
            POST: /auth/{mount_point}/config/identity Produces: 204 (empty body)

        :param iam_alias: How to generate the identity alias when using the iam auth method. Valid choices are role_id,
            unique_id, and full_arn When role_id is selected, the randomly generated ID of the role is used. When
            unique_id is selected, the IAM Unique ID of the IAM principal (either the user or role) is used as the
            identity alias name. When full_arn is selected, the ARN returned by the sts:GetCallerIdentity call is used
            as the alias name. This is either arn:aws:iam::<account_id>:user/<optional_path/><user_name> or
            arn:aws:sts::<account_id>:assumed-role/<role_name_without_path>/<role_session_name>. Note: if you
            select full_arn and then delete and recreate the IAM role, Vault won't be aware and any identity aliases
            set up for the role name will still be valid
        :type iam_alias: str | unicode
        :param iam_metadata: The metadata to include on the token returned by the login endpoint.
            This metadata will be added to both audit logs, and on the ``iam_alias``. By default, it includes ``account_id``
            and ``auth_type``. Additionally, ``canonical_arn``, ``client_arn``, ``client_user_id``, ``inferred_aws_region``, ``inferred_entity_id``,
            and ``inferred_entity_type`` are available. To include no metadata, set to an empty list ``[]``.
            To use only particular fields, select the explicit fields. To restore to defaults, send only a field of ``default``.
            Only select fields that will have a low rate of change for your ``iam_alias`` because each change triggers a storage
            write and can have a performance impact at scale.
        :type iam_metadata: str | unicode | list
        :param ec2_alias: Configures how to generate the identity alias when using the ec2 auth method. Valid choices
            are role_id, instance_id, and image_id. When role_id is selected, the randomly generated ID of the role is
            used. When instance_id is selected, the instance identifier is used as the identity alias name. When
            image_id is selected, AMI ID of the instance is used as the identity alias name
        :type ec2_alias: str | unicode
        :param ec2_metadata: The metadata to include on the token returned by the login endpoint. This metadata will be
            added to both audit logs, and on the ``ec2_alias``. By default, it includes ``account_id`` and ``auth_type``. Additionally,
            ``ami_id``, ``instance_id``, and ``region`` are available. To include no metadata, set to an empty list ``[]``.
            To use only particular fields, select the explicit fields. To restore to defaults, send only a field of ``default``.
            Only select fields that will have a low rate of change for your ``ec2_alias`` because each change triggers a storage
            write and can have a performance impact at scale.
        :type ec2_metadata: str | unicode | list
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request
        :rtype: request.Response
        """
        if iam_alias is not None and iam_alias not in ALLOWED_IAM_ALIAS_TYPES:
            error_msg = f"invalid iam alias type provided: '{iam_alias}' - supported iam alias types: '{','.join(ALLOWED_IAM_ALIAS_TYPES)}'"
            raise exceptions.ParamValidationError(error_msg)
        if ec2_alias is not None and ec2_alias not in ALLOWED_EC2_ALIAS_TYPES:
            error_msg = f"invalid ec2 alias type provided: '{ec2_alias}' - supported ec2 alias types: '{','.join(ALLOWED_EC2_ALIAS_TYPES)}'"
            raise exceptions.ParamValidationError(error_msg)

        params = utils.remove_nones(
            {
                "iam_alias": iam_alias,
                "ec2_alias": ec2_alias,
                "ec2_metadata": ec2_metadata,
                "iam_metadata": iam_metadata,
            }
        )
        api_auth = "/v1/auth/{mount_point}/config/identity".format(
            mount_point=mount_point
        )
        return self._adapter.post(
            url=api_auth,
            json=params,
        )

    def read_identity_integration(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Return previously configured identity integration configuration.

        Supported methods:
            GET: /auth/{mount_point}/config/identity. Produces: 200 application/json

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str | unicode
        :return: The data key from the JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/identity", mount_point=mount_point
        )
        response = self._adapter.get(
            url=api_path,
        )
        return response.get("data")

    def create_certificate_configuration(
        self,
        cert_name,
        aws_public_cert,
        document_type=None,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
    ):
        """Register AWS public key to be used to verify the instance identity documents.

        While the PKCS#7 signature of the identity documents have DSA digest, the identity signature will have RSA
        digest, and hence the public keys for each type varies respectively. Indicate the type of the public key using
        the "type" parameter

        Supported methods:
            POST: /auth/{mount_point}/config/certificate/:cert_name Produces: 204 (empty body)

        :param cert_name: Name of the certificate
        :type cert_name: string | unicode
        :param aws_public_cert: Base64 encoded AWS Public key required to verify PKCS7 signature of the EC2 instance
            metadata
        :param document_type: Takes the value of either "pkcs7" or "identity", indicating the type of document which can be
            verified using the given certificate
        :type document_type: string | unicode
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request
        :rtype: request.Response
        """
        params = {
            "cert_name": cert_name,
            "aws_public_cert": aws_public_cert,
        }
        params.update(
            utils.remove_nones(
                {
                    "document_type": document_type,
                }
            )
        )
        api_path = utils.format_url(
            "/v1/auth/{0}/config/certificate/{1}", mount_point, cert_name
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_certificate_configuration(
        self, cert_name, mount_point=AWS_DEFAULT_MOUNT_POINT
    ):
        """Return previously configured AWS public key.

        Supported methods:
            GET: /v1/auth/{mount_point}/config/certificate/:cert_name Produces: 200 application/json

        :param cert_name: Name of the certificate
        :type cert_name: str | unicode
        :param mount_point: The path the AWS auth method was mounted on.
        :return: The data key from the JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/config/certificate/{1}", mount_point, cert_name
        )
        response = self._adapter.get(
            url=api_path,
        )
        return response.get("data")

    def delete_certificate_configuration(
        self, cert_name, mount_point=AWS_DEFAULT_MOUNT_POINT
    ):
        """Remove previously configured AWS public key.

        Supported methods:
            DELETE: /auth/{mount_point}/config/certificate/:cert_name Produces: 204 (empty body)

        :param cert_name: Name of the certificate
        :type cert_name: str | unicode
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request
        :rtype: request.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/config/certificate/{1}", mount_point, cert_name
        )
        return self._adapter.delete(
            url=api_path,
        )

    def list_certificate_configurations(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """List AWS public certificates that are registered with the method.

        Supported methods
            LIST: /auth/{mount_point}/config/certificates Produces: 200 application/json

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/certificates", mount_point=mount_point
        )
        response = self._adapter.list(
            url=api_path,
        )
        return response.get("data")

    def create_sts_role(
        self, account_id, sts_role, mount_point=AWS_DEFAULT_MOUNT_POINT
    ):
        """Allow the explicit association of STS roles to satellite AWS accounts (i.e. those which are not the
            account in which the Vault server is running.)

            Vault will use credentials obtained by assuming these STS roles when validating IAM principals or EC2
            instances in the particular AWS account

            Supported methods:
                POST: /v1/auth/{mount_point}/config/sts/:account_id Produces: 204 (empty body)

        :param account_id: AWS account ID to be associated with STS role.
            If set, Vault will use assumed credentials to verify any login attempts from EC2 instances in this account.
        :type account_id: str
        :param sts_role: AWS ARN for STS role to be assumed when interacting with the account specified.
            The Vault server must have permissions to assume this role.
        :type sts_role: str
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/config/sts/{1}", mount_point, account_id
        )
        params = {
            "account_id": account_id,
            "sts_role": sts_role,
        }
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_sts_role(self, account_id, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Return previously configured STS role.

        :param account_id: AWS account ID that has been previously associated with STS role.
        :type account_id: str
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/config/sts/{1}", mount_point, account_id
        )
        response = self._adapter.get(
            url=api_path,
        )
        return response.get("data")

    def list_sts_roles(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """List AWS Account IDs for which an STS role is registered.

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/sts", mount_point=mount_point
        )
        response = self._adapter.list(url=api_path)
        return response.get("data")

    def delete_sts_role(self, account_id, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Delete a previously configured AWS account/STS role association.

        :param account_id:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/config/sts/{1}", mount_point, account_id
        )
        return self._adapter.delete(
            url=api_path,
        )

    def configure_identity_whitelist_tidy(
        self,
        safety_buffer=None,
        disable_periodic_tidy=None,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
    ):
        """Configure the periodic tidying operation of the whitelisted identity entries.

        :param safety_buffer: The amount of extra time that must have passed beyond the roletag expiration, before
            it is removed from the method storage.
        :type safety_buffer: str
        :param disable_periodic_tidy: If set to 'true', disables the periodic tidying of the identity-whitelist/<instance_id> entries.
        :type disable_periodic_tidy: bool
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/tidy/identity-whitelist",
            mount_point=mount_point,
        )
        params = utils.remove_nones(
            {
                "safety_buffer": safety_buffer,
                "disable_periodic_tidy": disable_periodic_tidy,
            }
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_identity_whitelist_tidy(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Read previously configured periodic whitelist tidying settings.

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/tidy/identity-whitelist",
            mount_point=mount_point,
        )
        response = self._adapter.get(url=api_path)
        return response.get("data")

    def delete_identity_whitelist_tidy(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Delete previously configured periodic whitelist tidying settings.

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/tidy/identity-whitelist",
            mount_point=mount_point,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def configure_role_tag_blacklist_tidy(
        self,
        safety_buffer=None,
        disable_periodic_tidy=None,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
    ):
        """Configure the periodic tidying operation of the blacklisted role tag entries.

        :param safety_buffer: The amount of extra time that must have passed beyond the roletag expiration, before
            it is removed from the method storage.
        :type safety_buffer: str
        :param disable_periodic_tidy: If set to 'true', disables the periodic tidying of the roletag-blacklist/<instance_id> entries.
        :type disable_periodic_tidy: bool
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/tidy/roletag-blacklist",
            mount_point=mount_point,
        )
        params = utils.remove_nones(
            {
                "safety_buffer": safety_buffer,
                "disable_periodic_tidy": disable_periodic_tidy,
            }
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_role_tag_blacklist_tidy(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Read previously configured periodic blacklist tidying settings.

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/tidy/roletag-blacklist",
            mount_point=mount_point,
        )
        response = self._adapter.get(url=api_path)
        return response.get("data")

    def delete_role_tag_blacklist_tidy(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Delete previously configured periodic blacklist tidying settings.

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config/tidy/roletag-blacklist",
            mount_point=mount_point,
        )
        return self._adapter.delete(url=api_path)

    def create_role(
        self,
        role,
        auth_type=None,
        bound_ami_id=None,
        bound_account_id=None,
        bound_region=None,
        bound_vpc_id=None,
        bound_subnet_id=None,
        bound_iam_role_arn=None,
        bound_iam_instance_profile_arn=None,
        bound_ec2_instance_id=None,
        role_tag=None,
        bound_iam_principal_arn=None,
        inferred_entity_type=None,
        inferred_aws_region=None,
        resolve_aws_unique_ids=None,
        ttl=None,
        max_ttl=None,
        period=None,
        policies=None,
        allow_instance_migration=None,
        disallow_reauthentication=None,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
    ):
        """Register a role in the method.

        :param role:
        :param auth_type:
        :param bound_ami_id:
        :param bound_account_id:
        :param bound_region:
        :param bound_vpc_id:
        :param bound_subnet_id:
        :param bound_iam_role_arn:
        :param bound_iam_instance_profile_arn:
        :param bound_ec2_instance_id:
        :param role_tag:
        :param bound_iam_principal_arn:
        :param inferred_entity_type:
        :param inferred_aws_region:
        :param resolve_aws_unique_ids:
        :param ttl:
        :param max_ttl:
        :param period:
        :param policies:
        :param allow_instance_migration:
        :param disallow_reauthentication:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/auth/{0}/role/{1}", mount_point, role)
        params = {
            "role": role,
        }
        params.update(
            utils.remove_nones(
                {
                    "auth_type": auth_type,
                    "resolve_aws_unique_ids": resolve_aws_unique_ids,
                    "bound_ami_id": bound_ami_id,
                    "bound_account_id": bound_account_id,
                    "bound_region": bound_region,
                    "bound_vpc_id": bound_vpc_id,
                    "bound_subnet_id": bound_subnet_id,
                    "bound_iam_role_arn": bound_iam_role_arn,
                    "bound_iam_instance_profile_arn": bound_iam_instance_profile_arn,
                    "bound_ec2_instance_id": bound_ec2_instance_id,
                    "role_tag": role_tag,
                    "bound_iam_principal_arn": bound_iam_principal_arn,
                    "inferred_entity_type": inferred_entity_type,
                    "inferred_aws_region": inferred_aws_region,
                    "ttl": ttl,
                    "max_ttl": max_ttl,
                    "period": period,
                    "policies": policies,
                    "allow_instance_migration": allow_instance_migration,
                    "disallow_reauthentication": disallow_reauthentication,
                }
            )
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_role(self, role, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Returns the previously registered role configuration

        :param role:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/auth/{0}/role/{1}", mount_point, role)
        response = self._adapter.get(url=api_path)
        return response.get("data")

    def list_roles(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Lists all the roles that are registered with the method

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/roles", mount_point=mount_point
        )
        response = self._adapter.list(
            url=api_path,
        )
        return response.get("data")

    def delete_role(self, role, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Deletes the previously registered role

        :param role:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/auth/{0}/role/{1}", mount_point, role)
        return self._adapter.delete(
            url=api_path,
        )

    def create_role_tags(
        self,
        role,
        policies=None,
        max_ttl=None,
        instance_id=None,
        allow_instance_migration=None,
        disallow_reauthentication=None,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
    ):
        """Create a role tag on the role, which helps in restricting the capabilities that are set on the role.

        Role tags are not tied to any specific ec2 instance unless specified explicitly using the
        instance_id parameter. By default, role tags are designed to be used across all instances that
        satisfies the constraints on the role. Regardless of which instances have role tags on them, capabilities
        defined in a role tag must be a strict subset of the given role's capabilities. Note that, since adding
        and removing a tag is often a widely distributed privilege, care needs to be taken to ensure that the
        instances are attached with correct tags to not let them gain more privileges than what were intended.
        If a role tag is changed, the capabilities inherited by the instance will be those defined on the new role
        tag. Since those must be a subset of the role capabilities, the role should never provide more capabilities
        than any given instance can be allowed to gain in a worst-case scenario

        :param role: Name of the role.
        :type role: str
        :param policies: Policies to be associated with the tag. If set, must be a subset of the role's policies. If
            set, but set to an empty value, only the 'default' policy will be given to issued tokens.
        :type policies: list
        :param max_ttl: The maximum allowed lifetime of tokens issued using this role.
        :type max_ttl: str
        :param instance_id: Instance ID for which this tag is intended for. If set, the created tag can only be used by
            the instance with the given ID.
        :type instance_id: str
        :param disallow_reauthentication: If set, only allows a single token to be granted per instance ID. This can be
            cleared with the auth/aws/identity-whitelist endpoint. Defaults to 'false'. Mutually exclusive with
            allow_instance_migration.
        :type disallow_reauthentication: bool
        :param allow_instance_migration: If set, allows migration of the underlying instance where the client resides.
            This keys off of pendingTime in the metadata document, so essentially, this disables the client nonce check
            whenever the instance is migrated to a new host and pendingTime is newer than the previously-remembered
            time. Use with caution. Defaults to 'false'. Mutually exclusive with disallow_reauthentication.
        :type allow_instance_migration: bool
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The create role tag response.
        :rtype: dict
        """
        api_path = utils.format_url("/v1/auth/{0}/role/{1}/tag", mount_point, role)

        params = utils.remove_nones(
            {
                "disallow_reauthentication": disallow_reauthentication,
                "policies": policies,
                "max_ttl": max_ttl,
                "instance_id": instance_id,
                "allow_instance_migration": allow_instance_migration,
            }
        )

        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def iam_login(
        self,
        access_key,
        secret_key,
        session_token=None,
        header_value=None,
        role=None,
        use_token=True,
        region="us-east-1",
        mount_point=AWS_DEFAULT_MOUNT_POINT,
    ):
        """Fetch a token

            This endpoint verifies the pkcs7 signature of the instance identity document or the signature of the
            signed GetCallerIdentity request. With the ec2 auth method, or when inferring an EC2 instance,
            verifies that the instance is actually in a running state. Cross checks the constraints defined on the
            role with which the login is being performed. With the ec2 auth method, as an alternative to pkcs7
            signature, the identity document along with its RSA digest can be supplied to this endpoint

        :param role: Name of the role against which the login is being attempted.
        :type role: str
        :param use_token: if True, uses the token in the response received from the auth request to set the "token"
            attribute on the the :py:meth:`hvac.adapters.Adapter` instance under the _adapter Client attribute.
        :type use_token: bool
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/login", mount_point=mount_point
        )

        request = aws_utils.generate_sigv4_auth_request(header_value=header_value)
        auth = aws_utils.SigV4Auth(access_key, secret_key, session_token, region)
        auth.add_auth(request)

        # https://github.com/hashicorp/vault/blob/master/builtin/credential/aws/cli.go
        headers = json.dumps({k: [request.headers[k]] for k in request.headers})
        params = {
            "iam_http_request_method": request.method,
            "iam_request_url": b64encode(request.url.encode("utf-8")).decode("utf-8"),
            "iam_request_headers": b64encode(headers.encode("utf-8")).decode("utf-8"),
            "iam_request_body": b64encode(request.body.encode("utf-8")).decode("utf-8"),
            "role": role,
        }

        return self._adapter.login(
            url=api_path,
            use_token=use_token,
            json=params,
        )

    def ec2_login(
        self,
        pkcs7,
        nonce=None,
        role=None,
        use_token=True,
        mount_point=AWS_DEFAULT_MOUNT_POINT,
    ):
        """Retrieve a Vault token using an AWS authentication method mount's EC2 role.

        :param pkcs7: PKCS7 signature of the identity document with all newline characters removed.
        :type pkcs7: str
        :param nonce: The nonce to be used for subsequent login requests.
        :type nonce: str
        :param role: Name of the role against which the login is being attempted.
        :type role: str
        :param use_token: if True, uses the token in the response received from the auth request to set the "token"
            attribute on the the :py:meth:`hvac.adapters.Adapter` instance under the _adapter Client attribute.
        :type use_token: bool
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/login", mount_point=mount_point
        )
        params = {"pkcs7": pkcs7}
        if nonce:
            params["nonce"] = nonce
        if role:
            params["role"] = role

        return self._adapter.login(
            url=api_path,
            use_token=use_token,
            json=params,
        )

    def place_role_tags_in_blacklist(
        self, role_tag, mount_point=AWS_DEFAULT_MOUNT_POINT
    ):
        """Places a valid role tag in a blacklist

            This ensures that the role tag cannot be used by any instance to perform a login operation again. Note
            that if the role tag was previously used to perform a successful login, placing the tag in the blacklist
            does not invalidate the already issued token

        :param role_tag:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/roletag-blacklist/{1}", mount_point, role_tag
        )
        return self._adapter.post(url=api_path)

    def read_role_tag_blacklist(self, role_tag, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Returns the blacklist entry of a previously blacklisted role tag

        :param role_tag:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/roletag-blacklist/{1}", mount_point, role_tag
        )
        response = self._adapter.get(url=api_path)
        return response.get("data")

    def list_blacklist_tags(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Lists all the role tags that are blacklisted

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/roletag-blacklist", mount_point=mount_point
        )
        response = self._adapter.list(
            url=api_path,
        )
        return response.get("data")

    def delete_blacklist_tags(self, role_tag, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Deletes a blacklisted role tag

        :param role_tag:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/roletag-blacklist/{1}", mount_point, role_tag
        )
        return self._adapter.delete(
            url=api_path,
        )

    @utils.aliased_parameter(
        "saftey_buffer", "safety_buffer", removed_in_version="3.0.0", position=1
    )
    def tidy_blacklist_tags(
        self, safety_buffer="72h", mount_point=AWS_DEFAULT_MOUNT_POINT
    ):
        """Cleans up the entries in the blacklist based on expiration time on the entry and safety_buffer

        :param safety_buffer:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/tidy/roletag-blacklist", mount_point=mount_point
        )
        params = {
            "safety_buffer": safety_buffer,
        }
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_identity_whitelist(self, instance_id, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Returns an entry in the whitelist. An entry will be created/updated by every successful login

        :param instance_id:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/identity-whitelist/{1}", mount_point, instance_id
        )
        response = self._adapter.get(url=api_path)
        return response.get("data")

    def list_identity_whitelist(self, mount_point=AWS_DEFAULT_MOUNT_POINT):
        """Lists all the instance IDs that are in the whitelist of successful logins

        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/identity-whitelist", mount_point=mount_point
        )
        response = self._adapter.list(
            url=api_path,
        )
        return response.get("data")

    def delete_identity_whitelist_entries(
        self, instance_id, mount_point=AWS_DEFAULT_MOUNT_POINT
    ):
        """Deletes a cache of the successful login from an instance

        :param instance_id:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{0}/identity-whitelist/{1}", mount_point, instance_id
        )
        return self._adapter.delete(
            url=api_path,
        )

    @utils.aliased_parameter(
        "saftey_buffer", "safety_buffer", removed_in_version="3.0.0", position=1
    )
    def tidy_identity_whitelist_entries(
        self, safety_buffer="72h", mount_point=AWS_DEFAULT_MOUNT_POINT
    ):
        """Cleans up the entries in the whitelist based on expiration time and safety_buffer

        :param safety_buffer:
        :param mount_point: The path the AWS auth method was mounted on.
        :type mount_point: str
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/tidy/identity-whitelist", mount_point=mount_point
        )
        params = {
            "safety_buffer": safety_buffer,
        }
        return self._adapter.post(url=api_path, json=params)
