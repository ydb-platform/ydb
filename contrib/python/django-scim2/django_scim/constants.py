ENCODING = 'utf-8'
SCIM_CONTENT_TYPE = 'application/scim+json'
VALID_PATCH_OPS = ('add', 'remove', 'replace')


class SchemaURI(object):
    ERROR = 'urn:ietf:params:scim:api:messages:2.0:Error'
    LIST_RESPONSE = 'urn:ietf:params:scim:api:messages:2.0:ListResponse'
    SERACH_REQUEST = 'urn:ietf:params:scim:api:messages:2.0:SearchRequest'
    NOT_SERACH_REQUEST = 'urn:ietf:params:scim:api:messages:2.0:NotSearchRequest'
    PATCH_OP = 'urn:ietf:params:scim:api:messages:2.0:PatchOp'

    USER = 'urn:ietf:params:scim:schemas:core:2.0:User'
    ENTERPRISE_URN = 'urn:ietf:params:scim:schemas:extension:enterprise'
    ENTERPRISE_USER = 'urn:ietf:params:scim:schemas:extension:enterprise:2.0:User'
    GROUP = 'urn:ietf:params:scim:schemas:core:2.0:Group'
    RESOURCE_TYPE = 'urn:ietf:params:scim:schemas:core:2.0:ResourceType'
    SERVICE_PROVIDER_CONFIG = 'urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig'
