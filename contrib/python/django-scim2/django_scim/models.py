from urllib.parse import urljoin

from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from . import constants, exceptions
from .settings import scim_settings
from .utils import get_base_scim_location_getter


class SCIMServiceProviderConfig(object):
    """
    A reference ServiceProviderConfig. This should be overridden to
    describe those authentication_schemes and features that are implemented by
    your app.
    """

    def __init__(self, request=None):
        self.request = request

    @property
    def meta(self):
        return {
            'location': self.location,
            'resourceType': 'ServiceProviderConfig',
        }

    @property
    def location(self):
        path = reverse('scim:service-provider-config')
        return urljoin(get_base_scim_location_getter()(self.request), path)

    def to_dict(self):
        return {
            'schemas': [constants.SchemaURI.SERVICE_PROVIDER_CONFIG],
            'documentationUri': scim_settings.DOCUMENTATION_URI,
            'patch': {
                'supported': True,
            },
            'bulk': {
                'supported': False,
                'maxOperations': 1000,
                'maxPayloadSize': 1048576,
            },
            # Django-SCIM2 does not fully support the SCIM2.0 filtering spec.
            # Until it does, let's under promise and over deliver to the world.
            'filter': {
                'supported': False,
                'maxResults': 50,
            },
            'changePassword': {
                'supported': True,
            },
            'sort': {
                'supported': False,
            },
            'etag': {
                'supported': False,
            },
            'authenticationSchemes': scim_settings.AUTHENTICATION_SCHEMES,
            'meta': self.meta,
        }


class AbstractSCIMCommonAttributesMixin(models.Model):
    """
    An abstract model to provide SCIM Common Attributes.

    https://tools.ietf.org/html/rfc7643#section-3.1

    Each SCIM resource (Users, Groups, etc.) includes the following
    common attributes.  With the exception of the "ServiceProviderConfig"
    and "ResourceType" server discovery endpoints and their associated
    resources, these attributes MUST be defined for all resources,
    including any extended resource types.  When accepted by a service
    provider (e.g., after a SCIM create), the attributes "id" and "meta"
    (and its associated sub-attributes) MUST be assigned values by the
    service provider.  Common attributes are considered to be part of
    every base resource schema and do not use their own "schemas" URI.

    For backward compatibility, some existing schema definitions MAY list
    common attributes as part of the schema.  The attribute
    characteristics (see Section 2.2) listed here SHALL take precedence
    over older definitions that may be included in existing schemas.
    """

    """
    id
      A unique identifier for a SCIM resource as defined by the service
      provider.  Each representation of the resource MUST include a
      non-empty "id" value.  This identifier MUST be unique across the
      SCIM service provider's entire set of resources.  It MUST be a
      stable, non-reassignable identifier that does not change when the
      same resource is returned in subsequent requests.  The value of
      the "id" attribute is always issued by the service provider and
      MUST NOT be specified by the client.  The string "bulkId" is a
      reserved keyword and MUST NOT be used within any unique identifier
      value.  The attribute characteristics are "caseExact" as "true", a
      mutability of "readOnly", and a "returned" characteristic of
      "always".  See Section 9 for additional considerations regarding
      privacy.
    """
    scim_id = models.CharField(
        _('SCIM ID'),
        max_length=254,
        null=True,
        blank=True,
        default=None,
        unique=True,
        help_text=_('A unique identifier for a SCIM resource as defined by the service provider.'),
    )

    """
    externalId
      A String that is an identifier for the resource as defined by the
      provisioning client.  The "externalId" may simplify identification
      of a resource between the provisioning client and the service
      provider by allowing the client to use a filter to locate the
      resource with an identifier from the provisioning domain,
      obviating the need to store a local mapping between the
      provisioning domain's identifier of the resource and the
      identifier used by the service provider.  Each resource MAY
      include a non-empty "externalId" value.  The value of the
      "externalId" attribute is always issued by the provisioning client
      and MUST NOT be specified by the service provider.  The service
      provider MUST always interpret the externalId as scoped to the
      provisioning domain.  While the server does not enforce
      uniqueness, it is assumed that the value's uniqueness is
      controlled by the client setting the value.  See Section 9 for
      additional considerations regarding privacy.  This attribute has
      "caseExact" as "true" and a mutability of "readWrite".  This
      attribute is OPTIONAL.
    """
    scim_external_id = models.CharField(
        _('SCIM External ID'),
        max_length=254,
        null=True,
        blank=True,
        default=None,
        db_index=True,
        help_text=_('A string that is an identifier for the resource as defined by the provisioning client.'),
    )

    def set_scim_id(self, is_new):
        if is_new:
            self.__class__.objects.filter(id=self.id).update(scim_id=self.id)
            self.scim_id = str(self.id)

    def save(self, *args, **kwargs):
        is_new = self.id is None
        super(AbstractSCIMCommonAttributesMixin, self).save(*args, **kwargs)
        self.set_scim_id(is_new)

    class Meta:
        abstract = True


class AbstractSCIMUserMixin(AbstractSCIMCommonAttributesMixin):
    """
    An abstract model to provide the User resource schema.

    # https://tools.ietf.org/html/rfc7643#section-4.1
    """

    """
    userName
      A service provider's unique identifier for the user, typically
      used by the user to directly authenticate to the service provider.
      Often displayed to the user as their unique identifier within the
      system (as opposed to "id" or "externalId", which are generally
      opaque and not user-friendly identifiers).  Each User MUST include
      a non-empty userName value.  This identifier MUST be unique across
      the service provider's entire set of Users.  This attribute is
      REQUIRED and is case insensitive.
    """
    scim_username = models.CharField(
        _('SCIM Username'),
        max_length=254,
        null=True,
        blank=True,
        default=None,
        db_index=True,
        help_text=_("A service provider's unique identifier for the user"),
    )

    @property
    def scim_groups(self):
        raise exceptions.NotImplementedError

    class Meta:
        abstract = True


class AbstractSCIMGroupMixin(AbstractSCIMCommonAttributesMixin):
    """
    An abstract model to provide the Group resource schema.

    # https://tools.ietf.org/html/rfc7643#section-4.2
    """

    """
    displayName
      A human-readable name for the Group.  REQUIRED.
    """
    scim_display_name = models.CharField(
        _('SCIM Display Name'),
        max_length=254,
        null=True,
        blank=True,
        default=None,
        db_index=True,
        help_text=_("A human-readable name for the Group."),
    )

    class Meta:
        abstract = True

    def set_scim_display_name(self, is_new):
        if is_new:
            self.__class__.objects.filter(id=self.id).update(scim_display_name=self.name)
            self.scim_display_name = self.name

    def save(self, *args, **kwargs):
        is_new = self.id is None
        super(AbstractSCIMGroupMixin, self).save(*args, **kwargs)
        self.set_scim_display_name(is_new)
