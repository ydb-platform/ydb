from office365.runtime.client_value import ClientValue
from office365.sharepoint.principal.source import PrincipalSource
from office365.sharepoint.principal.type import PrincipalType


class ClientPeoplePickerQueryParameters(ClientValue):
    def __init__(
        self,
        query_string,
        allow_emai_addresses=True,
        allow_multiple_entities=True,
        allow_only_email_addresses=False,
        all_url_zones=False,
        enabled_claim_providers=None,
        force_claims=False,
        maximum_entity_suggestions=1,
        principal_source=PrincipalSource.All,
        principal_type=PrincipalType.All,
        url_zone=0,
        url_zone_specified=False,
        sharepoint_group_id=0,
    ):
        """
        Specifies the properties of a principal query

        :type int urlZone: Specifies a location in the topology of the farm for the principal query.
        :param int sharepoint_group_id: specifies a group containing allowed principals to be used in the principal query
        :param str query_string: Specifies the value to be used in the principal query.
        :param int principal_type: Specifies the type to be used in the principal query.
        :param int principal_source: Specifies the source to be used in the principal query.
        :param int maximum_entity_suggestions: Specifies the maximum number of principals to be returned by the
        principal query.
        :param bool force_claims: Specifies whether the principal query SHOULD be handled by claims providers.
        :param bool enabled_claim_providers: Specifies the claims providers to be used in the principal query.
        :param bool all_url_zones: Specifies whether the principal query will search all locations in the topology
        of the farm.
        :param bool allow_only_email_addresses: Specifies whether to allow the picker to resolve only email addresses as
        valid entities. This property is only used when AllowEmailAddresses (section 3.2.5.217.1.1.1) is set to True.
        Otherwise it is ignored.
        :param bool allow_multiple_entities: Specifies whether the principal query allows multiple values.
        :param bool allow_emai_addresses: Specifies whether the principal query can return a resolved principal
        matching an unverified e-mail address when unable to resolve to a known principal.
        """
        super(ClientPeoplePickerQueryParameters, self).__init__()
        self.QueryString = query_string
        self.AllowEmailAddresses = allow_emai_addresses
        self.AllowMultipleEntities = allow_multiple_entities
        self.AllowOnlyEmailAddresses = allow_only_email_addresses
        self.AllUrlZones = all_url_zones
        self.EnabledClaimProviders = enabled_claim_providers
        self.ForceClaims = force_claims
        self.MaximumEntitySuggestions = maximum_entity_suggestions
        self.PrincipalSource = principal_source
        self.PrincipalType = principal_type
        self.UrlZone = url_zone
        self.UrlZoneSpecified = url_zone_specified
        self.SharePointGroupID = sharepoint_group_id

    @property
    def entity_type_name(self):
        return "SP.UI.ApplicationPages.ClientPeoplePickerQueryParameters"
