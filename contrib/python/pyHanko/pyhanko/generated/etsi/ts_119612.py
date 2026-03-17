from dataclasses import dataclass, field
from typing import Optional, Tuple, Union

from xsdata.models.datatype import XmlDateTime

from ..w3c.xmldsig_core import KeyValue, Signature
from ..xml import Langvalue

__NAMESPACE__ = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class AnyType:
    content: Tuple[object, ...] = field(
        default_factory=tuple,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
        },
    )


@dataclass(frozen=True)
class AttributedNonEmptyURIType:
    value: str = field(
        default="",
        metadata={
            "required": True,
            "min_length": 1,
        },
    )
    type_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "type",
            "type": "Attribute",
        },
    )


@dataclass(frozen=True)
class ExpiredCertsRevocationInfo:
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"

    value: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass(frozen=True)
class NextUpdateType:
    date_time: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "dateTime",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class NonEmptyURIListType:
    uri: Tuple[str, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "URI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
            "min_length": 1,
        },
    )


@dataclass(frozen=True)
class SchemeTerritory:
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass(frozen=True)
class ServiceStatus:
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"

    value: str = field(
        default="",
        metadata={
            "required": True,
            "min_length": 1,
        },
    )


@dataclass(frozen=True)
class ServiceTypeIdentifier:
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"

    value: str = field(
        default="",
        metadata={
            "required": True,
            "min_length": 1,
        },
    )


@dataclass(frozen=True)
class TSLType:
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"

    value: str = field(
        default="",
        metadata={
            "required": True,
            "min_length": 1,
        },
    )


@dataclass(frozen=True)
class DigitalIdentityType:
    x509_certificate: Optional[bytes] = field(
        default=None,
        metadata={
            "name": "X509Certificate",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "format": "base64",
        },
    )
    x509_subject_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "X509SubjectName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    key_value: Optional[KeyValue] = field(
        default=None,
        metadata={
            "name": "KeyValue",
            "type": "Element",
            "namespace": "http://www.w3.org/2000/09/xmldsig#",
        },
    )
    x509_ski: Optional[bytes] = field(
        default=None,
        metadata={
            "name": "X509SKI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "format": "base64",
        },
    )
    other: Optional[AnyType] = field(
        default=None,
        metadata={
            "name": "Other",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class DistributionPoints(NonEmptyURIListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ExtensionType(AnyType):
    critical: Optional[bool] = field(
        default=None,
        metadata={
            "name": "Critical",
            "type": "Attribute",
            "required": True,
        },
    )


@dataclass(frozen=True)
class MultiLangNormStringType:
    value: str = field(
        default="",
        metadata={
            "required": True,
            "min_length": 1,
        },
    )
    lang: Optional[Union[str, Langvalue]] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "namespace": "http://www.w3.org/XML/1998/namespace",
            "required": True,
        },
    )


@dataclass(frozen=True)
class MultiLangStringType:
    value: str = field(
        default="",
        metadata={
            "required": True,
            "min_length": 1,
        },
    )
    lang: Optional[Union[str, Langvalue]] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "namespace": "http://www.w3.org/XML/1998/namespace",
            "required": True,
        },
    )


@dataclass(frozen=True)
class NextUpdate(NextUpdateType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class NonEmptyMultiLangURIType:
    value: str = field(
        default="",
        metadata={
            "required": True,
            "min_length": 1,
        },
    )
    lang: Optional[Union[str, Langvalue]] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "namespace": "http://www.w3.org/XML/1998/namespace",
            "required": True,
        },
    )


@dataclass(frozen=True)
class PostalAddressType:
    street_address: Optional[str] = field(
        default=None,
        metadata={
            "name": "StreetAddress",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
            "min_length": 1,
        },
    )
    locality: Optional[str] = field(
        default=None,
        metadata={
            "name": "Locality",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
            "min_length": 1,
        },
    )
    state_or_province: Optional[str] = field(
        default=None,
        metadata={
            "name": "StateOrProvince",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_length": 1,
        },
    )
    postal_code: Optional[str] = field(
        default=None,
        metadata={
            "name": "PostalCode",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_length": 1,
        },
    )
    country_name: Optional[str] = field(
        default=None,
        metadata={
            "name": "CountryName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
            "min_length": 1,
        },
    )
    lang: Optional[Union[str, Langvalue]] = field(
        default=None,
        metadata={
            "type": "Attribute",
            "namespace": "http://www.w3.org/XML/1998/namespace",
            "required": True,
        },
    )


@dataclass(frozen=True)
class ServiceSupplyPointsType:
    service_supply_point: Tuple[AttributedNonEmptyURIType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "ServiceSupplyPoint",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class AdditionalInformationType:
    textual_information: Tuple[MultiLangStringType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "TextualInformation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    other_information: Tuple[AnyType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "OtherInformation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class AdditionalServiceInformationType:
    uri: Optional[NonEmptyMultiLangURIType] = field(
        default=None,
        metadata={
            "name": "URI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    information_value: Optional[str] = field(
        default=None,
        metadata={
            "name": "InformationValue",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    other_information: Optional[AnyType] = field(
        default=None,
        metadata={
            "name": "OtherInformation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class DigitalIdentityListType:
    digital_id: Tuple[DigitalIdentityType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "DigitalId",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class ElectronicAddressType:
    uri: Tuple[NonEmptyMultiLangURIType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "URI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class Extension(ExtensionType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class InternationalNamesType:
    name: Tuple[MultiLangNormStringType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "Name",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class NonEmptyMultiLangURIListType:
    uri: Tuple[NonEmptyMultiLangURIType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "URI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class PolicyOrLegalnoticeType:
    tslpolicy: Tuple[NonEmptyMultiLangURIType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "TSLPolicy",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    tsllegal_notice: Tuple[MultiLangStringType, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "TSLLegalNotice",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class PostalAddress(PostalAddressType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ServiceSupplyPoints(ServiceSupplyPointsType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class AdditionalInformation(AdditionalInformationType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class AdditionalServiceInformation(AdditionalServiceInformationType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ElectronicAddress(ElectronicAddressType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ExtensionsListType:
    extension: Tuple[Extension, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "Extension",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class PolicyOrLegalNotice(PolicyOrLegalnoticeType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class PostalAddressListType:
    postal_address: Tuple[PostalAddress, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "PostalAddress",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class SchemeInformationURI(NonEmptyMultiLangURIListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class SchemeName(InternationalNamesType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class SchemeOperatorName(InternationalNamesType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class SchemeTypeCommunityRules(NonEmptyMultiLangURIListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ServiceDigitalIdentity(DigitalIdentityListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class PostalAddresses(PostalAddressListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ServiceDigitalIdentityListType:
    service_digital_identity: Tuple[ServiceDigitalIdentity, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "ServiceDigitalIdentity",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class ServiceHistoryInstanceType:
    service_type_identifier: Optional[ServiceTypeIdentifier] = field(
        default=None,
        metadata={
            "name": "ServiceTypeIdentifier",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_name: Optional[InternationalNamesType] = field(
        default=None,
        metadata={
            "name": "ServiceName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_digital_identity: Optional[ServiceDigitalIdentity] = field(
        default=None,
        metadata={
            "name": "ServiceDigitalIdentity",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_status: Optional[ServiceStatus] = field(
        default=None,
        metadata={
            "name": "ServiceStatus",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    status_starting_time: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "StatusStartingTime",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_information_extensions: Optional[ExtensionsListType] = field(
        default=None,
        metadata={
            "name": "ServiceInformationExtensions",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class TSPServiceInformationType:
    service_type_identifier: Optional[ServiceTypeIdentifier] = field(
        default=None,
        metadata={
            "name": "ServiceTypeIdentifier",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_name: Optional[InternationalNamesType] = field(
        default=None,
        metadata={
            "name": "ServiceName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_digital_identity: Optional[ServiceDigitalIdentity] = field(
        default=None,
        metadata={
            "name": "ServiceDigitalIdentity",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_status: Optional[ServiceStatus] = field(
        default=None,
        metadata={
            "name": "ServiceStatus",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    status_starting_time: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "StatusStartingTime",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    scheme_service_definition_uri: Optional[NonEmptyMultiLangURIListType] = (
        field(
            default=None,
            metadata={
                "name": "SchemeServiceDefinitionURI",
                "type": "Element",
                "namespace": "http://uri.etsi.org/02231/v2#",
            },
        )
    )
    service_supply_points: Optional[ServiceSupplyPoints] = field(
        default=None,
        metadata={
            "name": "ServiceSupplyPoints",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    tspservice_definition_uri: Optional[NonEmptyMultiLangURIListType] = field(
        default=None,
        metadata={
            "name": "TSPServiceDefinitionURI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    service_information_extensions: Optional[ExtensionsListType] = field(
        default=None,
        metadata={
            "name": "ServiceInformationExtensions",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class AddressType:
    postal_addresses: Optional[PostalAddresses] = field(
        default=None,
        metadata={
            "name": "PostalAddresses",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    electronic_address: Optional[ElectronicAddress] = field(
        default=None,
        metadata={
            "name": "ElectronicAddress",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )


@dataclass(frozen=True)
class ServiceDigitalIdentities(ServiceDigitalIdentityListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ServiceHistoryInstance(ServiceHistoryInstanceType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ServiceInformation(TSPServiceInformationType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class OtherTSLPointerType:
    service_digital_identities: Optional[ServiceDigitalIdentities] = field(
        default=None,
        metadata={
            "name": "ServiceDigitalIdentities",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    tsllocation: Optional[str] = field(
        default=None,
        metadata={
            "name": "TSLLocation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
            "min_length": 1,
        },
    )
    additional_information: Optional[AdditionalInformation] = field(
        default=None,
        metadata={
            "name": "AdditionalInformation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class ServiceHistoryType:
    service_history_instance: Tuple[ServiceHistoryInstance, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "ServiceHistoryInstance",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class TSPInformationType:
    tspname: Optional[InternationalNamesType] = field(
        default=None,
        metadata={
            "name": "TSPName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    tsptrade_name: Optional[InternationalNamesType] = field(
        default=None,
        metadata={
            "name": "TSPTradeName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    tspaddress: Optional[AddressType] = field(
        default=None,
        metadata={
            "name": "TSPAddress",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    tspinformation_uri: Optional[NonEmptyMultiLangURIListType] = field(
        default=None,
        metadata={
            "name": "TSPInformationURI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    tspinformation_extensions: Optional[ExtensionsListType] = field(
        default=None,
        metadata={
            "name": "TSPInformationExtensions",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class OtherTSLPointer(OtherTSLPointerType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class ServiceHistory(ServiceHistoryType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class TSPInformation(TSPInformationType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class OtherTSLPointersType:
    other_tslpointer: Tuple[OtherTSLPointer, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "OtherTSLPointer",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class TSPServiceType:
    service_information: Optional[ServiceInformation] = field(
        default=None,
        metadata={
            "name": "ServiceInformation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    service_history: Optional[ServiceHistory] = field(
        default=None,
        metadata={
            "name": "ServiceHistory",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class PointersToOtherTSL(OtherTSLPointersType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class TSPService(TSPServiceType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class TSLSchemeInformationType:
    tslversion_identifier: Optional[int] = field(
        default=None,
        metadata={
            "name": "TSLVersionIdentifier",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    tslsequence_number: Optional[int] = field(
        default=None,
        metadata={
            "name": "TSLSequenceNumber",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    tsltype: Optional[TSLType] = field(
        default=None,
        metadata={
            "name": "TSLType",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    scheme_operator_name: Optional[SchemeOperatorName] = field(
        default=None,
        metadata={
            "name": "SchemeOperatorName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    scheme_operator_address: Optional[AddressType] = field(
        default=None,
        metadata={
            "name": "SchemeOperatorAddress",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    scheme_name: Optional[SchemeName] = field(
        default=None,
        metadata={
            "name": "SchemeName",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    scheme_information_uri: Optional[SchemeInformationURI] = field(
        default=None,
        metadata={
            "name": "SchemeInformationURI",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    status_determination_approach: Optional[str] = field(
        default=None,
        metadata={
            "name": "StatusDeterminationApproach",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
            "min_length": 1,
        },
    )
    scheme_type_community_rules: Optional[SchemeTypeCommunityRules] = field(
        default=None,
        metadata={
            "name": "SchemeTypeCommunityRules",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    scheme_territory: Optional[SchemeTerritory] = field(
        default=None,
        metadata={
            "name": "SchemeTerritory",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    policy_or_legal_notice: Optional[PolicyOrLegalNotice] = field(
        default=None,
        metadata={
            "name": "PolicyOrLegalNotice",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    historical_information_period: Optional[int] = field(
        default=None,
        metadata={
            "name": "HistoricalInformationPeriod",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    pointers_to_other_tsl: Optional[PointersToOtherTSL] = field(
        default=None,
        metadata={
            "name": "PointersToOtherTSL",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    list_issue_date_time: Optional[XmlDateTime] = field(
        default=None,
        metadata={
            "name": "ListIssueDateTime",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    next_update: Optional[NextUpdate] = field(
        default=None,
        metadata={
            "name": "NextUpdate",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    distribution_points: Optional[DistributionPoints] = field(
        default=None,
        metadata={
            "name": "DistributionPoints",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    scheme_extensions: Optional[ExtensionsListType] = field(
        default=None,
        metadata={
            "name": "SchemeExtensions",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )


@dataclass(frozen=True)
class TSPServicesListType:
    tspservice: Tuple[TSPService, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "TSPService",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class SchemeInformation(TSLSchemeInformationType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class TSPServices(TSPServicesListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class TSPType:
    tspinformation: Optional[TSPInformation] = field(
        default=None,
        metadata={
            "name": "TSPInformation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    tspservices: Optional[TSPServices] = field(
        default=None,
        metadata={
            "name": "TSPServices",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )


@dataclass(frozen=True)
class TrustServiceProvider(TSPType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class TrustServiceProviderListType:
    trust_service_provider: Tuple[TrustServiceProvider, ...] = field(
        default_factory=tuple,
        metadata={
            "name": "TrustServiceProvider",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "min_occurs": 1,
        },
    )


@dataclass(frozen=True)
class TrustServiceProviderList(TrustServiceProviderListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"


@dataclass(frozen=True)
class TrustStatusListType:
    scheme_information: Optional[SchemeInformation] = field(
        default=None,
        metadata={
            "name": "SchemeInformation",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
            "required": True,
        },
    )
    trust_service_provider_list: Optional[TrustServiceProviderList] = field(
        default=None,
        metadata={
            "name": "TrustServiceProviderList",
            "type": "Element",
            "namespace": "http://uri.etsi.org/02231/v2#",
        },
    )
    signature: Optional[Signature] = field(
        default=None,
        metadata={
            "name": "Signature",
            "type": "Element",
            "namespace": "http://www.w3.org/2000/09/xmldsig#",
        },
    )
    tsltag: Optional[str] = field(
        default=None,
        metadata={
            "name": "TSLTag",
            "type": "Attribute",
            "required": True,
        },
    )
    id: Optional[str] = field(
        default=None,
        metadata={
            "name": "Id",
            "type": "Attribute",
        },
    )


@dataclass(frozen=True)
class TrustServiceStatusList(TrustStatusListType):
    class Meta:
        namespace = "http://uri.etsi.org/02231/v2#"
