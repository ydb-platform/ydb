# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from databricks.sdk.client_types import HostType
from databricks.sdk.service._internal import (_enum, _from_dict,
                                              _repeated_dict, _repeated_enum)

_LOG = logging.getLogger("databricks.sdk")


# all definitions in this file are in alphabetical order


@dataclass
class AddExchangeForListingResponse:
    exchange_for_listing: Optional[ExchangeListing] = None

    def as_dict(self) -> dict:
        """Serializes the AddExchangeForListingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exchange_for_listing:
            body["exchange_for_listing"] = self.exchange_for_listing.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the AddExchangeForListingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exchange_for_listing:
            body["exchange_for_listing"] = self.exchange_for_listing
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AddExchangeForListingResponse:
        """Deserializes the AddExchangeForListingResponse from a dictionary."""
        return cls(exchange_for_listing=_from_dict(d, "exchange_for_listing", ExchangeListing))


class AssetType(Enum):

    ASSET_TYPE_APP = "ASSET_TYPE_APP"
    ASSET_TYPE_DATA_TABLE = "ASSET_TYPE_DATA_TABLE"
    ASSET_TYPE_GIT_REPO = "ASSET_TYPE_GIT_REPO"
    ASSET_TYPE_MCP = "ASSET_TYPE_MCP"
    ASSET_TYPE_MEDIA = "ASSET_TYPE_MEDIA"
    ASSET_TYPE_MODEL = "ASSET_TYPE_MODEL"
    ASSET_TYPE_NOTEBOOK = "ASSET_TYPE_NOTEBOOK"
    ASSET_TYPE_PARTNER_INTEGRATION = "ASSET_TYPE_PARTNER_INTEGRATION"


@dataclass
class BatchGetListingsResponse:
    listings: Optional[List[Listing]] = None

    def as_dict(self) -> dict:
        """Serializes the BatchGetListingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.listings:
            body["listings"] = [v.as_dict() for v in self.listings]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BatchGetListingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.listings:
            body["listings"] = self.listings
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BatchGetListingsResponse:
        """Deserializes the BatchGetListingsResponse from a dictionary."""
        return cls(listings=_repeated_dict(d, "listings", Listing))


@dataclass
class BatchGetProvidersResponse:
    providers: Optional[List[ProviderInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the BatchGetProvidersResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.providers:
            body["providers"] = [v.as_dict() for v in self.providers]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the BatchGetProvidersResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.providers:
            body["providers"] = self.providers
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> BatchGetProvidersResponse:
        """Deserializes the BatchGetProvidersResponse from a dictionary."""
        return cls(providers=_repeated_dict(d, "providers", ProviderInfo))


class Category(Enum):

    ADVERTISING_AND_MARKETING = "ADVERTISING_AND_MARKETING"
    CLIMATE_AND_ENVIRONMENT = "CLIMATE_AND_ENVIRONMENT"
    COMMERCE = "COMMERCE"
    DEMOGRAPHICS = "DEMOGRAPHICS"
    ECONOMICS = "ECONOMICS"
    EDUCATION = "EDUCATION"
    ENERGY = "ENERGY"
    FINANCIAL = "FINANCIAL"
    GAMING = "GAMING"
    GEOSPATIAL = "GEOSPATIAL"
    HEALTH = "HEALTH"
    LOOKUP_TABLES = "LOOKUP_TABLES"
    MANUFACTURING = "MANUFACTURING"
    MEDIA = "MEDIA"
    OTHER = "OTHER"
    PUBLIC_SECTOR = "PUBLIC_SECTOR"
    RETAIL = "RETAIL"
    SCIENCE_AND_RESEARCH = "SCIENCE_AND_RESEARCH"
    SECURITY = "SECURITY"
    SPORTS = "SPORTS"
    TRANSPORTATION_AND_LOGISTICS = "TRANSPORTATION_AND_LOGISTICS"
    TRAVEL_AND_TOURISM = "TRAVEL_AND_TOURISM"


@dataclass
class ConsumerTerms:
    version: str

    def as_dict(self) -> dict:
        """Serializes the ConsumerTerms into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ConsumerTerms into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ConsumerTerms:
        """Deserializes the ConsumerTerms from a dictionary."""
        return cls(version=d.get("version", None))


@dataclass
class ContactInfo:
    """contact info for the consumer requesting data or performing a listing installation"""

    company: Optional[str] = None

    email: Optional[str] = None

    first_name: Optional[str] = None

    last_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ContactInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.company is not None:
            body["company"] = self.company
        if self.email is not None:
            body["email"] = self.email
        if self.first_name is not None:
            body["first_name"] = self.first_name
        if self.last_name is not None:
            body["last_name"] = self.last_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ContactInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.company is not None:
            body["company"] = self.company
        if self.email is not None:
            body["email"] = self.email
        if self.first_name is not None:
            body["first_name"] = self.first_name
        if self.last_name is not None:
            body["last_name"] = self.last_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ContactInfo:
        """Deserializes the ContactInfo from a dictionary."""
        return cls(
            company=d.get("company", None),
            email=d.get("email", None),
            first_name=d.get("first_name", None),
            last_name=d.get("last_name", None),
        )


class Cost(Enum):

    FREE = "FREE"
    PAID = "PAID"


@dataclass
class CreateExchangeFilterResponse:
    filter_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the CreateExchangeFilterResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.filter_id is not None:
            body["filter_id"] = self.filter_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateExchangeFilterResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.filter_id is not None:
            body["filter_id"] = self.filter_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateExchangeFilterResponse:
        """Deserializes the CreateExchangeFilterResponse from a dictionary."""
        return cls(filter_id=d.get("filter_id", None))


@dataclass
class CreateExchangeResponse:
    exchange_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the CreateExchangeResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exchange_id is not None:
            body["exchange_id"] = self.exchange_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateExchangeResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exchange_id is not None:
            body["exchange_id"] = self.exchange_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateExchangeResponse:
        """Deserializes the CreateExchangeResponse from a dictionary."""
        return cls(exchange_id=d.get("exchange_id", None))


@dataclass
class CreateFileResponse:
    file_info: Optional[FileInfo] = None

    upload_url: Optional[str] = None
    """Pre-signed POST URL to blob storage"""

    def as_dict(self) -> dict:
        """Serializes the CreateFileResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_info:
            body["file_info"] = self.file_info.as_dict()
        if self.upload_url is not None:
            body["upload_url"] = self.upload_url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateFileResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_info:
            body["file_info"] = self.file_info
        if self.upload_url is not None:
            body["upload_url"] = self.upload_url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateFileResponse:
        """Deserializes the CreateFileResponse from a dictionary."""
        return cls(file_info=_from_dict(d, "file_info", FileInfo), upload_url=d.get("upload_url", None))


@dataclass
class CreateListingResponse:
    listing_id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the CreateListingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateListingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateListingResponse:
        """Deserializes the CreateListingResponse from a dictionary."""
        return cls(listing_id=d.get("listing_id", None))


@dataclass
class CreatePersonalizationRequestResponse:
    id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the CreatePersonalizationRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreatePersonalizationRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreatePersonalizationRequestResponse:
        """Deserializes the CreatePersonalizationRequestResponse from a dictionary."""
        return cls(id=d.get("id", None))


@dataclass
class CreateProviderResponse:
    id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the CreateProviderResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the CreateProviderResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CreateProviderResponse:
        """Deserializes the CreateProviderResponse from a dictionary."""
        return cls(id=d.get("id", None))


class DataRefresh(Enum):

    DAILY = "DAILY"
    HOURLY = "HOURLY"
    MINUTE = "MINUTE"
    MONTHLY = "MONTHLY"
    NONE = "NONE"
    QUARTERLY = "QUARTERLY"
    SECOND = "SECOND"
    WEEKLY = "WEEKLY"
    YEARLY = "YEARLY"


@dataclass
class DataRefreshInfo:
    interval: int

    unit: DataRefresh

    def as_dict(self) -> dict:
        """Serializes the DataRefreshInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.interval is not None:
            body["interval"] = self.interval
        if self.unit is not None:
            body["unit"] = self.unit.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DataRefreshInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.interval is not None:
            body["interval"] = self.interval
        if self.unit is not None:
            body["unit"] = self.unit
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DataRefreshInfo:
        """Deserializes the DataRefreshInfo from a dictionary."""
        return cls(interval=d.get("interval", None), unit=_enum(d, "unit", DataRefresh))


@dataclass
class DeleteExchangeFilterResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteExchangeFilterResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteExchangeFilterResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteExchangeFilterResponse:
        """Deserializes the DeleteExchangeFilterResponse from a dictionary."""
        return cls()


@dataclass
class DeleteExchangeResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteExchangeResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteExchangeResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteExchangeResponse:
        """Deserializes the DeleteExchangeResponse from a dictionary."""
        return cls()


@dataclass
class DeleteFileResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteFileResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteFileResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteFileResponse:
        """Deserializes the DeleteFileResponse from a dictionary."""
        return cls()


@dataclass
class DeleteInstallationResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteInstallationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteInstallationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteInstallationResponse:
        """Deserializes the DeleteInstallationResponse from a dictionary."""
        return cls()


@dataclass
class DeleteListingResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteListingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteListingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteListingResponse:
        """Deserializes the DeleteListingResponse from a dictionary."""
        return cls()


@dataclass
class DeleteProviderResponse:
    def as_dict(self) -> dict:
        """Serializes the DeleteProviderResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the DeleteProviderResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeleteProviderResponse:
        """Deserializes the DeleteProviderResponse from a dictionary."""
        return cls()


class DeltaSharingRecipientType(Enum):

    DELTA_SHARING_RECIPIENT_TYPE_DATABRICKS = "DELTA_SHARING_RECIPIENT_TYPE_DATABRICKS"
    DELTA_SHARING_RECIPIENT_TYPE_OPEN = "DELTA_SHARING_RECIPIENT_TYPE_OPEN"


@dataclass
class Exchange:
    name: str

    comment: Optional[str] = None

    created_at: Optional[int] = None

    created_by: Optional[str] = None

    filters: Optional[List[ExchangeFilter]] = None

    id: Optional[str] = None

    linked_listings: Optional[List[ExchangeListing]] = None

    updated_at: Optional[int] = None

    updated_by: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the Exchange into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.filters:
            body["filters"] = [v.as_dict() for v in self.filters]
        if self.id is not None:
            body["id"] = self.id
        if self.linked_listings:
            body["linked_listings"] = [v.as_dict() for v in self.linked_listings]
        if self.name is not None:
            body["name"] = self.name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Exchange into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.filters:
            body["filters"] = self.filters
        if self.id is not None:
            body["id"] = self.id
        if self.linked_listings:
            body["linked_listings"] = self.linked_listings
        if self.name is not None:
            body["name"] = self.name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Exchange:
        """Deserializes the Exchange from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            filters=_repeated_dict(d, "filters", ExchangeFilter),
            id=d.get("id", None),
            linked_listings=_repeated_dict(d, "linked_listings", ExchangeListing),
            name=d.get("name", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class ExchangeFilter:
    exchange_id: str

    filter_value: str

    filter_type: ExchangeFilterType

    created_at: Optional[int] = None

    created_by: Optional[str] = None

    id: Optional[str] = None

    name: Optional[str] = None

    updated_at: Optional[int] = None

    updated_by: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ExchangeFilter into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.exchange_id is not None:
            body["exchange_id"] = self.exchange_id
        if self.filter_type is not None:
            body["filter_type"] = self.filter_type.value
        if self.filter_value is not None:
            body["filter_value"] = self.filter_value
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExchangeFilter into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.exchange_id is not None:
            body["exchange_id"] = self.exchange_id
        if self.filter_type is not None:
            body["filter_type"] = self.filter_type
        if self.filter_value is not None:
            body["filter_value"] = self.filter_value
        if self.id is not None:
            body["id"] = self.id
        if self.name is not None:
            body["name"] = self.name
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExchangeFilter:
        """Deserializes the ExchangeFilter from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            exchange_id=d.get("exchange_id", None),
            filter_type=_enum(d, "filter_type", ExchangeFilterType),
            filter_value=d.get("filter_value", None),
            id=d.get("id", None),
            name=d.get("name", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


class ExchangeFilterType(Enum):

    GLOBAL_METASTORE_ID = "GLOBAL_METASTORE_ID"


@dataclass
class ExchangeListing:
    created_at: Optional[int] = None

    created_by: Optional[str] = None

    exchange_id: Optional[str] = None

    exchange_name: Optional[str] = None

    id: Optional[str] = None

    listing_id: Optional[str] = None

    listing_name: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ExchangeListing into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.exchange_id is not None:
            body["exchange_id"] = self.exchange_id
        if self.exchange_name is not None:
            body["exchange_name"] = self.exchange_name
        if self.id is not None:
            body["id"] = self.id
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.listing_name is not None:
            body["listing_name"] = self.listing_name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ExchangeListing into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.exchange_id is not None:
            body["exchange_id"] = self.exchange_id
        if self.exchange_name is not None:
            body["exchange_name"] = self.exchange_name
        if self.id is not None:
            body["id"] = self.id
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.listing_name is not None:
            body["listing_name"] = self.listing_name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ExchangeListing:
        """Deserializes the ExchangeListing from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            exchange_id=d.get("exchange_id", None),
            exchange_name=d.get("exchange_name", None),
            id=d.get("id", None),
            listing_id=d.get("listing_id", None),
            listing_name=d.get("listing_name", None),
        )


@dataclass
class FileInfo:
    created_at: Optional[int] = None

    display_name: Optional[str] = None
    """Name displayed to users for applicable files, e.g. embedded notebooks"""

    download_link: Optional[str] = None

    file_parent: Optional[FileParent] = None

    id: Optional[str] = None

    marketplace_file_type: Optional[MarketplaceFileType] = None

    mime_type: Optional[str] = None

    status: Optional[FileStatus] = None

    status_message: Optional[str] = None
    """Populated if status is in a failed state with more information on reason for the failure."""

    updated_at: Optional[int] = None

    def as_dict(self) -> dict:
        """Serializes the FileInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.download_link is not None:
            body["download_link"] = self.download_link
        if self.file_parent:
            body["file_parent"] = self.file_parent.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.marketplace_file_type is not None:
            body["marketplace_file_type"] = self.marketplace_file_type.value
        if self.mime_type is not None:
            body["mime_type"] = self.mime_type
        if self.status is not None:
            body["status"] = self.status.value
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.display_name is not None:
            body["display_name"] = self.display_name
        if self.download_link is not None:
            body["download_link"] = self.download_link
        if self.file_parent:
            body["file_parent"] = self.file_parent
        if self.id is not None:
            body["id"] = self.id
        if self.marketplace_file_type is not None:
            body["marketplace_file_type"] = self.marketplace_file_type
        if self.mime_type is not None:
            body["mime_type"] = self.mime_type
        if self.status is not None:
            body["status"] = self.status
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileInfo:
        """Deserializes the FileInfo from a dictionary."""
        return cls(
            created_at=d.get("created_at", None),
            display_name=d.get("display_name", None),
            download_link=d.get("download_link", None),
            file_parent=_from_dict(d, "file_parent", FileParent),
            id=d.get("id", None),
            marketplace_file_type=_enum(d, "marketplace_file_type", MarketplaceFileType),
            mime_type=d.get("mime_type", None),
            status=_enum(d, "status", FileStatus),
            status_message=d.get("status_message", None),
            updated_at=d.get("updated_at", None),
        )


@dataclass
class FileParent:
    file_parent_type: Optional[FileParentType] = None

    parent_id: Optional[str] = None
    """TODO make the following fields required"""

    def as_dict(self) -> dict:
        """Serializes the FileParent into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_parent_type is not None:
            body["file_parent_type"] = self.file_parent_type.value
        if self.parent_id is not None:
            body["parent_id"] = self.parent_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the FileParent into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_parent_type is not None:
            body["file_parent_type"] = self.file_parent_type
        if self.parent_id is not None:
            body["parent_id"] = self.parent_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> FileParent:
        """Deserializes the FileParent from a dictionary."""
        return cls(file_parent_type=_enum(d, "file_parent_type", FileParentType), parent_id=d.get("parent_id", None))


class FileParentType(Enum):

    LISTING = "LISTING"
    LISTING_RESOURCE = "LISTING_RESOURCE"
    PROVIDER = "PROVIDER"


class FileStatus(Enum):

    FILE_STATUS_PUBLISHED = "FILE_STATUS_PUBLISHED"
    FILE_STATUS_SANITIZATION_FAILED = "FILE_STATUS_SANITIZATION_FAILED"
    FILE_STATUS_SANITIZING = "FILE_STATUS_SANITIZING"
    FILE_STATUS_STAGING = "FILE_STATUS_STAGING"


class FulfillmentType(Enum):

    INSTALL = "INSTALL"
    REQUEST_ACCESS = "REQUEST_ACCESS"


@dataclass
class GetExchangeResponse:
    exchange: Optional[Exchange] = None

    def as_dict(self) -> dict:
        """Serializes the GetExchangeResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exchange:
            body["exchange"] = self.exchange.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetExchangeResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exchange:
            body["exchange"] = self.exchange
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetExchangeResponse:
        """Deserializes the GetExchangeResponse from a dictionary."""
        return cls(exchange=_from_dict(d, "exchange", Exchange))


@dataclass
class GetFileResponse:
    file_info: Optional[FileInfo] = None

    def as_dict(self) -> dict:
        """Serializes the GetFileResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_info:
            body["file_info"] = self.file_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetFileResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_info:
            body["file_info"] = self.file_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetFileResponse:
        """Deserializes the GetFileResponse from a dictionary."""
        return cls(file_info=_from_dict(d, "file_info", FileInfo))


@dataclass
class GetLatestVersionProviderAnalyticsDashboardResponse:
    version: Optional[int] = None
    """version here is latest logical version of the dashboard template"""

    def as_dict(self) -> dict:
        """Serializes the GetLatestVersionProviderAnalyticsDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetLatestVersionProviderAnalyticsDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetLatestVersionProviderAnalyticsDashboardResponse:
        """Deserializes the GetLatestVersionProviderAnalyticsDashboardResponse from a dictionary."""
        return cls(version=d.get("version", None))


@dataclass
class GetListingContentMetadataResponse:
    next_page_token: Optional[str] = None

    shared_data_objects: Optional[List[SharedDataObject]] = None

    def as_dict(self) -> dict:
        """Serializes the GetListingContentMetadataResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.shared_data_objects:
            body["shared_data_objects"] = [v.as_dict() for v in self.shared_data_objects]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetListingContentMetadataResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.shared_data_objects:
            body["shared_data_objects"] = self.shared_data_objects
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetListingContentMetadataResponse:
        """Deserializes the GetListingContentMetadataResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            shared_data_objects=_repeated_dict(d, "shared_data_objects", SharedDataObject),
        )


@dataclass
class GetListingResponse:
    listing: Optional[Listing] = None

    def as_dict(self) -> dict:
        """Serializes the GetListingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.listing:
            body["listing"] = self.listing.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetListingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.listing:
            body["listing"] = self.listing
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetListingResponse:
        """Deserializes the GetListingResponse from a dictionary."""
        return cls(listing=_from_dict(d, "listing", Listing))


@dataclass
class GetListingsResponse:
    listings: Optional[List[Listing]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the GetListingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.listings:
            body["listings"] = [v.as_dict() for v in self.listings]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetListingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.listings:
            body["listings"] = self.listings
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetListingsResponse:
        """Deserializes the GetListingsResponse from a dictionary."""
        return cls(listings=_repeated_dict(d, "listings", Listing), next_page_token=d.get("next_page_token", None))


@dataclass
class GetPersonalizationRequestResponse:
    personalization_requests: Optional[List[PersonalizationRequest]] = None

    def as_dict(self) -> dict:
        """Serializes the GetPersonalizationRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.personalization_requests:
            body["personalization_requests"] = [v.as_dict() for v in self.personalization_requests]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetPersonalizationRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.personalization_requests:
            body["personalization_requests"] = self.personalization_requests
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetPersonalizationRequestResponse:
        """Deserializes the GetPersonalizationRequestResponse from a dictionary."""
        return cls(personalization_requests=_repeated_dict(d, "personalization_requests", PersonalizationRequest))


@dataclass
class GetProviderResponse:
    provider: Optional[ProviderInfo] = None

    def as_dict(self) -> dict:
        """Serializes the GetProviderResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.provider:
            body["provider"] = self.provider.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the GetProviderResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.provider:
            body["provider"] = self.provider
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> GetProviderResponse:
        """Deserializes the GetProviderResponse from a dictionary."""
        return cls(provider=_from_dict(d, "provider", ProviderInfo))


@dataclass
class Installation:
    installation: Optional[InstallationDetail] = None

    def as_dict(self) -> dict:
        """Serializes the Installation into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.installation:
            body["installation"] = self.installation.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Installation into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.installation:
            body["installation"] = self.installation
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Installation:
        """Deserializes the Installation from a dictionary."""
        return cls(installation=_from_dict(d, "installation", InstallationDetail))


@dataclass
class InstallationDetail:
    catalog_name: Optional[str] = None

    error_message: Optional[str] = None

    id: Optional[str] = None

    installed_on: Optional[int] = None

    listing_id: Optional[str] = None

    listing_name: Optional[str] = None

    recipient_type: Optional[DeltaSharingRecipientType] = None

    repo_name: Optional[str] = None

    repo_path: Optional[str] = None

    share_name: Optional[str] = None

    status: Optional[InstallationStatus] = None

    token_detail: Optional[TokenDetail] = None

    tokens: Optional[List[TokenInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the InstallationDetail into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.id is not None:
            body["id"] = self.id
        if self.installed_on is not None:
            body["installed_on"] = self.installed_on
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.listing_name is not None:
            body["listing_name"] = self.listing_name
        if self.recipient_type is not None:
            body["recipient_type"] = self.recipient_type.value
        if self.repo_name is not None:
            body["repo_name"] = self.repo_name
        if self.repo_path is not None:
            body["repo_path"] = self.repo_path
        if self.share_name is not None:
            body["share_name"] = self.share_name
        if self.status is not None:
            body["status"] = self.status.value
        if self.token_detail:
            body["token_detail"] = self.token_detail.as_dict()
        if self.tokens:
            body["tokens"] = [v.as_dict() for v in self.tokens]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the InstallationDetail into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.catalog_name is not None:
            body["catalog_name"] = self.catalog_name
        if self.error_message is not None:
            body["error_message"] = self.error_message
        if self.id is not None:
            body["id"] = self.id
        if self.installed_on is not None:
            body["installed_on"] = self.installed_on
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.listing_name is not None:
            body["listing_name"] = self.listing_name
        if self.recipient_type is not None:
            body["recipient_type"] = self.recipient_type
        if self.repo_name is not None:
            body["repo_name"] = self.repo_name
        if self.repo_path is not None:
            body["repo_path"] = self.repo_path
        if self.share_name is not None:
            body["share_name"] = self.share_name
        if self.status is not None:
            body["status"] = self.status
        if self.token_detail:
            body["token_detail"] = self.token_detail
        if self.tokens:
            body["tokens"] = self.tokens
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> InstallationDetail:
        """Deserializes the InstallationDetail from a dictionary."""
        return cls(
            catalog_name=d.get("catalog_name", None),
            error_message=d.get("error_message", None),
            id=d.get("id", None),
            installed_on=d.get("installed_on", None),
            listing_id=d.get("listing_id", None),
            listing_name=d.get("listing_name", None),
            recipient_type=_enum(d, "recipient_type", DeltaSharingRecipientType),
            repo_name=d.get("repo_name", None),
            repo_path=d.get("repo_path", None),
            share_name=d.get("share_name", None),
            status=_enum(d, "status", InstallationStatus),
            token_detail=_from_dict(d, "token_detail", TokenDetail),
            tokens=_repeated_dict(d, "tokens", TokenInfo),
        )


class InstallationStatus(Enum):

    FAILED = "FAILED"
    INSTALLED = "INSTALLED"


@dataclass
class ListAllInstallationsResponse:
    installations: Optional[List[InstallationDetail]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListAllInstallationsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.installations:
            body["installations"] = [v.as_dict() for v in self.installations]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAllInstallationsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.installations:
            body["installations"] = self.installations
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAllInstallationsResponse:
        """Deserializes the ListAllInstallationsResponse from a dictionary."""
        return cls(
            installations=_repeated_dict(d, "installations", InstallationDetail),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListAllPersonalizationRequestsResponse:
    next_page_token: Optional[str] = None

    personalization_requests: Optional[List[PersonalizationRequest]] = None

    def as_dict(self) -> dict:
        """Serializes the ListAllPersonalizationRequestsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.personalization_requests:
            body["personalization_requests"] = [v.as_dict() for v in self.personalization_requests]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListAllPersonalizationRequestsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.personalization_requests:
            body["personalization_requests"] = self.personalization_requests
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListAllPersonalizationRequestsResponse:
        """Deserializes the ListAllPersonalizationRequestsResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None),
            personalization_requests=_repeated_dict(d, "personalization_requests", PersonalizationRequest),
        )


@dataclass
class ListExchangeFiltersResponse:
    filters: Optional[List[ExchangeFilter]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListExchangeFiltersResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.filters:
            body["filters"] = [v.as_dict() for v in self.filters]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListExchangeFiltersResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.filters:
            body["filters"] = self.filters
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListExchangeFiltersResponse:
        """Deserializes the ListExchangeFiltersResponse from a dictionary."""
        return cls(filters=_repeated_dict(d, "filters", ExchangeFilter), next_page_token=d.get("next_page_token", None))


@dataclass
class ListExchangesForListingResponse:
    exchange_listing: Optional[List[ExchangeListing]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListExchangesForListingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exchange_listing:
            body["exchange_listing"] = [v.as_dict() for v in self.exchange_listing]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListExchangesForListingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exchange_listing:
            body["exchange_listing"] = self.exchange_listing
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListExchangesForListingResponse:
        """Deserializes the ListExchangesForListingResponse from a dictionary."""
        return cls(
            exchange_listing=_repeated_dict(d, "exchange_listing", ExchangeListing),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListExchangesResponse:
    exchanges: Optional[List[Exchange]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListExchangesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exchanges:
            body["exchanges"] = [v.as_dict() for v in self.exchanges]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListExchangesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exchanges:
            body["exchanges"] = self.exchanges
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListExchangesResponse:
        """Deserializes the ListExchangesResponse from a dictionary."""
        return cls(exchanges=_repeated_dict(d, "exchanges", Exchange), next_page_token=d.get("next_page_token", None))


@dataclass
class ListFilesResponse:
    file_infos: Optional[List[FileInfo]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListFilesResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.file_infos:
            body["file_infos"] = [v.as_dict() for v in self.file_infos]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListFilesResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.file_infos:
            body["file_infos"] = self.file_infos
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListFilesResponse:
        """Deserializes the ListFilesResponse from a dictionary."""
        return cls(file_infos=_repeated_dict(d, "file_infos", FileInfo), next_page_token=d.get("next_page_token", None))


@dataclass
class ListFulfillmentsResponse:
    fulfillments: Optional[List[ListingFulfillment]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListFulfillmentsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.fulfillments:
            body["fulfillments"] = [v.as_dict() for v in self.fulfillments]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListFulfillmentsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.fulfillments:
            body["fulfillments"] = self.fulfillments
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListFulfillmentsResponse:
        """Deserializes the ListFulfillmentsResponse from a dictionary."""
        return cls(
            fulfillments=_repeated_dict(d, "fulfillments", ListingFulfillment),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListInstallationsResponse:
    installations: Optional[List[InstallationDetail]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListInstallationsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.installations:
            body["installations"] = [v.as_dict() for v in self.installations]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListInstallationsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.installations:
            body["installations"] = self.installations
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListInstallationsResponse:
        """Deserializes the ListInstallationsResponse from a dictionary."""
        return cls(
            installations=_repeated_dict(d, "installations", InstallationDetail),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListListingsForExchangeResponse:
    exchange_listings: Optional[List[ExchangeListing]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListListingsForExchangeResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exchange_listings:
            body["exchange_listings"] = [v.as_dict() for v in self.exchange_listings]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListListingsForExchangeResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exchange_listings:
            body["exchange_listings"] = self.exchange_listings
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListListingsForExchangeResponse:
        """Deserializes the ListListingsForExchangeResponse from a dictionary."""
        return cls(
            exchange_listings=_repeated_dict(d, "exchange_listings", ExchangeListing),
            next_page_token=d.get("next_page_token", None),
        )


@dataclass
class ListListingsResponse:
    listings: Optional[List[Listing]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ListListingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.listings:
            body["listings"] = [v.as_dict() for v in self.listings]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListListingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.listings:
            body["listings"] = self.listings
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListListingsResponse:
        """Deserializes the ListListingsResponse from a dictionary."""
        return cls(listings=_repeated_dict(d, "listings", Listing), next_page_token=d.get("next_page_token", None))


@dataclass
class ListProviderAnalyticsDashboardResponse:
    id: str

    dashboard_id: str
    """dashboard_id will be used to open Lakeview dashboard."""

    version: Optional[int] = None

    def as_dict(self) -> dict:
        """Serializes the ListProviderAnalyticsDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.id is not None:
            body["id"] = self.id
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListProviderAnalyticsDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.id is not None:
            body["id"] = self.id
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListProviderAnalyticsDashboardResponse:
        """Deserializes the ListProviderAnalyticsDashboardResponse from a dictionary."""
        return cls(dashboard_id=d.get("dashboard_id", None), id=d.get("id", None), version=d.get("version", None))


@dataclass
class ListProvidersResponse:
    next_page_token: Optional[str] = None

    providers: Optional[List[ProviderInfo]] = None

    def as_dict(self) -> dict:
        """Serializes the ListProvidersResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.providers:
            body["providers"] = [v.as_dict() for v in self.providers]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListProvidersResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        if self.providers:
            body["providers"] = self.providers
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListProvidersResponse:
        """Deserializes the ListProvidersResponse from a dictionary."""
        return cls(
            next_page_token=d.get("next_page_token", None), providers=_repeated_dict(d, "providers", ProviderInfo)
        )


@dataclass
class Listing:
    summary: ListingSummary

    detail: Optional[ListingDetail] = None

    id: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the Listing into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.detail:
            body["detail"] = self.detail.as_dict()
        if self.id is not None:
            body["id"] = self.id
        if self.summary:
            body["summary"] = self.summary.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the Listing into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.detail:
            body["detail"] = self.detail
        if self.id is not None:
            body["id"] = self.id
        if self.summary:
            body["summary"] = self.summary
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Listing:
        """Deserializes the Listing from a dictionary."""
        return cls(
            detail=_from_dict(d, "detail", ListingDetail),
            id=d.get("id", None),
            summary=_from_dict(d, "summary", ListingSummary),
        )


@dataclass
class ListingDetail:
    assets: Optional[List[AssetType]] = None
    """Type of assets included in the listing. eg. GIT_REPO, DATA_TABLE, MODEL, NOTEBOOK"""

    collection_date_end: Optional[int] = None
    """The ending date timestamp for when the data spans"""

    collection_date_start: Optional[int] = None
    """The starting date timestamp for when the data spans"""

    collection_granularity: Optional[DataRefreshInfo] = None
    """Smallest unit of time in the dataset"""

    cost: Optional[Cost] = None
    """Whether the dataset is free or paid"""

    data_source: Optional[str] = None
    """Where/how the data is sourced"""

    description: Optional[str] = None

    documentation_link: Optional[str] = None

    embedded_notebook_file_infos: Optional[List[FileInfo]] = None

    file_ids: Optional[List[str]] = None

    geographical_coverage: Optional[str] = None
    """Which geo region the listing data is collected from"""

    license: Optional[str] = None
    """ID 20, 21 removed don't use License of the data asset - Required for listings with model based
    assets"""

    pricing_model: Optional[str] = None
    """What the pricing model is (e.g. paid, subscription, paid upfront); should only be present if
    cost is paid TODO: Not used yet, should deprecate if we will never use it"""

    privacy_policy_link: Optional[str] = None

    size: Optional[float] = None
    """size of the dataset in GB"""

    support_link: Optional[str] = None

    tags: Optional[List[ListingTag]] = None
    """Listing tags - Simple key value pair to annotate listings. When should I use tags vs dedicated
    fields? Using tags avoids the need to add new columns in the database for new annotations.
    However, this should be used sparingly since tags are stored as key value pair. Use tags only:
    1. If the field is optional and won't need to have NOT NULL integrity check 2. The value is
    fairly fixed, static and low cardinality (eg. enums). 3. The value won't be used in filters or
    joins with other tables."""

    terms_of_service: Optional[str] = None

    update_frequency: Optional[DataRefreshInfo] = None
    """How often data is updated"""

    def as_dict(self) -> dict:
        """Serializes the ListingDetail into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.assets:
            body["assets"] = [v.value for v in self.assets]
        if self.collection_date_end is not None:
            body["collection_date_end"] = self.collection_date_end
        if self.collection_date_start is not None:
            body["collection_date_start"] = self.collection_date_start
        if self.collection_granularity:
            body["collection_granularity"] = self.collection_granularity.as_dict()
        if self.cost is not None:
            body["cost"] = self.cost.value
        if self.data_source is not None:
            body["data_source"] = self.data_source
        if self.description is not None:
            body["description"] = self.description
        if self.documentation_link is not None:
            body["documentation_link"] = self.documentation_link
        if self.embedded_notebook_file_infos:
            body["embedded_notebook_file_infos"] = [v.as_dict() for v in self.embedded_notebook_file_infos]
        if self.file_ids:
            body["file_ids"] = [v for v in self.file_ids]
        if self.geographical_coverage is not None:
            body["geographical_coverage"] = self.geographical_coverage
        if self.license is not None:
            body["license"] = self.license
        if self.pricing_model is not None:
            body["pricing_model"] = self.pricing_model
        if self.privacy_policy_link is not None:
            body["privacy_policy_link"] = self.privacy_policy_link
        if self.size is not None:
            body["size"] = self.size
        if self.support_link is not None:
            body["support_link"] = self.support_link
        if self.tags:
            body["tags"] = [v.as_dict() for v in self.tags]
        if self.terms_of_service is not None:
            body["terms_of_service"] = self.terms_of_service
        if self.update_frequency:
            body["update_frequency"] = self.update_frequency.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListingDetail into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.assets:
            body["assets"] = self.assets
        if self.collection_date_end is not None:
            body["collection_date_end"] = self.collection_date_end
        if self.collection_date_start is not None:
            body["collection_date_start"] = self.collection_date_start
        if self.collection_granularity:
            body["collection_granularity"] = self.collection_granularity
        if self.cost is not None:
            body["cost"] = self.cost
        if self.data_source is not None:
            body["data_source"] = self.data_source
        if self.description is not None:
            body["description"] = self.description
        if self.documentation_link is not None:
            body["documentation_link"] = self.documentation_link
        if self.embedded_notebook_file_infos:
            body["embedded_notebook_file_infos"] = self.embedded_notebook_file_infos
        if self.file_ids:
            body["file_ids"] = self.file_ids
        if self.geographical_coverage is not None:
            body["geographical_coverage"] = self.geographical_coverage
        if self.license is not None:
            body["license"] = self.license
        if self.pricing_model is not None:
            body["pricing_model"] = self.pricing_model
        if self.privacy_policy_link is not None:
            body["privacy_policy_link"] = self.privacy_policy_link
        if self.size is not None:
            body["size"] = self.size
        if self.support_link is not None:
            body["support_link"] = self.support_link
        if self.tags:
            body["tags"] = self.tags
        if self.terms_of_service is not None:
            body["terms_of_service"] = self.terms_of_service
        if self.update_frequency:
            body["update_frequency"] = self.update_frequency
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListingDetail:
        """Deserializes the ListingDetail from a dictionary."""
        return cls(
            assets=_repeated_enum(d, "assets", AssetType),
            collection_date_end=d.get("collection_date_end", None),
            collection_date_start=d.get("collection_date_start", None),
            collection_granularity=_from_dict(d, "collection_granularity", DataRefreshInfo),
            cost=_enum(d, "cost", Cost),
            data_source=d.get("data_source", None),
            description=d.get("description", None),
            documentation_link=d.get("documentation_link", None),
            embedded_notebook_file_infos=_repeated_dict(d, "embedded_notebook_file_infos", FileInfo),
            file_ids=d.get("file_ids", None),
            geographical_coverage=d.get("geographical_coverage", None),
            license=d.get("license", None),
            pricing_model=d.get("pricing_model", None),
            privacy_policy_link=d.get("privacy_policy_link", None),
            size=d.get("size", None),
            support_link=d.get("support_link", None),
            tags=_repeated_dict(d, "tags", ListingTag),
            terms_of_service=d.get("terms_of_service", None),
            update_frequency=_from_dict(d, "update_frequency", DataRefreshInfo),
        )


@dataclass
class ListingFulfillment:
    listing_id: str

    fulfillment_type: Optional[FulfillmentType] = None

    recipient_type: Optional[DeltaSharingRecipientType] = None

    repo_info: Optional[RepoInfo] = None

    share_info: Optional[ShareInfo] = None

    def as_dict(self) -> dict:
        """Serializes the ListingFulfillment into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.fulfillment_type is not None:
            body["fulfillment_type"] = self.fulfillment_type.value
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.recipient_type is not None:
            body["recipient_type"] = self.recipient_type.value
        if self.repo_info:
            body["repo_info"] = self.repo_info.as_dict()
        if self.share_info:
            body["share_info"] = self.share_info.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListingFulfillment into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.fulfillment_type is not None:
            body["fulfillment_type"] = self.fulfillment_type
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.recipient_type is not None:
            body["recipient_type"] = self.recipient_type
        if self.repo_info:
            body["repo_info"] = self.repo_info
        if self.share_info:
            body["share_info"] = self.share_info
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListingFulfillment:
        """Deserializes the ListingFulfillment from a dictionary."""
        return cls(
            fulfillment_type=_enum(d, "fulfillment_type", FulfillmentType),
            listing_id=d.get("listing_id", None),
            recipient_type=_enum(d, "recipient_type", DeltaSharingRecipientType),
            repo_info=_from_dict(d, "repo_info", RepoInfo),
            share_info=_from_dict(d, "share_info", ShareInfo),
        )


@dataclass
class ListingSetting:
    visibility: Optional[Visibility] = None

    def as_dict(self) -> dict:
        """Serializes the ListingSetting into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.visibility is not None:
            body["visibility"] = self.visibility.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListingSetting into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.visibility is not None:
            body["visibility"] = self.visibility
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListingSetting:
        """Deserializes the ListingSetting from a dictionary."""
        return cls(visibility=_enum(d, "visibility", Visibility))


class ListingShareType(Enum):

    FULL = "FULL"
    SAMPLE = "SAMPLE"


class ListingStatus(Enum):
    """Enums"""

    DRAFT = "DRAFT"
    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    SUSPENDED = "SUSPENDED"


@dataclass
class ListingSummary:
    name: str

    listing_type: ListingType

    categories: Optional[List[Category]] = None

    created_at: Optional[int] = None

    created_by: Optional[str] = None

    created_by_id: Optional[int] = None

    exchange_ids: Optional[List[str]] = None

    git_repo: Optional[RepoInfo] = None
    """if a git repo is being created, a listing will be initialized with this field as opposed to a
    share"""

    provider_id: Optional[str] = None

    provider_region: Optional[RegionInfo] = None

    published_at: Optional[int] = None

    published_by: Optional[str] = None

    setting: Optional[ListingSetting] = None

    share: Optional[ShareInfo] = None

    status: Optional[ListingStatus] = None

    subtitle: Optional[str] = None

    updated_at: Optional[int] = None

    updated_by: Optional[str] = None

    updated_by_id: Optional[int] = None

    def as_dict(self) -> dict:
        """Serializes the ListingSummary into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.categories:
            body["categories"] = [v.value for v in self.categories]
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.created_by_id is not None:
            body["created_by_id"] = self.created_by_id
        if self.exchange_ids:
            body["exchange_ids"] = [v for v in self.exchange_ids]
        if self.git_repo:
            body["git_repo"] = self.git_repo.as_dict()
        if self.listing_type is not None:
            body["listingType"] = self.listing_type.value
        if self.name is not None:
            body["name"] = self.name
        if self.provider_id is not None:
            body["provider_id"] = self.provider_id
        if self.provider_region:
            body["provider_region"] = self.provider_region.as_dict()
        if self.published_at is not None:
            body["published_at"] = self.published_at
        if self.published_by is not None:
            body["published_by"] = self.published_by
        if self.setting:
            body["setting"] = self.setting.as_dict()
        if self.share:
            body["share"] = self.share.as_dict()
        if self.status is not None:
            body["status"] = self.status.value
        if self.subtitle is not None:
            body["subtitle"] = self.subtitle
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.updated_by_id is not None:
            body["updated_by_id"] = self.updated_by_id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListingSummary into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.categories:
            body["categories"] = self.categories
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.created_by_id is not None:
            body["created_by_id"] = self.created_by_id
        if self.exchange_ids:
            body["exchange_ids"] = self.exchange_ids
        if self.git_repo:
            body["git_repo"] = self.git_repo
        if self.listing_type is not None:
            body["listingType"] = self.listing_type
        if self.name is not None:
            body["name"] = self.name
        if self.provider_id is not None:
            body["provider_id"] = self.provider_id
        if self.provider_region:
            body["provider_region"] = self.provider_region
        if self.published_at is not None:
            body["published_at"] = self.published_at
        if self.published_by is not None:
            body["published_by"] = self.published_by
        if self.setting:
            body["setting"] = self.setting
        if self.share:
            body["share"] = self.share
        if self.status is not None:
            body["status"] = self.status
        if self.subtitle is not None:
            body["subtitle"] = self.subtitle
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        if self.updated_by_id is not None:
            body["updated_by_id"] = self.updated_by_id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListingSummary:
        """Deserializes the ListingSummary from a dictionary."""
        return cls(
            categories=_repeated_enum(d, "categories", Category),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            created_by_id=d.get("created_by_id", None),
            exchange_ids=d.get("exchange_ids", None),
            git_repo=_from_dict(d, "git_repo", RepoInfo),
            listing_type=_enum(d, "listingType", ListingType),
            name=d.get("name", None),
            provider_id=d.get("provider_id", None),
            provider_region=_from_dict(d, "provider_region", RegionInfo),
            published_at=d.get("published_at", None),
            published_by=d.get("published_by", None),
            setting=_from_dict(d, "setting", ListingSetting),
            share=_from_dict(d, "share", ShareInfo),
            status=_enum(d, "status", ListingStatus),
            subtitle=d.get("subtitle", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
            updated_by_id=d.get("updated_by_id", None),
        )


@dataclass
class ListingTag:
    tag_name: Optional[ListingTagType] = None
    """Tag name (enum)"""

    tag_values: Optional[List[str]] = None
    """String representation of the tag value. Values should be string literals (no complex types)"""

    def as_dict(self) -> dict:
        """Serializes the ListingTag into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.tag_name is not None:
            body["tag_name"] = self.tag_name.value
        if self.tag_values:
            body["tag_values"] = [v for v in self.tag_values]
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ListingTag into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.tag_name is not None:
            body["tag_name"] = self.tag_name
        if self.tag_values:
            body["tag_values"] = self.tag_values
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ListingTag:
        """Deserializes the ListingTag from a dictionary."""
        return cls(tag_name=_enum(d, "tag_name", ListingTagType), tag_values=d.get("tag_values", None))


class ListingTagType(Enum):

    LISTING_TAG_TYPE_LANGUAGE = "LISTING_TAG_TYPE_LANGUAGE"
    LISTING_TAG_TYPE_TASK = "LISTING_TAG_TYPE_TASK"


class ListingType(Enum):

    PERSONALIZED = "PERSONALIZED"
    STANDARD = "STANDARD"


class MarketplaceFileType(Enum):

    APP = "APP"
    EMBEDDED_NOTEBOOK = "EMBEDDED_NOTEBOOK"
    PROVIDER_ICON = "PROVIDER_ICON"


@dataclass
class PersonalizationRequest:
    consumer_region: RegionInfo

    comment: Optional[str] = None

    contact_info: Optional[ContactInfo] = None

    created_at: Optional[int] = None

    id: Optional[str] = None

    intended_use: Optional[str] = None

    is_from_lighthouse: Optional[bool] = None

    listing_id: Optional[str] = None

    listing_name: Optional[str] = None

    metastore_id: Optional[str] = None

    provider_id: Optional[str] = None

    recipient_type: Optional[DeltaSharingRecipientType] = None

    share: Optional[ShareInfo] = None
    """Share information is required for data listings but should be empty/ignored for non-data
    listings (MCP and App)."""

    status: Optional[PersonalizationRequestStatus] = None

    status_message: Optional[str] = None

    updated_at: Optional[int] = None

    def as_dict(self) -> dict:
        """Serializes the PersonalizationRequest into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.consumer_region:
            body["consumer_region"] = self.consumer_region.as_dict()
        if self.contact_info:
            body["contact_info"] = self.contact_info.as_dict()
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.id is not None:
            body["id"] = self.id
        if self.intended_use is not None:
            body["intended_use"] = self.intended_use
        if self.is_from_lighthouse is not None:
            body["is_from_lighthouse"] = self.is_from_lighthouse
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.listing_name is not None:
            body["listing_name"] = self.listing_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.provider_id is not None:
            body["provider_id"] = self.provider_id
        if self.recipient_type is not None:
            body["recipient_type"] = self.recipient_type.value
        if self.share:
            body["share"] = self.share.as_dict()
        if self.status is not None:
            body["status"] = self.status.value
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the PersonalizationRequest into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.comment is not None:
            body["comment"] = self.comment
        if self.consumer_region:
            body["consumer_region"] = self.consumer_region
        if self.contact_info:
            body["contact_info"] = self.contact_info
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.id is not None:
            body["id"] = self.id
        if self.intended_use is not None:
            body["intended_use"] = self.intended_use
        if self.is_from_lighthouse is not None:
            body["is_from_lighthouse"] = self.is_from_lighthouse
        if self.listing_id is not None:
            body["listing_id"] = self.listing_id
        if self.listing_name is not None:
            body["listing_name"] = self.listing_name
        if self.metastore_id is not None:
            body["metastore_id"] = self.metastore_id
        if self.provider_id is not None:
            body["provider_id"] = self.provider_id
        if self.recipient_type is not None:
            body["recipient_type"] = self.recipient_type
        if self.share:
            body["share"] = self.share
        if self.status is not None:
            body["status"] = self.status
        if self.status_message is not None:
            body["status_message"] = self.status_message
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> PersonalizationRequest:
        """Deserializes the PersonalizationRequest from a dictionary."""
        return cls(
            comment=d.get("comment", None),
            consumer_region=_from_dict(d, "consumer_region", RegionInfo),
            contact_info=_from_dict(d, "contact_info", ContactInfo),
            created_at=d.get("created_at", None),
            id=d.get("id", None),
            intended_use=d.get("intended_use", None),
            is_from_lighthouse=d.get("is_from_lighthouse", None),
            listing_id=d.get("listing_id", None),
            listing_name=d.get("listing_name", None),
            metastore_id=d.get("metastore_id", None),
            provider_id=d.get("provider_id", None),
            recipient_type=_enum(d, "recipient_type", DeltaSharingRecipientType),
            share=_from_dict(d, "share", ShareInfo),
            status=_enum(d, "status", PersonalizationRequestStatus),
            status_message=d.get("status_message", None),
            updated_at=d.get("updated_at", None),
        )


class PersonalizationRequestStatus(Enum):

    DENIED = "DENIED"
    FULFILLED = "FULFILLED"
    NEW = "NEW"
    REQUEST_PENDING = "REQUEST_PENDING"


@dataclass
class ProviderAnalyticsDashboard:
    id: str

    def as_dict(self) -> dict:
        """Serializes the ProviderAnalyticsDashboard into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProviderAnalyticsDashboard into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.id is not None:
            body["id"] = self.id
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProviderAnalyticsDashboard:
        """Deserializes the ProviderAnalyticsDashboard from a dictionary."""
        return cls(id=d.get("id", None))


@dataclass
class ProviderInfo:
    name: str

    business_contact_email: str

    term_of_service_link: str

    privacy_policy_link: str

    company_website_link: Optional[str] = None

    dark_mode_icon_file_id: Optional[str] = None

    dark_mode_icon_file_path: Optional[str] = None

    description: Optional[str] = None

    icon_file_id: Optional[str] = None

    icon_file_path: Optional[str] = None

    id: Optional[str] = None

    is_featured: Optional[bool] = None
    """is_featured is accessible by consumers only"""

    published_by: Optional[str] = None
    """published_by is only applicable to data aggregators (e.g. Crux)"""

    support_contact_email: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the ProviderInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.business_contact_email is not None:
            body["business_contact_email"] = self.business_contact_email
        if self.company_website_link is not None:
            body["company_website_link"] = self.company_website_link
        if self.dark_mode_icon_file_id is not None:
            body["dark_mode_icon_file_id"] = self.dark_mode_icon_file_id
        if self.dark_mode_icon_file_path is not None:
            body["dark_mode_icon_file_path"] = self.dark_mode_icon_file_path
        if self.description is not None:
            body["description"] = self.description
        if self.icon_file_id is not None:
            body["icon_file_id"] = self.icon_file_id
        if self.icon_file_path is not None:
            body["icon_file_path"] = self.icon_file_path
        if self.id is not None:
            body["id"] = self.id
        if self.is_featured is not None:
            body["is_featured"] = self.is_featured
        if self.name is not None:
            body["name"] = self.name
        if self.privacy_policy_link is not None:
            body["privacy_policy_link"] = self.privacy_policy_link
        if self.published_by is not None:
            body["published_by"] = self.published_by
        if self.support_contact_email is not None:
            body["support_contact_email"] = self.support_contact_email
        if self.term_of_service_link is not None:
            body["term_of_service_link"] = self.term_of_service_link
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ProviderInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.business_contact_email is not None:
            body["business_contact_email"] = self.business_contact_email
        if self.company_website_link is not None:
            body["company_website_link"] = self.company_website_link
        if self.dark_mode_icon_file_id is not None:
            body["dark_mode_icon_file_id"] = self.dark_mode_icon_file_id
        if self.dark_mode_icon_file_path is not None:
            body["dark_mode_icon_file_path"] = self.dark_mode_icon_file_path
        if self.description is not None:
            body["description"] = self.description
        if self.icon_file_id is not None:
            body["icon_file_id"] = self.icon_file_id
        if self.icon_file_path is not None:
            body["icon_file_path"] = self.icon_file_path
        if self.id is not None:
            body["id"] = self.id
        if self.is_featured is not None:
            body["is_featured"] = self.is_featured
        if self.name is not None:
            body["name"] = self.name
        if self.privacy_policy_link is not None:
            body["privacy_policy_link"] = self.privacy_policy_link
        if self.published_by is not None:
            body["published_by"] = self.published_by
        if self.support_contact_email is not None:
            body["support_contact_email"] = self.support_contact_email
        if self.term_of_service_link is not None:
            body["term_of_service_link"] = self.term_of_service_link
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProviderInfo:
        """Deserializes the ProviderInfo from a dictionary."""
        return cls(
            business_contact_email=d.get("business_contact_email", None),
            company_website_link=d.get("company_website_link", None),
            dark_mode_icon_file_id=d.get("dark_mode_icon_file_id", None),
            dark_mode_icon_file_path=d.get("dark_mode_icon_file_path", None),
            description=d.get("description", None),
            icon_file_id=d.get("icon_file_id", None),
            icon_file_path=d.get("icon_file_path", None),
            id=d.get("id", None),
            is_featured=d.get("is_featured", None),
            name=d.get("name", None),
            privacy_policy_link=d.get("privacy_policy_link", None),
            published_by=d.get("published_by", None),
            support_contact_email=d.get("support_contact_email", None),
            term_of_service_link=d.get("term_of_service_link", None),
        )


@dataclass
class RegionInfo:
    cloud: Optional[str] = None

    region: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the RegionInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.region is not None:
            body["region"] = self.region
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RegionInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.cloud is not None:
            body["cloud"] = self.cloud
        if self.region is not None:
            body["region"] = self.region
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegionInfo:
        """Deserializes the RegionInfo from a dictionary."""
        return cls(cloud=d.get("cloud", None), region=d.get("region", None))


@dataclass
class RemoveExchangeForListingResponse:
    def as_dict(self) -> dict:
        """Serializes the RemoveExchangeForListingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RemoveExchangeForListingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RemoveExchangeForListingResponse:
        """Deserializes the RemoveExchangeForListingResponse from a dictionary."""
        return cls()


@dataclass
class RepoInfo:
    git_repo_url: str
    """the git repo url e.g. https://github.com/databrickslabs/dolly.git"""

    def as_dict(self) -> dict:
        """Serializes the RepoInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.git_repo_url is not None:
            body["git_repo_url"] = self.git_repo_url
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.git_repo_url is not None:
            body["git_repo_url"] = self.git_repo_url
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoInfo:
        """Deserializes the RepoInfo from a dictionary."""
        return cls(git_repo_url=d.get("git_repo_url", None))


@dataclass
class RepoInstallation:
    repo_name: str
    """the user-specified repo name for their installed git repo listing"""

    repo_path: str
    """refers to the full url file path that navigates the user to the repo's entrypoint (e.g. a
    README.md file, or the repo file view in the unified UI) should just be a relative path"""

    def as_dict(self) -> dict:
        """Serializes the RepoInstallation into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.repo_name is not None:
            body["repo_name"] = self.repo_name
        if self.repo_path is not None:
            body["repo_path"] = self.repo_path
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the RepoInstallation into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.repo_name is not None:
            body["repo_name"] = self.repo_name
        if self.repo_path is not None:
            body["repo_path"] = self.repo_path
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RepoInstallation:
        """Deserializes the RepoInstallation from a dictionary."""
        return cls(repo_name=d.get("repo_name", None), repo_path=d.get("repo_path", None))


@dataclass
class SearchListingsResponse:
    listings: Optional[List[Listing]] = None

    next_page_token: Optional[str] = None

    def as_dict(self) -> dict:
        """Serializes the SearchListingsResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.listings:
            body["listings"] = [v.as_dict() for v in self.listings]
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SearchListingsResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.listings:
            body["listings"] = self.listings
        if self.next_page_token is not None:
            body["next_page_token"] = self.next_page_token
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SearchListingsResponse:
        """Deserializes the SearchListingsResponse from a dictionary."""
        return cls(listings=_repeated_dict(d, "listings", Listing), next_page_token=d.get("next_page_token", None))


@dataclass
class ShareInfo:
    name: str

    type: ListingShareType

    def as_dict(self) -> dict:
        """Serializes the ShareInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type.value
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the ShareInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.name is not None:
            body["name"] = self.name
        if self.type is not None:
            body["type"] = self.type
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ShareInfo:
        """Deserializes the ShareInfo from a dictionary."""
        return cls(name=d.get("name", None), type=_enum(d, "type", ListingShareType))


@dataclass
class SharedDataObject:
    data_object_type: Optional[str] = None
    """The type of the data object. Could be one of: TABLE, SCHEMA, NOTEBOOK_FILE, MODEL, VOLUME"""

    name: Optional[str] = None
    """Name of the shared object"""

    def as_dict(self) -> dict:
        """Serializes the SharedDataObject into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.data_object_type is not None:
            body["data_object_type"] = self.data_object_type
        if self.name is not None:
            body["name"] = self.name
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the SharedDataObject into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.data_object_type is not None:
            body["data_object_type"] = self.data_object_type
        if self.name is not None:
            body["name"] = self.name
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SharedDataObject:
        """Deserializes the SharedDataObject from a dictionary."""
        return cls(data_object_type=d.get("data_object_type", None), name=d.get("name", None))


@dataclass
class TokenDetail:
    bearer_token: Optional[str] = None

    endpoint: Optional[str] = None

    expiration_time: Optional[str] = None

    share_credentials_version: Optional[int] = None
    """These field names must follow the delta sharing protocol. Original message:
    RetrieveToken.Response in managed-catalog/api/messages/recipient.proto"""

    def as_dict(self) -> dict:
        """Serializes the TokenDetail into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.bearer_token is not None:
            body["bearerToken"] = self.bearer_token
        if self.endpoint is not None:
            body["endpoint"] = self.endpoint
        if self.expiration_time is not None:
            body["expirationTime"] = self.expiration_time
        if self.share_credentials_version is not None:
            body["shareCredentialsVersion"] = self.share_credentials_version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TokenDetail into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.bearer_token is not None:
            body["bearerToken"] = self.bearer_token
        if self.endpoint is not None:
            body["endpoint"] = self.endpoint
        if self.expiration_time is not None:
            body["expirationTime"] = self.expiration_time
        if self.share_credentials_version is not None:
            body["shareCredentialsVersion"] = self.share_credentials_version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TokenDetail:
        """Deserializes the TokenDetail from a dictionary."""
        return cls(
            bearer_token=d.get("bearerToken", None),
            endpoint=d.get("endpoint", None),
            expiration_time=d.get("expirationTime", None),
            share_credentials_version=d.get("shareCredentialsVersion", None),
        )


@dataclass
class TokenInfo:
    activation_url: Optional[str] = None
    """Full activation url to retrieve the access token. It will be empty if the token is already
    retrieved."""

    created_at: Optional[int] = None
    """Time at which this Recipient Token was created, in epoch milliseconds."""

    created_by: Optional[str] = None
    """Username of Recipient Token creator."""

    expiration_time: Optional[int] = None
    """Expiration timestamp of the token in epoch milliseconds."""

    id: Optional[str] = None
    """Unique id of the Recipient Token."""

    updated_at: Optional[int] = None
    """Time at which this Recipient Token was updated, in epoch milliseconds."""

    updated_by: Optional[str] = None
    """Username of Recipient Token updater."""

    def as_dict(self) -> dict:
        """Serializes the TokenInfo into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.activation_url is not None:
            body["activation_url"] = self.activation_url
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.id is not None:
            body["id"] = self.id
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the TokenInfo into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.activation_url is not None:
            body["activation_url"] = self.activation_url
        if self.created_at is not None:
            body["created_at"] = self.created_at
        if self.created_by is not None:
            body["created_by"] = self.created_by
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        if self.id is not None:
            body["id"] = self.id
        if self.updated_at is not None:
            body["updated_at"] = self.updated_at
        if self.updated_by is not None:
            body["updated_by"] = self.updated_by
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TokenInfo:
        """Deserializes the TokenInfo from a dictionary."""
        return cls(
            activation_url=d.get("activation_url", None),
            created_at=d.get("created_at", None),
            created_by=d.get("created_by", None),
            expiration_time=d.get("expiration_time", None),
            id=d.get("id", None),
            updated_at=d.get("updated_at", None),
            updated_by=d.get("updated_by", None),
        )


@dataclass
class UpdateExchangeFilterResponse:
    filter: Optional[ExchangeFilter] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateExchangeFilterResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.filter:
            body["filter"] = self.filter.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateExchangeFilterResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.filter:
            body["filter"] = self.filter
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateExchangeFilterResponse:
        """Deserializes the UpdateExchangeFilterResponse from a dictionary."""
        return cls(filter=_from_dict(d, "filter", ExchangeFilter))


@dataclass
class UpdateExchangeResponse:
    exchange: Optional[Exchange] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateExchangeResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.exchange:
            body["exchange"] = self.exchange.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateExchangeResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.exchange:
            body["exchange"] = self.exchange
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateExchangeResponse:
        """Deserializes the UpdateExchangeResponse from a dictionary."""
        return cls(exchange=_from_dict(d, "exchange", Exchange))


@dataclass
class UpdateInstallationResponse:
    installation: Optional[InstallationDetail] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateInstallationResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.installation:
            body["installation"] = self.installation.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateInstallationResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.installation:
            body["installation"] = self.installation
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateInstallationResponse:
        """Deserializes the UpdateInstallationResponse from a dictionary."""
        return cls(installation=_from_dict(d, "installation", InstallationDetail))


@dataclass
class UpdateListingResponse:
    listing: Optional[Listing] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateListingResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.listing:
            body["listing"] = self.listing.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateListingResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.listing:
            body["listing"] = self.listing
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateListingResponse:
        """Deserializes the UpdateListingResponse from a dictionary."""
        return cls(listing=_from_dict(d, "listing", Listing))


@dataclass
class UpdatePersonalizationRequestResponse:
    request: Optional[PersonalizationRequest] = None

    def as_dict(self) -> dict:
        """Serializes the UpdatePersonalizationRequestResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.request:
            body["request"] = self.request.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdatePersonalizationRequestResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.request:
            body["request"] = self.request
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdatePersonalizationRequestResponse:
        """Deserializes the UpdatePersonalizationRequestResponse from a dictionary."""
        return cls(request=_from_dict(d, "request", PersonalizationRequest))


@dataclass
class UpdateProviderAnalyticsDashboardResponse:
    id: str
    """id & version should be the same as the request"""

    dashboard_id: str
    """this is newly created Lakeview dashboard for the user"""

    version: Optional[int] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateProviderAnalyticsDashboardResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.id is not None:
            body["id"] = self.id
        if self.version is not None:
            body["version"] = self.version
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateProviderAnalyticsDashboardResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.dashboard_id is not None:
            body["dashboard_id"] = self.dashboard_id
        if self.id is not None:
            body["id"] = self.id
        if self.version is not None:
            body["version"] = self.version
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateProviderAnalyticsDashboardResponse:
        """Deserializes the UpdateProviderAnalyticsDashboardResponse from a dictionary."""
        return cls(dashboard_id=d.get("dashboard_id", None), id=d.get("id", None), version=d.get("version", None))


@dataclass
class UpdateProviderResponse:
    provider: Optional[ProviderInfo] = None

    def as_dict(self) -> dict:
        """Serializes the UpdateProviderResponse into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.provider:
            body["provider"] = self.provider.as_dict()
        return body

    def as_shallow_dict(self) -> dict:
        """Serializes the UpdateProviderResponse into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.provider:
            body["provider"] = self.provider
        return body

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> UpdateProviderResponse:
        """Deserializes the UpdateProviderResponse from a dictionary."""
        return cls(provider=_from_dict(d, "provider", ProviderInfo))


class Visibility(Enum):

    PRIVATE = "PRIVATE"
    PUBLIC = "PUBLIC"


class ConsumerFulfillmentsAPI:
    """Fulfillments are entities that allow consumers to preview installations."""

    def __init__(self, api_client):
        self._api = api_client

    def get(
        self, listing_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[SharedDataObject]:
        """Get a high level preview of the metadata of listing installable content.

        :param listing_id: str
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`SharedDataObject`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", f"/api/2.1/marketplace-consumer/listings/{listing_id}/content", query=query, headers=headers
            )
            if "shared_data_objects" in json:
                for v in json["shared_data_objects"]:
                    yield SharedDataObject.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list(
        self, listing_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ListingFulfillment]:
        """Get all listings fulfillments associated with a listing. A _fulfillment_ is a potential installation.
        Standard installations contain metadata about the attached share or git repo. Only one of these fields
        will be present. Personalized installations contain metadata about the attached share or git repo, as
        well as the Delta Sharing recipient type.

        :param listing_id: str
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ListingFulfillment`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", f"/api/2.1/marketplace-consumer/listings/{listing_id}/fulfillments", query=query, headers=headers
            )
            if "fulfillments" in json:
                for v in json["fulfillments"]:
                    yield ListingFulfillment.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class ConsumerInstallationsAPI:
    """Installations are entities that allow consumers to interact with Databricks Marketplace listings."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        listing_id: str,
        *,
        accepted_consumer_terms: Optional[ConsumerTerms] = None,
        catalog_name: Optional[str] = None,
        recipient_type: Optional[DeltaSharingRecipientType] = None,
        repo_detail: Optional[RepoInstallation] = None,
        share_name: Optional[str] = None,
    ) -> Installation:
        """Install payload associated with a Databricks Marketplace listing.

        :param listing_id: str
        :param accepted_consumer_terms: :class:`ConsumerTerms` (optional)
        :param catalog_name: str (optional)
        :param recipient_type: :class:`DeltaSharingRecipientType` (optional)
        :param repo_detail: :class:`RepoInstallation` (optional)
          for git repo installations
        :param share_name: str (optional)

        :returns: :class:`Installation`
        """

        body = {}
        if accepted_consumer_terms is not None:
            body["accepted_consumer_terms"] = accepted_consumer_terms.as_dict()
        if catalog_name is not None:
            body["catalog_name"] = catalog_name
        if recipient_type is not None:
            body["recipient_type"] = recipient_type.value
        if repo_detail is not None:
            body["repo_detail"] = repo_detail.as_dict()
        if share_name is not None:
            body["share_name"] = share_name
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST", f"/api/2.1/marketplace-consumer/listings/{listing_id}/installations", body=body, headers=headers
        )
        return Installation.from_dict(res)

    def delete(self, listing_id: str, installation_id: str):
        """Uninstall an installation associated with a Databricks Marketplace listing.

        :param listing_id: str
        :param installation_id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do(
            "DELETE",
            f"/api/2.1/marketplace-consumer/listings/{listing_id}/installations/{installation_id}",
            headers=headers,
        )

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[InstallationDetail]:
        """List all installations across all listings.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`InstallationDetail`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/marketplace-consumer/installations", query=query, headers=headers)
            if "installations" in json:
                for v in json["installations"]:
                    yield InstallationDetail.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_listing_installations(
        self, listing_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[InstallationDetail]:
        """List all installations for a particular listing.

        :param listing_id: str
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`InstallationDetail`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET",
                f"/api/2.1/marketplace-consumer/listings/{listing_id}/installations",
                query=query,
                headers=headers,
            )
            if "installations" in json:
                for v in json["installations"]:
                    yield InstallationDetail.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        listing_id: str,
        installation_id: str,
        installation: InstallationDetail,
        *,
        rotate_token: Optional[bool] = None,
    ) -> UpdateInstallationResponse:
        """This is a update API that will update the part of the fields defined in the installation table as well
        as interact with external services according to the fields not included in the installation table 1.
        the token will be rotate if the rotateToken flag is true 2. the token will be forcibly rotate if the
        rotateToken flag is true and the tokenInfo field is empty

        :param listing_id: str
        :param installation_id: str
        :param installation: :class:`InstallationDetail`
        :param rotate_token: bool (optional)

        :returns: :class:`UpdateInstallationResponse`
        """

        body = {}
        if installation is not None:
            body["installation"] = installation.as_dict()
        if rotate_token is not None:
            body["rotate_token"] = rotate_token
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PUT",
            f"/api/2.1/marketplace-consumer/listings/{listing_id}/installations/{installation_id}",
            body=body,
            headers=headers,
        )
        return UpdateInstallationResponse.from_dict(res)


class ConsumerListingsAPI:
    """Listings are the core entities in the Marketplace. They represent the products that are available for
    consumption."""

    def __init__(self, api_client):
        self._api = api_client

    def batch_get(self, *, ids: Optional[List[str]] = None) -> BatchGetListingsResponse:
        """Batch get a published listing in the Databricks Marketplace that the consumer has access to.

        :param ids: List[str] (optional)

        :returns: :class:`BatchGetListingsResponse`
        """

        query = {}
        if ids is not None:
            query["ids"] = [v for v in ids]
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.1/marketplace-consumer/listings:batchGet", query=query, headers=headers)
        return BatchGetListingsResponse.from_dict(res)

    def get(self, id: str) -> GetListingResponse:
        """Get a published listing in the Databricks Marketplace that the consumer has access to.

        :param id: str

        :returns: :class:`GetListingResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/marketplace-consumer/listings/{id}", headers=headers)
        return GetListingResponse.from_dict(res)

    def list(
        self,
        *,
        assets: Optional[List[AssetType]] = None,
        categories: Optional[List[Category]] = None,
        is_free: Optional[bool] = None,
        is_private_exchange: Optional[bool] = None,
        is_staff_pick: Optional[bool] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        provider_ids: Optional[List[str]] = None,
        tags: Optional[List[ListingTag]] = None,
    ) -> Iterator[Listing]:
        """List all published listings in the Databricks Marketplace that the consumer has access to.

        :param assets: List[:class:`AssetType`] (optional)
          Matches any of the following asset types
        :param categories: List[:class:`Category`] (optional)
          Matches any of the following categories
        :param is_free: bool (optional)
          Filters each listing based on if it is free.
        :param is_private_exchange: bool (optional)
          Filters each listing based on if it is a private exchange.
        :param is_staff_pick: bool (optional)
          Filters each listing based on whether it is a staff pick.
        :param page_size: int (optional)
        :param page_token: str (optional)
        :param provider_ids: List[str] (optional)
          Matches any of the following provider ids
        :param tags: List[:class:`ListingTag`] (optional)
          Matches any of the following tags

        :returns: Iterator over :class:`Listing`
        """

        query = {}
        if assets is not None:
            query["assets"] = [v.value for v in assets]
        if categories is not None:
            query["categories"] = [v.value for v in categories]
        if is_free is not None:
            query["is_free"] = is_free
        if is_private_exchange is not None:
            query["is_private_exchange"] = is_private_exchange
        if is_staff_pick is not None:
            query["is_staff_pick"] = is_staff_pick
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        if provider_ids is not None:
            query["provider_ids"] = [v for v in provider_ids]
        if tags is not None:
            query["tags"] = [v.as_dict() for v in tags]
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/marketplace-consumer/listings", query=query, headers=headers)
            if "listings" in json:
                for v in json["listings"]:
                    yield Listing.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def search(
        self,
        query: str,
        *,
        assets: Optional[List[AssetType]] = None,
        categories: Optional[List[Category]] = None,
        is_free: Optional[bool] = None,
        is_private_exchange: Optional[bool] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        provider_ids: Optional[List[str]] = None,
    ) -> Iterator[Listing]:
        """Search published listings in the Databricks Marketplace that the consumer has access to. This query
        supports a variety of different search parameters and performs fuzzy matching.

        :param query: str
          Fuzzy matches query
        :param assets: List[:class:`AssetType`] (optional)
          Matches any of the following asset types
        :param categories: List[:class:`Category`] (optional)
          Matches any of the following categories
        :param is_free: bool (optional)
        :param is_private_exchange: bool (optional)
        :param page_size: int (optional)
        :param page_token: str (optional)
        :param provider_ids: List[str] (optional)
          Matches any of the following provider ids

        :returns: Iterator over :class:`Listing`
        """

        query = {}
        if assets is not None:
            query["assets"] = [v.value for v in assets]
        if categories is not None:
            query["categories"] = [v.value for v in categories]
        if is_free is not None:
            query["is_free"] = is_free
        if is_private_exchange is not None:
            query["is_private_exchange"] = is_private_exchange
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        if provider_ids is not None:
            query["provider_ids"] = [v for v in provider_ids]
        if query is not None:
            query["query"] = query
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/marketplace-consumer/search-listings", query=query, headers=headers)
            if "listings" in json:
                for v in json["listings"]:
                    yield Listing.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class ConsumerPersonalizationRequestsAPI:
    """Personalization Requests allow customers to interact with the individualized Marketplace listing flow."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        listing_id: str,
        intended_use: str,
        accepted_consumer_terms: ConsumerTerms,
        *,
        comment: Optional[str] = None,
        company: Optional[str] = None,
        first_name: Optional[str] = None,
        is_from_lighthouse: Optional[bool] = None,
        last_name: Optional[str] = None,
        recipient_type: Optional[DeltaSharingRecipientType] = None,
    ) -> CreatePersonalizationRequestResponse:
        """Create a personalization request for a listing.

        :param listing_id: str
        :param intended_use: str
        :param accepted_consumer_terms: :class:`ConsumerTerms`
        :param comment: str (optional)
        :param company: str (optional)
        :param first_name: str (optional)
        :param is_from_lighthouse: bool (optional)
        :param last_name: str (optional)
        :param recipient_type: :class:`DeltaSharingRecipientType` (optional)

        :returns: :class:`CreatePersonalizationRequestResponse`
        """

        body = {}
        if accepted_consumer_terms is not None:
            body["accepted_consumer_terms"] = accepted_consumer_terms.as_dict()
        if comment is not None:
            body["comment"] = comment
        if company is not None:
            body["company"] = company
        if first_name is not None:
            body["first_name"] = first_name
        if intended_use is not None:
            body["intended_use"] = intended_use
        if is_from_lighthouse is not None:
            body["is_from_lighthouse"] = is_from_lighthouse
        if last_name is not None:
            body["last_name"] = last_name
        if recipient_type is not None:
            body["recipient_type"] = recipient_type.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "POST",
            f"/api/2.1/marketplace-consumer/listings/{listing_id}/personalization-requests",
            body=body,
            headers=headers,
        )
        return CreatePersonalizationRequestResponse.from_dict(res)

    def get(self, listing_id: str) -> GetPersonalizationRequestResponse:
        """Get the personalization request for a listing. Each consumer can make at *most* one personalization
        request for a listing.

        :param listing_id: str

        :returns: :class:`GetPersonalizationRequestResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "GET", f"/api/2.1/marketplace-consumer/listings/{listing_id}/personalization-requests", headers=headers
        )
        return GetPersonalizationRequestResponse.from_dict(res)

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[PersonalizationRequest]:
        """List personalization requests for a consumer across all listings.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`PersonalizationRequest`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", "/api/2.1/marketplace-consumer/personalization-requests", query=query, headers=headers
            )
            if "personalization_requests" in json:
                for v in json["personalization_requests"]:
                    yield PersonalizationRequest.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class ConsumerProvidersAPI:
    """Providers are the entities that publish listings to the Marketplace."""

    def __init__(self, api_client):
        self._api = api_client

    def batch_get(self, *, ids: Optional[List[str]] = None) -> BatchGetProvidersResponse:
        """Batch get a provider in the Databricks Marketplace with at least one visible listing.

        :param ids: List[str] (optional)

        :returns: :class:`BatchGetProvidersResponse`
        """

        query = {}
        if ids is not None:
            query["ids"] = [v for v in ids]
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.1/marketplace-consumer/providers:batchGet", query=query, headers=headers)
        return BatchGetProvidersResponse.from_dict(res)

    def get(self, id: str) -> GetProviderResponse:
        """Get a provider in the Databricks Marketplace with at least one visible listing.

        :param id: str

        :returns: :class:`GetProviderResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.1/marketplace-consumer/providers/{id}", headers=headers)
        return GetProviderResponse.from_dict(res)

    def list(
        self, *, is_featured: Optional[bool] = None, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ProviderInfo]:
        """List all providers in the Databricks Marketplace with at least one visible listing.

        :param is_featured: bool (optional)
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ProviderInfo`
        """

        query = {}
        if is_featured is not None:
            query["is_featured"] = is_featured
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.1/marketplace-consumer/providers", query=query, headers=headers)
            if "providers" in json:
                for v in json["providers"]:
                    yield ProviderInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class ProviderExchangeFiltersAPI:
    """Marketplace exchanges filters curate which groups can access an exchange."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, filter: ExchangeFilter) -> CreateExchangeFilterResponse:
        """Add an exchange filter.

        :param filter: :class:`ExchangeFilter`

        :returns: :class:`CreateExchangeFilterResponse`
        """

        body = {}
        if filter is not None:
            body["filter"] = filter.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/marketplace-exchange/filters", body=body, headers=headers)
        return CreateExchangeFilterResponse.from_dict(res)

    def delete(self, id: str):
        """Delete an exchange filter

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/marketplace-exchange/filters/{id}", headers=headers)

    def list(
        self, exchange_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ExchangeFilter]:
        """List exchange filter

        :param exchange_id: str
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ExchangeFilter`
        """

        query = {}
        if exchange_id is not None:
            query["exchange_id"] = exchange_id
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/marketplace-exchange/filters", query=query, headers=headers)
            if "filters" in json:
                for v in json["filters"]:
                    yield ExchangeFilter.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(self, id: str, filter: ExchangeFilter) -> UpdateExchangeFilterResponse:
        """Update an exchange filter.

        :param id: str
        :param filter: :class:`ExchangeFilter`

        :returns: :class:`UpdateExchangeFilterResponse`
        """

        body = {}
        if filter is not None:
            body["filter"] = filter.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/marketplace-exchange/filters/{id}", body=body, headers=headers)
        return UpdateExchangeFilterResponse.from_dict(res)


class ProviderExchangesAPI:
    """Marketplace exchanges allow providers to share their listings with a curated set of customers."""

    def __init__(self, api_client):
        self._api = api_client

    def add_listing_to_exchange(self, listing_id: str, exchange_id: str) -> AddExchangeForListingResponse:
        """Associate an exchange with a listing

        :param listing_id: str
        :param exchange_id: str

        :returns: :class:`AddExchangeForListingResponse`
        """

        body = {}
        if exchange_id is not None:
            body["exchange_id"] = exchange_id
        if listing_id is not None:
            body["listing_id"] = listing_id
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/marketplace-exchange/exchanges-for-listing", body=body, headers=headers)
        return AddExchangeForListingResponse.from_dict(res)

    def create(self, exchange: Exchange) -> CreateExchangeResponse:
        """Create an exchange

        :param exchange: :class:`Exchange`

        :returns: :class:`CreateExchangeResponse`
        """

        body = {}
        if exchange is not None:
            body["exchange"] = exchange.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/marketplace-exchange/exchanges", body=body, headers=headers)
        return CreateExchangeResponse.from_dict(res)

    def delete(self, id: str):
        """This removes a listing from marketplace.

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/marketplace-exchange/exchanges/{id}", headers=headers)

    def delete_listing_from_exchange(self, id: str):
        """Disassociate an exchange with a listing

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/marketplace-exchange/exchanges-for-listing/{id}", headers=headers)

    def get(self, id: str) -> GetExchangeResponse:
        """Get an exchange.

        :param id: str

        :returns: :class:`GetExchangeResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/marketplace-exchange/exchanges/{id}", headers=headers)
        return GetExchangeResponse.from_dict(res)

    def list(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[Exchange]:
        """List exchanges visible to provider

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`Exchange`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/marketplace-exchange/exchanges", query=query, headers=headers)
            if "exchanges" in json:
                for v in json["exchanges"]:
                    yield Exchange.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_exchanges_for_listing(
        self, listing_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ExchangeListing]:
        """List exchanges associated with a listing

        :param listing_id: str
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ExchangeListing`
        """

        query = {}
        if listing_id is not None:
            query["listing_id"] = listing_id
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", "/api/2.0/marketplace-exchange/exchanges-for-listing", query=query, headers=headers
            )
            if "exchange_listing" in json:
                for v in json["exchange_listing"]:
                    yield ExchangeListing.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def list_listings_for_exchange(
        self, exchange_id: str, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[ExchangeListing]:
        """List listings associated with an exchange

        :param exchange_id: str
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ExchangeListing`
        """

        query = {}
        if exchange_id is not None:
            query["exchange_id"] = exchange_id
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", "/api/2.0/marketplace-exchange/listings-for-exchange", query=query, headers=headers
            )
            if "exchange_listings" in json:
                for v in json["exchange_listings"]:
                    yield ExchangeListing.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(self, id: str, exchange: Exchange) -> UpdateExchangeResponse:
        """Update an exchange

        :param id: str
        :param exchange: :class:`Exchange`

        :returns: :class:`UpdateExchangeResponse`
        """

        body = {}
        if exchange is not None:
            body["exchange"] = exchange.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/marketplace-exchange/exchanges/{id}", body=body, headers=headers)
        return UpdateExchangeResponse.from_dict(res)


class ProviderFilesAPI:
    """Marketplace offers a set of file APIs for various purposes such as preview notebooks and provider icons."""

    def __init__(self, api_client):
        self._api = api_client

    def create(
        self,
        file_parent: FileParent,
        marketplace_file_type: MarketplaceFileType,
        mime_type: str,
        *,
        display_name: Optional[str] = None,
    ) -> CreateFileResponse:
        """Create a file. Currently, only provider icons and attached notebooks are supported.

        :param file_parent: :class:`FileParent`
        :param marketplace_file_type: :class:`MarketplaceFileType`
        :param mime_type: str
        :param display_name: str (optional)

        :returns: :class:`CreateFileResponse`
        """

        body = {}
        if display_name is not None:
            body["display_name"] = display_name
        if file_parent is not None:
            body["file_parent"] = file_parent.as_dict()
        if marketplace_file_type is not None:
            body["marketplace_file_type"] = marketplace_file_type.value
        if mime_type is not None:
            body["mime_type"] = mime_type
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/marketplace-provider/files", body=body, headers=headers)
        return CreateFileResponse.from_dict(res)

    def delete(self, file_id: str):
        """Delete a file

        :param file_id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/marketplace-provider/files/{file_id}", headers=headers)

    def get(self, file_id: str) -> GetFileResponse:
        """Get a file

        :param file_id: str

        :returns: :class:`GetFileResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/marketplace-provider/files/{file_id}", headers=headers)
        return GetFileResponse.from_dict(res)

    def list(
        self, file_parent: FileParent, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[FileInfo]:
        """List files attached to a parent entity.

        :param file_parent: :class:`FileParent`
        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`FileInfo`
        """

        query = {}
        if file_parent is not None:
            query["file_parent"] = file_parent.as_dict()
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/marketplace-provider/files", query=query, headers=headers)
            if "file_infos" in json:
                for v in json["file_infos"]:
                    yield FileInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]


class ProviderListingsAPI:
    """Listings are the core entities in the Marketplace. They represent the products that are available for
    consumption."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, listing: Listing) -> CreateListingResponse:
        """Create a new listing

        :param listing: :class:`Listing`

        :returns: :class:`CreateListingResponse`
        """

        body = {}
        if listing is not None:
            body["listing"] = listing.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/marketplace-provider/listing", body=body, headers=headers)
        return CreateListingResponse.from_dict(res)

    def delete(self, id: str):
        """Delete a listing

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/marketplace-provider/listings/{id}", headers=headers)

    def get(self, id: str) -> GetListingResponse:
        """Get a listing

        :param id: str

        :returns: :class:`GetListingResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/marketplace-provider/listings/{id}", headers=headers)
        return GetListingResponse.from_dict(res)

    def list(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[Listing]:
        """List listings owned by this provider

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`Listing`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/marketplace-provider/listings", query=query, headers=headers)
            if "listings" in json:
                for v in json["listings"]:
                    yield Listing.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(self, id: str, listing: Listing) -> UpdateListingResponse:
        """Update a listing

        :param id: str
        :param listing: :class:`Listing`

        :returns: :class:`UpdateListingResponse`
        """

        body = {}
        if listing is not None:
            body["listing"] = listing.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/marketplace-provider/listings/{id}", body=body, headers=headers)
        return UpdateListingResponse.from_dict(res)


class ProviderPersonalizationRequestsAPI:
    """Personalization requests are an alternate to instantly available listings. Control the lifecycle of
    personalized solutions."""

    def __init__(self, api_client):
        self._api = api_client

    def list(
        self, *, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> Iterator[PersonalizationRequest]:
        """List personalization requests to this provider. This will return all personalization requests,
        regardless of which listing they are for.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`PersonalizationRequest`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do(
                "GET", "/api/2.0/marketplace-provider/personalization-requests", query=query, headers=headers
            )
            if "personalization_requests" in json:
                for v in json["personalization_requests"]:
                    yield PersonalizationRequest.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(
        self,
        listing_id: str,
        request_id: str,
        status: PersonalizationRequestStatus,
        *,
        reason: Optional[str] = None,
        share: Optional[ShareInfo] = None,
    ) -> UpdatePersonalizationRequestResponse:
        """Update personalization request. This method only permits updating the status of the request.

        :param listing_id: str
        :param request_id: str
        :param status: :class:`PersonalizationRequestStatus`
        :param reason: str (optional)
        :param share: :class:`ShareInfo` (optional)

        :returns: :class:`UpdatePersonalizationRequestResponse`
        """

        body = {}
        if reason is not None:
            body["reason"] = reason
        if share is not None:
            body["share"] = share.as_dict()
        if status is not None:
            body["status"] = status.value
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do(
            "PUT",
            f"/api/2.0/marketplace-provider/listings/{listing_id}/personalization-requests/{request_id}/request-status",
            body=body,
            headers=headers,
        )
        return UpdatePersonalizationRequestResponse.from_dict(res)


class ProviderProviderAnalyticsDashboardsAPI:
    """Manage templated analytics solution for providers."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self) -> ProviderAnalyticsDashboard:
        """Create provider analytics dashboard. Returns Marketplace specific `id`. Not to be confused with the
        Lakeview dashboard id.


        :returns: :class:`ProviderAnalyticsDashboard`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/marketplace-provider/analytics_dashboard", headers=headers)
        return ProviderAnalyticsDashboard.from_dict(res)

    def get(self) -> ListProviderAnalyticsDashboardResponse:
        """Get provider analytics dashboard.


        :returns: :class:`ListProviderAnalyticsDashboardResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/marketplace-provider/analytics_dashboard", headers=headers)
        return ListProviderAnalyticsDashboardResponse.from_dict(res)

    def get_latest_version(self) -> GetLatestVersionProviderAnalyticsDashboardResponse:
        """Get latest version of provider analytics dashboard.


        :returns: :class:`GetLatestVersionProviderAnalyticsDashboardResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", "/api/2.0/marketplace-provider/analytics_dashboard/latest", headers=headers)
        return GetLatestVersionProviderAnalyticsDashboardResponse.from_dict(res)

    def update(self, id: str, *, version: Optional[int] = None) -> UpdateProviderAnalyticsDashboardResponse:
        """Update provider analytics dashboard.

        :param id: str
          id is immutable property and can't be updated.
        :param version: int (optional)
          this is the version of the dashboard template we want to update our user to current expectation is
          that it should be equal to latest version of the dashboard template

        :returns: :class:`UpdateProviderAnalyticsDashboardResponse`
        """

        body = {}
        if version is not None:
            body["version"] = version
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/marketplace-provider/analytics_dashboard/{id}", body=body, headers=headers)
        return UpdateProviderAnalyticsDashboardResponse.from_dict(res)


class ProviderProvidersAPI:
    """Providers are entities that manage assets in Marketplace."""

    def __init__(self, api_client):
        self._api = api_client

    def create(self, provider: ProviderInfo) -> CreateProviderResponse:
        """Create a provider

        :param provider: :class:`ProviderInfo`

        :returns: :class:`CreateProviderResponse`
        """

        body = {}
        if provider is not None:
            body["provider"] = provider.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("POST", "/api/2.0/marketplace-provider/provider", body=body, headers=headers)
        return CreateProviderResponse.from_dict(res)

    def delete(self, id: str):
        """Delete provider

        :param id: str


        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        self._api.do("DELETE", f"/api/2.0/marketplace-provider/providers/{id}", headers=headers)

    def get(self, id: str) -> GetProviderResponse:
        """Get provider profile

        :param id: str

        :returns: :class:`GetProviderResponse`
        """

        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("GET", f"/api/2.0/marketplace-provider/providers/{id}", headers=headers)
        return GetProviderResponse.from_dict(res)

    def list(self, *, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[ProviderInfo]:
        """List provider profiles for account.

        :param page_size: int (optional)
        :param page_token: str (optional)

        :returns: Iterator over :class:`ProviderInfo`
        """

        query = {}
        if page_size is not None:
            query["page_size"] = page_size
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        while True:
            json = self._api.do("GET", "/api/2.0/marketplace-provider/providers", query=query, headers=headers)
            if "providers" in json:
                for v in json["providers"]:
                    yield ProviderInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]

    def update(self, id: str, provider: ProviderInfo) -> UpdateProviderResponse:
        """Update provider profile

        :param id: str
        :param provider: :class:`ProviderInfo`

        :returns: :class:`UpdateProviderResponse`
        """

        body = {}
        if provider is not None:
            body["provider"] = provider.as_dict()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        cfg = self._api._cfg
        if cfg.host_type == HostType.UNIFIED and cfg.workspace_id:
            headers["X-Databricks-Org-Id"] = cfg.workspace_id

        res = self._api.do("PUT", f"/api/2.0/marketplace-provider/providers/{id}", body=body, headers=headers)
        return UpdateProviderResponse.from_dict(res)
