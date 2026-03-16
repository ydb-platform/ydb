from typing import Optional, List, Dict, Union
from dataclasses import dataclass, asdict


@dataclass
class ItemOffersRequest:
    """ Implements definition: https://developer-docs.amazon.com/sp-api/docs/product-pricing-api-v0-reference
    #itemoffersrequest """
    uri: str
    method: str
    MarketplaceId: str
    ItemCondition: str = None
    CustomerType: str = None
    headers: Dict = None


@dataclass
class GetItemOffersBatchRequest:
    """ Implements definition: https://developer-docs.amazon.com/sp-api/docs/product-pricing-api-v0-reference
    #getitemoffersbatchrequest """
    requests: Optional[List[Union[ItemOffersRequest, Dict]]] = None

    def __post_init__(self):
        self.requests = self.parse_requests(self.requests)

    def to_dict(self):
        return asdict(self)

    @staticmethod
    def parse_requests(requests) -> List[ItemOffersRequest]:
        parsed_requestes = []

        for request in requests:
            if isinstance(request, Dict):
                request = ItemOffersRequest(**request)

            if not isinstance(request, ItemOffersRequest):
                raise TypeError

            parsed_requestes.append(request)

        return parsed_requestes
    

@dataclass
class ListingOffersRequest:
    """ Implements definition: https://developer-docs.amazon.com/sp-api/docs/product-pricing-api-v0-reference
    #listingoffersrequest """
    uri: str
    MarketplaceId: str
    ItemCondition: str
    method: str = "GET"
    CustomerType: str = "Consumer"


@dataclass
class GetListingOffersBatchRequest:
    """ Implements definition: https://developer-docs.amazon.com/sp-api/docs/product-pricing-api-v0-reference
    #getlistingoffersbatchrequest """
    requests: Optional[List[Union[ListingOffersRequest, Dict]]] = None

    def __post_init__(self):
        self.requests = self.parse_requests(self.requests)

    def to_dict(self):
        return asdict(self)

    @staticmethod
    def parse_requests(requests) -> List[ListingOffersRequest]:
        parsed_requestes = []

        for request in requests:
            if isinstance(request, Dict):
                request = ListingOffersRequest(**request)

            if not isinstance(request, ListingOffersRequest):
                raise TypeError

            parsed_requestes.append(request)

        return parsed_requestes

