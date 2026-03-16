# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.treasury._financial_account_close_params import (
        FinancialAccountCloseParams,
    )
    from stripe.params.treasury._financial_account_create_params import (
        FinancialAccountCreateParams,
    )
    from stripe.params.treasury._financial_account_list_params import (
        FinancialAccountListParams,
    )
    from stripe.params.treasury._financial_account_modify_params import (
        FinancialAccountModifyParams,
    )
    from stripe.params.treasury._financial_account_retrieve_features_params import (
        FinancialAccountRetrieveFeaturesParams,
    )
    from stripe.params.treasury._financial_account_retrieve_params import (
        FinancialAccountRetrieveParams,
    )
    from stripe.params.treasury._financial_account_update_features_params import (
        FinancialAccountUpdateFeaturesParams,
    )
    from stripe.treasury._financial_account_features import (
        FinancialAccountFeatures,
    )


class FinancialAccount(
    CreateableAPIResource["FinancialAccount"],
    ListableAPIResource["FinancialAccount"],
    UpdateableAPIResource["FinancialAccount"],
):
    """
    Stripe Treasury provides users with a container for money called a FinancialAccount that is separate from their Payments balance.
    FinancialAccounts serve as the source and destination of Treasury's money movement APIs.
    """

    OBJECT_NAME: ClassVar[Literal["treasury.financial_account"]] = (
        "treasury.financial_account"
    )

    class Balance(StripeObject):
        cash: Dict[str, int]
        """
        Funds the user can spend right now.
        """
        inbound_pending: Dict[str, int]
        """
        Funds not spendable yet, but will become available at a later time.
        """
        outbound_pending: Dict[str, int]
        """
        Funds in the account, but not spendable because they are being held for pending outbound flows.
        """

    class FinancialAddress(StripeObject):
        class Aba(StripeObject):
            account_holder_name: str
            """
            The name of the person or business that owns the bank account.
            """
            account_number: Optional[str]
            """
            The account number.
            """
            account_number_last4: str
            """
            The last four characters of the account number.
            """
            bank_name: str
            """
            Name of the bank.
            """
            routing_number: str
            """
            Routing number for the account.
            """

        aba: Optional[Aba]
        """
        ABA Records contain U.S. bank account details per the ABA format.
        """
        supported_networks: Optional[List[Literal["ach", "us_domestic_wire"]]]
        """
        The list of networks that the address supports
        """
        type: Literal["aba"]
        """
        The type of financial address
        """
        _inner_class_types = {"aba": Aba}

    class PlatformRestrictions(StripeObject):
        inbound_flows: Optional[Literal["restricted", "unrestricted"]]
        """
        Restricts all inbound money movement.
        """
        outbound_flows: Optional[Literal["restricted", "unrestricted"]]
        """
        Restricts all outbound money movement.
        """

    class StatusDetails(StripeObject):
        class Closed(StripeObject):
            reasons: List[
                Literal["account_rejected", "closed_by_platform", "other"]
            ]
            """
            The array that contains reasons for a FinancialAccount closure.
            """

        closed: Optional[Closed]
        """
        Details related to the closure of this FinancialAccount
        """
        _inner_class_types = {"closed": Closed}

    active_features: Optional[
        List[
            Literal[
                "card_issuing",
                "deposit_insurance",
                "financial_addresses.aba",
                "financial_addresses.aba.forwarding",
                "inbound_transfers.ach",
                "intra_stripe_flows",
                "outbound_payments.ach",
                "outbound_payments.us_domestic_wire",
                "outbound_transfers.ach",
                "outbound_transfers.us_domestic_wire",
                "remote_deposit_capture",
            ]
        ]
    ]
    """
    The array of paths to active Features in the Features hash.
    """
    balance: Balance
    """
    Balance information for the FinancialAccount
    """
    country: str
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    features: Optional["FinancialAccountFeatures"]
    """
    Encodes whether a FinancialAccount has access to a particular Feature, with a `status` enum and associated `status_details`.
    Stripe or the platform can control Features via the requested field.
    """
    financial_addresses: List[FinancialAddress]
    """
    The set of credentials that resolve to a FinancialAccount.
    """
    id: str
    """
    Unique identifier for the object.
    """
    is_default: Optional[bool]
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    nickname: Optional[str]
    """
    The nickname for the FinancialAccount.
    """
    object: Literal["treasury.financial_account"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    pending_features: Optional[
        List[
            Literal[
                "card_issuing",
                "deposit_insurance",
                "financial_addresses.aba",
                "financial_addresses.aba.forwarding",
                "inbound_transfers.ach",
                "intra_stripe_flows",
                "outbound_payments.ach",
                "outbound_payments.us_domestic_wire",
                "outbound_transfers.ach",
                "outbound_transfers.us_domestic_wire",
                "remote_deposit_capture",
            ]
        ]
    ]
    """
    The array of paths to pending Features in the Features hash.
    """
    platform_restrictions: Optional[PlatformRestrictions]
    """
    The set of functionalities that the platform can restrict on the FinancialAccount.
    """
    restricted_features: Optional[
        List[
            Literal[
                "card_issuing",
                "deposit_insurance",
                "financial_addresses.aba",
                "financial_addresses.aba.forwarding",
                "inbound_transfers.ach",
                "intra_stripe_flows",
                "outbound_payments.ach",
                "outbound_payments.us_domestic_wire",
                "outbound_transfers.ach",
                "outbound_transfers.us_domestic_wire",
                "remote_deposit_capture",
            ]
        ]
    ]
    """
    The array of paths to restricted Features in the Features hash.
    """
    status: Literal["closed", "open"]
    """
    Status of this FinancialAccount.
    """
    status_details: StatusDetails
    supported_currencies: List[str]
    """
    The currencies the FinancialAccount can hold a balance in. Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase.
    """

    @classmethod
    def _cls_close(
        cls,
        financial_account: str,
        **params: Unpack["FinancialAccountCloseParams"],
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        return cast(
            "FinancialAccount",
            cls._static_request(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/close".format(
                    financial_account=sanitize_id(financial_account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def close(
        financial_account: str, **params: Unpack["FinancialAccountCloseParams"]
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        ...

    @overload
    def close(
        self, **params: Unpack["FinancialAccountCloseParams"]
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        ...

    @class_method_variant("_cls_close")
    def close(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["FinancialAccountCloseParams"]
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        return cast(
            "FinancialAccount",
            self._request(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/close".format(
                    financial_account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_close_async(
        cls,
        financial_account: str,
        **params: Unpack["FinancialAccountCloseParams"],
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        return cast(
            "FinancialAccount",
            await cls._static_request_async(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/close".format(
                    financial_account=sanitize_id(financial_account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def close_async(
        financial_account: str, **params: Unpack["FinancialAccountCloseParams"]
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        ...

    @overload
    async def close_async(
        self, **params: Unpack["FinancialAccountCloseParams"]
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        ...

    @class_method_variant("_cls_close_async")
    async def close_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["FinancialAccountCloseParams"]
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        return cast(
            "FinancialAccount",
            await self._request_async(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/close".format(
                    financial_account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["FinancialAccountCreateParams"]
    ) -> "FinancialAccount":
        """
        Creates a new FinancialAccount. Each connected account can have up to three FinancialAccounts by default.
        """
        return cast(
            "FinancialAccount",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["FinancialAccountCreateParams"]
    ) -> "FinancialAccount":
        """
        Creates a new FinancialAccount. Each connected account can have up to three FinancialAccounts by default.
        """
        return cast(
            "FinancialAccount",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["FinancialAccountListParams"]
    ) -> ListObject["FinancialAccount"]:
        """
        Returns a list of FinancialAccounts.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["FinancialAccountListParams"]
    ) -> ListObject["FinancialAccount"]:
        """
        Returns a list of FinancialAccounts.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["FinancialAccountModifyParams"]
    ) -> "FinancialAccount":
        """
        Updates the details of a FinancialAccount.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "FinancialAccount",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["FinancialAccountModifyParams"]
    ) -> "FinancialAccount":
        """
        Updates the details of a FinancialAccount.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "FinancialAccount",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["FinancialAccountRetrieveParams"]
    ) -> "FinancialAccount":
        """
        Retrieves the details of a FinancialAccount.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["FinancialAccountRetrieveParams"]
    ) -> "FinancialAccount":
        """
        Retrieves the details of a FinancialAccount.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_retrieve_features(
        cls,
        financial_account: str,
        **params: Unpack["FinancialAccountRetrieveFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            cls._static_request(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def retrieve_features(
        financial_account: str,
        **params: Unpack["FinancialAccountRetrieveFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        ...

    @overload
    def retrieve_features(
        self, **params: Unpack["FinancialAccountRetrieveFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        ...

    @class_method_variant("_cls_retrieve_features")
    def retrieve_features(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["FinancialAccountRetrieveFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            self._request(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_retrieve_features_async(
        cls,
        financial_account: str,
        **params: Unpack["FinancialAccountRetrieveFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            await cls._static_request_async(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def retrieve_features_async(
        financial_account: str,
        **params: Unpack["FinancialAccountRetrieveFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        ...

    @overload
    async def retrieve_features_async(
        self, **params: Unpack["FinancialAccountRetrieveFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        ...

    @class_method_variant("_cls_retrieve_features_async")
    async def retrieve_features_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["FinancialAccountRetrieveFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            await self._request_async(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_update_features(
        cls,
        financial_account: str,
        **params: Unpack["FinancialAccountUpdateFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            cls._static_request(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def update_features(
        financial_account: str,
        **params: Unpack["FinancialAccountUpdateFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        ...

    @overload
    def update_features(
        self, **params: Unpack["FinancialAccountUpdateFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        ...

    @class_method_variant("_cls_update_features")
    def update_features(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["FinancialAccountUpdateFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            self._request(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_update_features_async(
        cls,
        financial_account: str,
        **params: Unpack["FinancialAccountUpdateFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            await cls._static_request_async(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def update_features_async(
        financial_account: str,
        **params: Unpack["FinancialAccountUpdateFeaturesParams"],
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        ...

    @overload
    async def update_features_async(
        self, **params: Unpack["FinancialAccountUpdateFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        ...

    @class_method_variant("_cls_update_features_async")
    async def update_features_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["FinancialAccountUpdateFeaturesParams"]
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            await self._request_async(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "balance": Balance,
        "financial_addresses": FinancialAddress,
        "platform_restrictions": PlatformRestrictions,
        "status_details": StatusDetails,
    }
