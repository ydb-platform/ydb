# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction
    from stripe._file import File
    from stripe.issuing._transaction import Transaction
    from stripe.params.issuing._dispute_create_params import (
        DisputeCreateParams,
    )
    from stripe.params.issuing._dispute_list_params import DisputeListParams
    from stripe.params.issuing._dispute_modify_params import (
        DisputeModifyParams,
    )
    from stripe.params.issuing._dispute_retrieve_params import (
        DisputeRetrieveParams,
    )
    from stripe.params.issuing._dispute_submit_params import (
        DisputeSubmitParams,
    )


class Dispute(
    CreateableAPIResource["Dispute"],
    ListableAPIResource["Dispute"],
    UpdateableAPIResource["Dispute"],
):
    """
    As a [card issuer](https://docs.stripe.com/issuing), you can dispute transactions that the cardholder does not recognize, suspects to be fraudulent, or has other issues with.

    Related guide: [Issuing disputes](https://docs.stripe.com/issuing/purchases/disputes)
    """

    OBJECT_NAME: ClassVar[Literal["issuing.dispute"]] = "issuing.dispute"

    class Evidence(StripeObject):
        class Canceled(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            canceled_at: Optional[int]
            """
            Date when order was canceled.
            """
            cancellation_policy_provided: Optional[bool]
            """
            Whether the cardholder was provided with a cancellation policy.
            """
            cancellation_reason: Optional[str]
            """
            Reason for canceling the order.
            """
            expected_at: Optional[int]
            """
            Date when the cardholder expected to receive the product.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """
            product_description: Optional[str]
            """
            Description of the merchandise or service that was purchased.
            """
            product_type: Optional[Literal["merchandise", "service"]]
            """
            Whether the product was a merchandise or service.
            """
            return_status: Optional[Literal["merchant_rejected", "successful"]]
            """
            Result of cardholder's attempt to return the product.
            """
            returned_at: Optional[int]
            """
            Date when the product was returned or attempted to be returned.
            """

        class Duplicate(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            card_statement: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Copy of the card statement showing that the product had already been paid for.
            """
            cash_receipt: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Copy of the receipt showing that the product had been paid for in cash.
            """
            check_image: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Image of the front and back of the check that was used to pay for the product.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """
            original_transaction: Optional[str]
            """
            Transaction (e.g., ipi_...) that the disputed transaction is a duplicate of. Of the two or more transactions that are copies of each other, this is original undisputed one.
            """

        class Fraudulent(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """

        class MerchandiseNotAsDescribed(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """
            received_at: Optional[int]
            """
            Date when the product was received.
            """
            return_description: Optional[str]
            """
            Description of the cardholder's attempt to return the product.
            """
            return_status: Optional[Literal["merchant_rejected", "successful"]]
            """
            Result of cardholder's attempt to return the product.
            """
            returned_at: Optional[int]
            """
            Date when the product was returned or attempted to be returned.
            """

        class NoValidAuthorization(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """

        class NotReceived(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            expected_at: Optional[int]
            """
            Date when the cardholder expected to receive the product.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """
            product_description: Optional[str]
            """
            Description of the merchandise or service that was purchased.
            """
            product_type: Optional[Literal["merchandise", "service"]]
            """
            Whether the product was a merchandise or service.
            """

        class Other(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """
            product_description: Optional[str]
            """
            Description of the merchandise or service that was purchased.
            """
            product_type: Optional[Literal["merchandise", "service"]]
            """
            Whether the product was a merchandise or service.
            """

        class ServiceNotAsDescribed(StripeObject):
            additional_documentation: Optional[ExpandableField["File"]]
            """
            (ID of a [file upload](https://stripe.com/docs/guides/file-upload)) Additional documentation supporting the dispute.
            """
            canceled_at: Optional[int]
            """
            Date when order was canceled.
            """
            cancellation_reason: Optional[str]
            """
            Reason for canceling the order.
            """
            explanation: Optional[str]
            """
            Explanation of why the cardholder is disputing this transaction.
            """
            received_at: Optional[int]
            """
            Date when the product was received.
            """

        canceled: Optional[Canceled]
        duplicate: Optional[Duplicate]
        fraudulent: Optional[Fraudulent]
        merchandise_not_as_described: Optional[MerchandiseNotAsDescribed]
        no_valid_authorization: Optional[NoValidAuthorization]
        not_received: Optional[NotReceived]
        other: Optional[Other]
        reason: Literal[
            "canceled",
            "duplicate",
            "fraudulent",
            "merchandise_not_as_described",
            "no_valid_authorization",
            "not_received",
            "other",
            "service_not_as_described",
        ]
        """
        The reason for filing the dispute. Its value will match the field containing the evidence.
        """
        service_not_as_described: Optional[ServiceNotAsDescribed]
        _inner_class_types = {
            "canceled": Canceled,
            "duplicate": Duplicate,
            "fraudulent": Fraudulent,
            "merchandise_not_as_described": MerchandiseNotAsDescribed,
            "no_valid_authorization": NoValidAuthorization,
            "not_received": NotReceived,
            "other": Other,
            "service_not_as_described": ServiceNotAsDescribed,
        }

    class Treasury(StripeObject):
        debit_reversal: Optional[str]
        """
        The Treasury [DebitReversal](https://docs.stripe.com/api/treasury/debit_reversals) representing this Issuing dispute
        """
        received_debit: str
        """
        The Treasury [ReceivedDebit](https://docs.stripe.com/api/treasury/received_debits) that is being disputed.
        """

    amount: int
    """
    Disputed amount in the card's currency and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). Usually the amount of the `transaction`, but can differ (usually because of currency fluctuation).
    """
    balance_transactions: Optional[List["BalanceTransaction"]]
    """
    List of balance transactions associated with the dispute.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    The currency the `transaction` was made in.
    """
    evidence: Evidence
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    loss_reason: Optional[
        Literal[
            "cardholder_authentication_issuer_liability",
            "eci5_token_transaction_with_tavv",
            "excess_disputes_in_timeframe",
            "has_not_met_the_minimum_dispute_amount_requirements",
            "invalid_duplicate_dispute",
            "invalid_incorrect_amount_dispute",
            "invalid_no_authorization",
            "invalid_use_of_disputes",
            "merchandise_delivered_or_shipped",
            "merchandise_or_service_as_described",
            "not_cancelled",
            "other",
            "refund_issued",
            "submitted_beyond_allowable_time_limit",
            "transaction_3ds_required",
            "transaction_approved_after_prior_fraud_dispute",
            "transaction_authorized",
            "transaction_electronically_read",
            "transaction_qualifies_for_visa_easy_payment_service",
            "transaction_unattended",
        ]
    ]
    """
    The enum that describes the dispute loss outcome. If the dispute is not lost, this field will be absent. New enum values may be added in the future, so be sure to handle unknown values.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["issuing.dispute"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    status: Literal["expired", "lost", "submitted", "unsubmitted", "won"]
    """
    Current status of the dispute.
    """
    transaction: ExpandableField["Transaction"]
    """
    The transaction being disputed.
    """
    treasury: Optional[Treasury]
    """
    [Treasury](https://docs.stripe.com/api/treasury) details related to this dispute if it was created on a [FinancialAccount](/docs/api/treasury/financial_accounts
    """

    @classmethod
    def create(cls, **params: Unpack["DisputeCreateParams"]) -> "Dispute":
        """
        Creates an Issuing Dispute object. Individual pieces of evidence within the evidence object are optional at this point. Stripe only validates that required evidence is present during submission. Refer to [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence) for more details about evidence requirements.
        """
        return cast(
            "Dispute",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["DisputeCreateParams"]
    ) -> "Dispute":
        """
        Creates an Issuing Dispute object. Individual pieces of evidence within the evidence object are optional at this point. Stripe only validates that required evidence is present during submission. Refer to [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence) for more details about evidence requirements.
        """
        return cast(
            "Dispute",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["DisputeListParams"]
    ) -> ListObject["Dispute"]:
        """
        Returns a list of Issuing Dispute objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, **params: Unpack["DisputeListParams"]
    ) -> ListObject["Dispute"]:
        """
        Returns a list of Issuing Dispute objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, id: str, **params: Unpack["DisputeModifyParams"]
    ) -> "Dispute":
        """
        Updates the specified Issuing Dispute object by setting the values of the parameters passed. Any parameters not provided will be left unchanged. Properties on the evidence object can be unset by passing in an empty string.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Dispute",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["DisputeModifyParams"]
    ) -> "Dispute":
        """
        Updates the specified Issuing Dispute object by setting the values of the parameters passed. Any parameters not provided will be left unchanged. Properties on the evidence object can be unset by passing in an empty string.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Dispute",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["DisputeRetrieveParams"]
    ) -> "Dispute":
        """
        Retrieves an Issuing Dispute object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["DisputeRetrieveParams"]
    ) -> "Dispute":
        """
        Retrieves an Issuing Dispute object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_submit(
        cls, dispute: str, **params: Unpack["DisputeSubmitParams"]
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        return cast(
            "Dispute",
            cls._static_request(
                "post",
                "/v1/issuing/disputes/{dispute}/submit".format(
                    dispute=sanitize_id(dispute)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def submit(
        dispute: str, **params: Unpack["DisputeSubmitParams"]
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        ...

    @overload
    def submit(self, **params: Unpack["DisputeSubmitParams"]) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        ...

    @class_method_variant("_cls_submit")
    def submit(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["DisputeSubmitParams"]
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        return cast(
            "Dispute",
            self._request(
                "post",
                "/v1/issuing/disputes/{dispute}/submit".format(
                    dispute=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_submit_async(
        cls, dispute: str, **params: Unpack["DisputeSubmitParams"]
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        return cast(
            "Dispute",
            await cls._static_request_async(
                "post",
                "/v1/issuing/disputes/{dispute}/submit".format(
                    dispute=sanitize_id(dispute)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def submit_async(
        dispute: str, **params: Unpack["DisputeSubmitParams"]
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        ...

    @overload
    async def submit_async(
        self, **params: Unpack["DisputeSubmitParams"]
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        ...

    @class_method_variant("_cls_submit_async")
    async def submit_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["DisputeSubmitParams"]
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        return cast(
            "Dispute",
            await self._request_async(
                "post",
                "/v1/issuing/disputes/{dispute}/submit".format(
                    dispute=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    _inner_class_types = {"evidence": Evidence, "treasury": Treasury}
