# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._account import Account
from stripe._customer import Customer
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._error import InvalidRequestError
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from stripe._verify_mixin import VerifyMixin
from typing import ClassVar, Dict, List, Optional, Union, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._card import Card
    from stripe.params._bank_account_delete_params import (
        BankAccountDeleteParams,
    )


class BankAccount(
    DeletableAPIResource["BankAccount"],
    UpdateableAPIResource["BankAccount"],
    VerifyMixin,
):
    """
    These bank accounts are payment methods on `Customer` objects.

    On the other hand [External Accounts](https://docs.stripe.com/api#external_accounts) are transfer
    destinations on `Account` objects for connected accounts.
    They can be bank accounts or debit cards as well, and are documented in the links above.

    Related guide: [Bank debits and transfers](https://docs.stripe.com/payments/bank-debits-transfers)
    """

    OBJECT_NAME: ClassVar[Literal["bank_account"]] = "bank_account"

    class FutureRequirements(StripeObject):
        class Error(StripeObject):
            code: Literal[
                "external_request",
                "information_missing",
                "invalid_address_city_state_postal_code",
                "invalid_address_highway_contract_box",
                "invalid_address_private_mailbox",
                "invalid_business_profile_name",
                "invalid_business_profile_name_denylisted",
                "invalid_company_name_denylisted",
                "invalid_dob_age_over_maximum",
                "invalid_dob_age_under_18",
                "invalid_dob_age_under_minimum",
                "invalid_product_description_length",
                "invalid_product_description_url_match",
                "invalid_representative_country",
                "invalid_signator",
                "invalid_statement_descriptor_business_mismatch",
                "invalid_statement_descriptor_denylisted",
                "invalid_statement_descriptor_length",
                "invalid_statement_descriptor_prefix_denylisted",
                "invalid_statement_descriptor_prefix_mismatch",
                "invalid_street_address",
                "invalid_tax_id",
                "invalid_tax_id_format",
                "invalid_tos_acceptance",
                "invalid_url_denylisted",
                "invalid_url_format",
                "invalid_url_length",
                "invalid_url_web_presence_detected",
                "invalid_url_website_business_information_mismatch",
                "invalid_url_website_empty",
                "invalid_url_website_inaccessible",
                "invalid_url_website_inaccessible_geoblocked",
                "invalid_url_website_inaccessible_password_protected",
                "invalid_url_website_incomplete",
                "invalid_url_website_incomplete_cancellation_policy",
                "invalid_url_website_incomplete_customer_service_details",
                "invalid_url_website_incomplete_legal_restrictions",
                "invalid_url_website_incomplete_refund_policy",
                "invalid_url_website_incomplete_return_policy",
                "invalid_url_website_incomplete_terms_and_conditions",
                "invalid_url_website_incomplete_under_construction",
                "invalid_url_website_other",
                "invalid_value_other",
                "unsupported_business_type",
                "verification_directors_mismatch",
                "verification_document_address_mismatch",
                "verification_document_address_missing",
                "verification_document_corrupt",
                "verification_document_country_not_supported",
                "verification_document_directors_mismatch",
                "verification_document_dob_mismatch",
                "verification_document_duplicate_type",
                "verification_document_expired",
                "verification_document_failed_copy",
                "verification_document_failed_greyscale",
                "verification_document_failed_other",
                "verification_document_failed_test_mode",
                "verification_document_fraudulent",
                "verification_document_id_number_mismatch",
                "verification_document_id_number_missing",
                "verification_document_incomplete",
                "verification_document_invalid",
                "verification_document_issue_or_expiry_date_missing",
                "verification_document_manipulated",
                "verification_document_missing_back",
                "verification_document_missing_front",
                "verification_document_name_mismatch",
                "verification_document_name_missing",
                "verification_document_nationality_mismatch",
                "verification_document_not_readable",
                "verification_document_not_signed",
                "verification_document_not_uploaded",
                "verification_document_photo_mismatch",
                "verification_document_too_large",
                "verification_document_type_not_supported",
                "verification_extraneous_directors",
                "verification_failed_address_match",
                "verification_failed_authorizer_authority",
                "verification_failed_business_iec_number",
                "verification_failed_document_match",
                "verification_failed_id_number_match",
                "verification_failed_keyed_identity",
                "verification_failed_keyed_match",
                "verification_failed_name_match",
                "verification_failed_other",
                "verification_failed_representative_authority",
                "verification_failed_residential_address",
                "verification_failed_tax_id_match",
                "verification_failed_tax_id_not_issued",
                "verification_legal_entity_structure_mismatch",
                "verification_missing_directors",
                "verification_missing_executives",
                "verification_missing_owners",
                "verification_rejected_ownership_exemption_reason",
                "verification_requires_additional_memorandum_of_associations",
                "verification_requires_additional_proof_of_registration",
                "verification_supportability",
            ]
            """
            The code for the type of error.
            """
            reason: str
            """
            An informative message that indicates the error type and provides additional details about the error.
            """
            requirement: str
            """
            The specific user onboarding requirement field (in the requirements hash) that needs to be resolved.
            """

        currently_due: Optional[List[str]]
        """
        Fields that need to be resolved to keep the external account enabled. If not resolved by `current_deadline`, these fields will appear in `past_due` as well, and the account is disabled.
        """
        errors: Optional[List[Error]]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        past_due: Optional[List[str]]
        """
        Fields that haven't been resolved by `current_deadline`. These fields need to be resolved to enable the external account.
        """
        pending_verification: Optional[List[str]]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"errors": Error}

    class Requirements(StripeObject):
        class Error(StripeObject):
            code: Literal[
                "external_request",
                "information_missing",
                "invalid_address_city_state_postal_code",
                "invalid_address_highway_contract_box",
                "invalid_address_private_mailbox",
                "invalid_business_profile_name",
                "invalid_business_profile_name_denylisted",
                "invalid_company_name_denylisted",
                "invalid_dob_age_over_maximum",
                "invalid_dob_age_under_18",
                "invalid_dob_age_under_minimum",
                "invalid_product_description_length",
                "invalid_product_description_url_match",
                "invalid_representative_country",
                "invalid_signator",
                "invalid_statement_descriptor_business_mismatch",
                "invalid_statement_descriptor_denylisted",
                "invalid_statement_descriptor_length",
                "invalid_statement_descriptor_prefix_denylisted",
                "invalid_statement_descriptor_prefix_mismatch",
                "invalid_street_address",
                "invalid_tax_id",
                "invalid_tax_id_format",
                "invalid_tos_acceptance",
                "invalid_url_denylisted",
                "invalid_url_format",
                "invalid_url_length",
                "invalid_url_web_presence_detected",
                "invalid_url_website_business_information_mismatch",
                "invalid_url_website_empty",
                "invalid_url_website_inaccessible",
                "invalid_url_website_inaccessible_geoblocked",
                "invalid_url_website_inaccessible_password_protected",
                "invalid_url_website_incomplete",
                "invalid_url_website_incomplete_cancellation_policy",
                "invalid_url_website_incomplete_customer_service_details",
                "invalid_url_website_incomplete_legal_restrictions",
                "invalid_url_website_incomplete_refund_policy",
                "invalid_url_website_incomplete_return_policy",
                "invalid_url_website_incomplete_terms_and_conditions",
                "invalid_url_website_incomplete_under_construction",
                "invalid_url_website_other",
                "invalid_value_other",
                "unsupported_business_type",
                "verification_directors_mismatch",
                "verification_document_address_mismatch",
                "verification_document_address_missing",
                "verification_document_corrupt",
                "verification_document_country_not_supported",
                "verification_document_directors_mismatch",
                "verification_document_dob_mismatch",
                "verification_document_duplicate_type",
                "verification_document_expired",
                "verification_document_failed_copy",
                "verification_document_failed_greyscale",
                "verification_document_failed_other",
                "verification_document_failed_test_mode",
                "verification_document_fraudulent",
                "verification_document_id_number_mismatch",
                "verification_document_id_number_missing",
                "verification_document_incomplete",
                "verification_document_invalid",
                "verification_document_issue_or_expiry_date_missing",
                "verification_document_manipulated",
                "verification_document_missing_back",
                "verification_document_missing_front",
                "verification_document_name_mismatch",
                "verification_document_name_missing",
                "verification_document_nationality_mismatch",
                "verification_document_not_readable",
                "verification_document_not_signed",
                "verification_document_not_uploaded",
                "verification_document_photo_mismatch",
                "verification_document_too_large",
                "verification_document_type_not_supported",
                "verification_extraneous_directors",
                "verification_failed_address_match",
                "verification_failed_authorizer_authority",
                "verification_failed_business_iec_number",
                "verification_failed_document_match",
                "verification_failed_id_number_match",
                "verification_failed_keyed_identity",
                "verification_failed_keyed_match",
                "verification_failed_name_match",
                "verification_failed_other",
                "verification_failed_representative_authority",
                "verification_failed_residential_address",
                "verification_failed_tax_id_match",
                "verification_failed_tax_id_not_issued",
                "verification_legal_entity_structure_mismatch",
                "verification_missing_directors",
                "verification_missing_executives",
                "verification_missing_owners",
                "verification_rejected_ownership_exemption_reason",
                "verification_requires_additional_memorandum_of_associations",
                "verification_requires_additional_proof_of_registration",
                "verification_supportability",
            ]
            """
            The code for the type of error.
            """
            reason: str
            """
            An informative message that indicates the error type and provides additional details about the error.
            """
            requirement: str
            """
            The specific user onboarding requirement field (in the requirements hash) that needs to be resolved.
            """

        currently_due: Optional[List[str]]
        """
        Fields that need to be resolved to keep the external account enabled. If not resolved by `current_deadline`, these fields will appear in `past_due` as well, and the account is disabled.
        """
        errors: Optional[List[Error]]
        """
        Details about validation and verification failures for `due` requirements that must be resolved.
        """
        past_due: Optional[List[str]]
        """
        Fields that haven't been resolved by `current_deadline`. These fields need to be resolved to enable the external account.
        """
        pending_verification: Optional[List[str]]
        """
        Fields that are being reviewed, or might become required depending on the results of a review. If the review fails, these fields can move to `eventually_due`, `currently_due`, `past_due` or `alternatives`. Fields might appear in `eventually_due`, `currently_due`, `past_due` or `alternatives` and in `pending_verification` if one verification fails but another is still pending.
        """
        _inner_class_types = {"errors": Error}

    account: Optional[ExpandableField["Account"]]
    """
    The account this bank account belongs to. Only applicable on Accounts (not customers or recipients) This property is only available when returned as an [External Account](https://docs.stripe.com/api/external_account_bank_accounts/object) where [controller.is_controller](https://docs.stripe.com/api/accounts/object#account_object-controller-is_controller) is `true`.
    """
    account_holder_name: Optional[str]
    """
    The name of the person or business that owns the bank account.
    """
    account_holder_type: Optional[str]
    """
    The type of entity that holds the account. This can be either `individual` or `company`.
    """
    account_type: Optional[str]
    """
    The bank account type. This can only be `checking` or `savings` in most countries. In Japan, this can only be `futsu` or `toza`.
    """
    available_payout_methods: Optional[List[Literal["instant", "standard"]]]
    """
    A set of available payout methods for this bank account. Only values from this set should be passed as the `method` when creating a payout.
    """
    bank_name: Optional[str]
    """
    Name of the bank associated with the routing number (e.g., `WELLS FARGO`).
    """
    country: str
    """
    Two-letter ISO code representing the country the bank account is located in.
    """
    currency: str
    """
    Three-letter [ISO code for the currency](https://stripe.com/docs/payouts) paid out to the bank account.
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    The ID of the customer that the bank account is associated with.
    """
    default_for_currency: Optional[bool]
    """
    Whether this bank account is the default external account for its currency.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    fingerprint: Optional[str]
    """
    Uniquely identifies this particular bank account. You can use this attribute to check whether two bank accounts are the same.
    """
    future_requirements: Optional[FutureRequirements]
    """
    Information about the [upcoming new requirements for the bank account](https://docs.stripe.com/connect/custom-accounts/future-requirements), including what information needs to be collected, and by when.
    """
    id: str
    """
    Unique identifier for the object.
    """
    last4: str
    """
    The last four digits of the bank account number.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["bank_account"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    requirements: Optional[Requirements]
    """
    Information about the requirements for the bank account, including what information needs to be collected.
    """
    routing_number: Optional[str]
    """
    The routing transit number for the bank account.
    """
    status: str
    """
    For bank accounts, possible values are `new`, `validated`, `verified`, `verification_failed`, `tokenized_account_number_deactivated` or `errored`. A bank account that hasn't had any activity or validation performed is `new`. If Stripe can determine that the bank account exists, its status will be `validated`. Note that there often isn't enough information to know (e.g., for smaller credit unions), and the validation is not always run. If customer bank account verification has succeeded, the bank account status will be `verified`. If the verification failed for any reason, such as microdeposit failure, the status will be `verification_failed`. If the status is `tokenized_account_number_deactivated`, the account utilizes a tokenized account number which has been deactivated due to expiration or revocation. This account will need to be reverified to continue using it for money movement. If a payout sent to this bank account fails, we'll set the status to `errored` and will not continue to send [scheduled payouts](https://stripe.com/docs/payouts#payout-schedule) until the bank details are updated.

    For external accounts, possible values are `new`, `errored`, `verification_failed`, and `tokenized_account_number_deactivated`. If a payout fails, the status is set to `errored` and scheduled payouts are stopped until account details are updated. In the US and India, if we can't [verify the owner of the bank account](https://support.stripe.com/questions/bank-account-ownership-verification), we'll set the status to `verification_failed`. Other validations aren't run against external accounts because they're only used for payouts. This means the other statuses don't apply.
    """

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            Union["BankAccount", "Card"],
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            Union["BankAccount", "Card"],
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["BankAccountDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    def instance_url(self):
        token = self.id
        extn = sanitize_id(token)
        if hasattr(self, "customer"):
            customer = self.customer

            base = Customer.class_url()
            assert customer is not None
            if isinstance(customer, Customer):
                customer = customer.id
            owner_extn = sanitize_id(customer)
            class_base = "sources"

        elif hasattr(self, "account"):
            account = self.account

            base = Account.class_url()
            assert account is not None
            if isinstance(account, Account):
                account = account.id
            owner_extn = sanitize_id(account)
            class_base = "external_accounts"

        else:
            raise InvalidRequestError(
                "Could not determine whether bank_account_id %s is "
                "attached to a customer or an account." % token,
                "id",
            )

        return "%s/%s/%s/%s" % (base, owner_extn, class_base, extn)

    @classmethod
    def modify(cls, sid, **params):
        raise NotImplementedError(
            "Can't modify a bank account without a customer or account ID. "
            "Use stripe.Customer.modify_source('customer_id', 'bank_account_id', ...) "
            "(see https://stripe.com/docs/api/customer_bank_accounts/update) or "
            "stripe.Account.modify_external_account('customer_id', 'bank_account_id', ...) "
            "(see https://stripe.com/docs/api/external_account_bank_accounts/update)."
        )

    @classmethod
    def retrieve(cls, id, **params):
        raise NotImplementedError(
            "Can't retrieve a bank account without a customer or account ID. "
            "Use stripe.customer.retrieve_source('customer_id', 'bank_account_id') "
            "(see https://stripe.com/docs/api/customer_bank_accounts/retrieve) or "
            "stripe.Account.retrieve_external_account('account_id', 'bank_account_id') "
            "(see https://stripe.com/docs/api/external_account_bank_accounts/retrieve)."
        )

    _inner_class_types = {
        "future_requirements": FutureRequirements,
        "requirements": Requirements,
    }
