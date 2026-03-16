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
    from stripe.identity._verification_report import VerificationReport
    from stripe.params.identity._verification_session_cancel_params import (
        VerificationSessionCancelParams,
    )
    from stripe.params.identity._verification_session_create_params import (
        VerificationSessionCreateParams,
    )
    from stripe.params.identity._verification_session_list_params import (
        VerificationSessionListParams,
    )
    from stripe.params.identity._verification_session_modify_params import (
        VerificationSessionModifyParams,
    )
    from stripe.params.identity._verification_session_redact_params import (
        VerificationSessionRedactParams,
    )
    from stripe.params.identity._verification_session_retrieve_params import (
        VerificationSessionRetrieveParams,
    )


class VerificationSession(
    CreateableAPIResource["VerificationSession"],
    ListableAPIResource["VerificationSession"],
    UpdateableAPIResource["VerificationSession"],
):
    """
    A VerificationSession guides you through the process of collecting and verifying the identities
    of your users. It contains details about the type of verification, such as what [verification
    check](https://docs.stripe.com/docs/identity/verification-checks) to perform. Only create one VerificationSession for
    each verification in your system.

    A VerificationSession transitions through [multiple
    statuses](https://docs.stripe.com/docs/identity/how-sessions-work) throughout its lifetime as it progresses through
    the verification flow. The VerificationSession contains the user's verified data after
    verification checks are complete.

    Related guide: [The Verification Sessions API](https://docs.stripe.com/identity/verification-sessions)
    """

    OBJECT_NAME: ClassVar[Literal["identity.verification_session"]] = (
        "identity.verification_session"
    )

    class LastError(StripeObject):
        code: Optional[
            Literal[
                "abandoned",
                "consent_declined",
                "country_not_supported",
                "device_not_supported",
                "document_expired",
                "document_type_not_supported",
                "document_unverified_other",
                "email_unverified_other",
                "email_verification_declined",
                "id_number_insufficient_document_data",
                "id_number_mismatch",
                "id_number_unverified_other",
                "phone_unverified_other",
                "phone_verification_declined",
                "selfie_document_missing_photo",
                "selfie_face_mismatch",
                "selfie_manipulated",
                "selfie_unverified_other",
                "under_supported_age",
            ]
        ]
        """
        A short machine-readable string giving the reason for the verification or user-session failure.
        """
        reason: Optional[str]
        """
        A message that explains the reason for verification or user-session failure.
        """

    class Options(StripeObject):
        class Document(StripeObject):
            allowed_types: Optional[
                List[Literal["driving_license", "id_card", "passport"]]
            ]
            """
            Array of strings of allowed identity document types. If the provided identity document isn't one of the allowed types, the verification check will fail with a document_type_not_allowed error code.
            """
            require_id_number: Optional[bool]
            """
            Collect an ID number and perform an [ID number check](https://docs.stripe.com/identity/verification-checks?type=id-number) with the document's extracted name and date of birth.
            """
            require_live_capture: Optional[bool]
            """
            Disable image uploads, identity document images have to be captured using the device's camera.
            """
            require_matching_selfie: Optional[bool]
            """
            Capture a face image and perform a [selfie check](https://docs.stripe.com/identity/verification-checks?type=selfie) comparing a photo ID and a picture of your user's face. [Learn more](https://docs.stripe.com/identity/selfie).
            """

        class Email(StripeObject):
            require_verification: Optional[bool]
            """
            Request one time password verification of `provided_details.email`.
            """

        class IdNumber(StripeObject):
            pass

        class Matching(StripeObject):
            dob: Optional[Literal["none", "similar"]]
            """
            Strictness of the DOB matching policy to apply.
            """
            name: Optional[Literal["none", "similar"]]
            """
            Strictness of the name matching policy to apply.
            """

        class Phone(StripeObject):
            require_verification: Optional[bool]
            """
            Request one time password verification of `provided_details.phone`.
            """

        document: Optional[Document]
        email: Optional[Email]
        id_number: Optional[IdNumber]
        matching: Optional[Matching]
        phone: Optional[Phone]
        _inner_class_types = {
            "document": Document,
            "email": Email,
            "id_number": IdNumber,
            "matching": Matching,
            "phone": Phone,
        }

    class ProvidedDetails(StripeObject):
        email: Optional[str]
        """
        Email of user being verified
        """
        phone: Optional[str]
        """
        Phone number of user being verified
        """

    class Redaction(StripeObject):
        status: Literal["processing", "redacted"]
        """
        Indicates whether this object and its related objects have been redacted or not.
        """

    class RelatedPerson(StripeObject):
        account: str
        """
        Token referencing the associated Account of the related Person resource.
        """
        person: str
        """
        Token referencing the related Person resource.
        """

    class VerifiedOutputs(StripeObject):
        class Address(StripeObject):
            city: Optional[str]
            """
            City, district, suburb, town, or village.
            """
            country: Optional[str]
            """
            Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
            """
            line1: Optional[str]
            """
            Address line 1, such as the street, PO Box, or company name.
            """
            line2: Optional[str]
            """
            Address line 2, such as the apartment, suite, unit, or building.
            """
            postal_code: Optional[str]
            """
            ZIP or postal code.
            """
            state: Optional[str]
            """
            State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
            """

        class Dob(StripeObject):
            day: Optional[int]
            """
            Numerical day between 1 and 31.
            """
            month: Optional[int]
            """
            Numerical month between 1 and 12.
            """
            year: Optional[int]
            """
            The four-digit year.
            """

        address: Optional[Address]
        """
        The user's verified address.
        """
        dob: Optional[Dob]
        """
        The user's verified date of birth.
        """
        email: Optional[str]
        """
        The user's verified email address
        """
        first_name: Optional[str]
        """
        The user's verified first name.
        """
        id_number: Optional[str]
        """
        The user's verified id number.
        """
        id_number_type: Optional[Literal["br_cpf", "sg_nric", "us_ssn"]]
        """
        The user's verified id number type.
        """
        last_name: Optional[str]
        """
        The user's verified last name.
        """
        phone: Optional[str]
        """
        The user's verified phone number
        """
        sex: Optional[Literal["[redacted]", "female", "male", "unknown"]]
        """
        The user's verified sex.
        """
        unparsed_place_of_birth: Optional[str]
        """
        The user's verified place of birth as it appears in the document.
        """
        unparsed_sex: Optional[str]
        """
        The user's verified sex as it appears in the document.
        """
        _inner_class_types = {"address": Address, "dob": Dob}

    client_reference_id: Optional[str]
    """
    A string to reference this user. This can be a customer ID, a session ID, or similar, and can be used to reconcile this verification with your internal systems.
    """
    client_secret: Optional[str]
    """
    The short-lived client secret used by Stripe.js to [show a verification modal](https://docs.stripe.com/js/identity/modal) inside your app. This client secret expires after 24 hours and can only be used once. Don't store it, log it, embed it in a URL, or expose it to anyone other than the user. Make sure that you have TLS enabled on any page that includes the client secret. Refer to our docs on [passing the client secret to the frontend](https://docs.stripe.com/identity/verification-sessions#client-secret) to learn more.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    last_error: Optional[LastError]
    """
    If present, this property tells you the last error encountered when processing the verification.
    """
    last_verification_report: Optional[ExpandableField["VerificationReport"]]
    """
    ID of the most recent VerificationReport. [Learn more about accessing detailed verification results.](https://docs.stripe.com/identity/verification-sessions#results)
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["identity.verification_session"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    options: Optional[Options]
    """
    A set of options for the session's verification checks.
    """
    provided_details: Optional[ProvidedDetails]
    """
    Details provided about the user being verified. These details may be shown to the user.
    """
    redaction: Optional[Redaction]
    """
    Redaction status of this VerificationSession. If the VerificationSession is not redacted, this field will be null.
    """
    related_customer: Optional[str]
    """
    Customer ID
    """
    related_customer_account: Optional[str]
    """
    The ID of the Account representing a customer.
    """
    related_person: Optional[RelatedPerson]
    status: Literal["canceled", "processing", "requires_input", "verified"]
    """
    Status of this VerificationSession. [Learn more about the lifecycle of sessions](https://docs.stripe.com/identity/how-sessions-work).
    """
    type: Literal["document", "id_number", "verification_flow"]
    """
    The type of [verification check](https://docs.stripe.com/identity/verification-checks) to be performed.
    """
    url: Optional[str]
    """
    The short-lived URL that you use to redirect a user to Stripe to submit their identity information. This URL expires after 48 hours and can only be used once. Don't store it, log it, send it in emails or expose it to anyone other than the user. Refer to our docs on [verifying identity documents](https://docs.stripe.com/identity/verify-identity-documents?platform=web&type=redirect) to learn how to redirect users to Stripe.
    """
    verification_flow: Optional[str]
    """
    The configuration token of a verification flow from the dashboard.
    """
    verified_outputs: Optional[VerifiedOutputs]
    """
    The user's verified data.
    """

    @classmethod
    def _cls_cancel(
        cls, session: str, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        return cast(
            "VerificationSession",
            cls._static_request(
                "post",
                "/v1/identity/verification_sessions/{session}/cancel".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        session: str, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        return cast(
            "VerificationSession",
            self._request(
                "post",
                "/v1/identity/verification_sessions/{session}/cancel".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls, session: str, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        return cast(
            "VerificationSession",
            await cls._static_request_async(
                "post",
                "/v1/identity/verification_sessions/{session}/cancel".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        session: str, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["VerificationSessionCancelParams"]
    ) -> "VerificationSession":
        """
        A VerificationSession object can be canceled when it is in requires_input [status](https://docs.stripe.com/docs/identity/how-sessions-work).

        Once canceled, future submission attempts are disabled. This cannot be undone. [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#cancel).
        """
        return cast(
            "VerificationSession",
            await self._request_async(
                "post",
                "/v1/identity/verification_sessions/{session}/cancel".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["VerificationSessionCreateParams"]
    ) -> "VerificationSession":
        """
        Creates a VerificationSession object.

        After the VerificationSession is created, display a verification modal using the session client_secret or send your users to the session's url.

        If your API key is in test mode, verification checks won't actually process, though everything else will occur as if in live mode.

        Related guide: [Verify your users' identity documents](https://docs.stripe.com/docs/identity/verify-identity-documents)
        """
        return cast(
            "VerificationSession",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["VerificationSessionCreateParams"]
    ) -> "VerificationSession":
        """
        Creates a VerificationSession object.

        After the VerificationSession is created, display a verification modal using the session client_secret or send your users to the session's url.

        If your API key is in test mode, verification checks won't actually process, though everything else will occur as if in live mode.

        Related guide: [Verify your users' identity documents](https://docs.stripe.com/docs/identity/verify-identity-documents)
        """
        return cast(
            "VerificationSession",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["VerificationSessionListParams"]
    ) -> ListObject["VerificationSession"]:
        """
        Returns a list of VerificationSessions
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
        cls, **params: Unpack["VerificationSessionListParams"]
    ) -> ListObject["VerificationSession"]:
        """
        Returns a list of VerificationSessions
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
        cls, id: str, **params: Unpack["VerificationSessionModifyParams"]
    ) -> "VerificationSession":
        """
        Updates a VerificationSession object.

        When the session status is requires_input, you can use this method to update the
        verification check and options.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "VerificationSession",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["VerificationSessionModifyParams"]
    ) -> "VerificationSession":
        """
        Updates a VerificationSession object.

        When the session status is requires_input, you can use this method to update the
        verification check and options.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "VerificationSession",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def _cls_redact(
        cls, session: str, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        return cast(
            "VerificationSession",
            cls._static_request(
                "post",
                "/v1/identity/verification_sessions/{session}/redact".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def redact(
        session: str, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        ...

    @overload
    def redact(
        self, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        ...

    @class_method_variant("_cls_redact")
    def redact(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        return cast(
            "VerificationSession",
            self._request(
                "post",
                "/v1/identity/verification_sessions/{session}/redact".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_redact_async(
        cls, session: str, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        return cast(
            "VerificationSession",
            await cls._static_request_async(
                "post",
                "/v1/identity/verification_sessions/{session}/redact".format(
                    session=sanitize_id(session)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def redact_async(
        session: str, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        ...

    @overload
    async def redact_async(
        self, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        ...

    @class_method_variant("_cls_redact_async")
    async def redact_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["VerificationSessionRedactParams"]
    ) -> "VerificationSession":
        """
        Redact a VerificationSession to remove all collected information from Stripe. This will redact
        the VerificationSession and all objects related to it, including VerificationReports, Events,
        request logs, etc.

        A VerificationSession object can be redacted when it is in requires_input or verified
        [status](https://docs.stripe.com/docs/identity/how-sessions-work). Redacting a VerificationSession in requires_action
        state will automatically cancel it.

        The redaction process may take up to four days. When the redaction process is in progress, the
        VerificationSession's redaction.status field will be set to processing; when the process is
        finished, it will change to redacted and an identity.verification_session.redacted event
        will be emitted.

        Redaction is irreversible. Redacted objects are still accessible in the Stripe API, but all the
        fields that contain personal data will be replaced by the string [redacted] or a similar
        placeholder. The metadata field will also be erased. Redacted objects cannot be updated or
        used for any purpose.

        [Learn more](https://docs.stripe.com/docs/identity/verification-sessions#redact).
        """
        return cast(
            "VerificationSession",
            await self._request_async(
                "post",
                "/v1/identity/verification_sessions/{session}/redact".format(
                    session=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["VerificationSessionRetrieveParams"]
    ) -> "VerificationSession":
        """
        Retrieves the details of a VerificationSession that was previously created.

        When the session status is requires_input, you can use this method to retrieve a valid
        client_secret or url to allow re-submission.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["VerificationSessionRetrieveParams"]
    ) -> "VerificationSession":
        """
        Retrieves the details of a VerificationSession that was previously created.

        When the session status is requires_input, you can use this method to retrieve a valid
        client_secret or url to allow re-submission.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "last_error": LastError,
        "options": Options,
        "provided_details": ProvidedDetails,
        "redaction": Redaction,
        "related_person": RelatedPerson,
        "verified_outputs": VerifiedOutputs,
    }
