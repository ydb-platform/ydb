# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, List, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.identity._verification_report_list_params import (
        VerificationReportListParams,
    )
    from stripe.params.identity._verification_report_retrieve_params import (
        VerificationReportRetrieveParams,
    )


class VerificationReport(ListableAPIResource["VerificationReport"]):
    """
    A VerificationReport is the result of an attempt to collect and verify data from a user.
    The collection of verification checks performed is determined from the `type` and `options`
    parameters used. You can find the result of each verification check performed in the
    appropriate sub-resource: `document`, `id_number`, `selfie`.

    Each VerificationReport contains a copy of any data collected by the user as well as
    reference IDs which can be used to access collected images through the [FileUpload](https://docs.stripe.com/api/files)
    API. To configure and create VerificationReports, use the
    [VerificationSession](https://docs.stripe.com/api/identity/verification_sessions) API.

    Related guide: [Accessing verification results](https://docs.stripe.com/identity/verification-sessions#results).
    """

    OBJECT_NAME: ClassVar[Literal["identity.verification_report"]] = (
        "identity.verification_report"
    )

    class Document(StripeObject):
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

        class Error(StripeObject):
            code: Optional[
                Literal[
                    "document_expired",
                    "document_type_not_supported",
                    "document_unverified_other",
                ]
            ]
            """
            A short machine-readable string giving the reason for the verification failure.
            """
            reason: Optional[str]
            """
            A human-readable message giving the reason for the failure. These messages can be shown to your users.
            """

        class ExpirationDate(StripeObject):
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

        class IssuedDate(StripeObject):
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
        Address as it appears in the document.
        """
        dob: Optional[Dob]
        """
        Date of birth as it appears in the document.
        """
        error: Optional[Error]
        """
        Details on the verification error. Present when status is `unverified`.
        """
        expiration_date: Optional[ExpirationDate]
        """
        Expiration date of the document.
        """
        files: Optional[List[str]]
        """
        Array of [File](https://docs.stripe.com/api/files) ids containing images for this document.
        """
        first_name: Optional[str]
        """
        First name as it appears in the document.
        """
        issued_date: Optional[IssuedDate]
        """
        Issued date of the document.
        """
        issuing_country: Optional[str]
        """
        Issuing country of the document.
        """
        last_name: Optional[str]
        """
        Last name as it appears in the document.
        """
        number: Optional[str]
        """
        Document ID number.
        """
        sex: Optional[Literal["[redacted]", "female", "male", "unknown"]]
        """
        Sex of the person in the document.
        """
        status: Literal["unverified", "verified"]
        """
        Status of this `document` check.
        """
        type: Optional[Literal["driving_license", "id_card", "passport"]]
        """
        Type of the document.
        """
        unparsed_place_of_birth: Optional[str]
        """
        Place of birth as it appears in the document.
        """
        unparsed_sex: Optional[str]
        """
        Sex as it appears in the document.
        """
        _inner_class_types = {
            "address": Address,
            "dob": Dob,
            "error": Error,
            "expiration_date": ExpirationDate,
            "issued_date": IssuedDate,
        }

    class Email(StripeObject):
        class Error(StripeObject):
            code: Optional[
                Literal[
                    "email_unverified_other", "email_verification_declined"
                ]
            ]
            """
            A short machine-readable string giving the reason for the verification failure.
            """
            reason: Optional[str]
            """
            A human-readable message giving the reason for the failure. These messages can be shown to your users.
            """

        email: Optional[str]
        """
        Email to be verified.
        """
        error: Optional[Error]
        """
        Details on the verification error. Present when status is `unverified`.
        """
        status: Literal["unverified", "verified"]
        """
        Status of this `email` check.
        """
        _inner_class_types = {"error": Error}

    class IdNumber(StripeObject):
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

        class Error(StripeObject):
            code: Optional[
                Literal[
                    "id_number_insufficient_document_data",
                    "id_number_mismatch",
                    "id_number_unverified_other",
                ]
            ]
            """
            A short machine-readable string giving the reason for the verification failure.
            """
            reason: Optional[str]
            """
            A human-readable message giving the reason for the failure. These messages can be shown to your users.
            """

        dob: Optional[Dob]
        """
        Date of birth.
        """
        error: Optional[Error]
        """
        Details on the verification error. Present when status is `unverified`.
        """
        first_name: Optional[str]
        """
        First name.
        """
        id_number: Optional[str]
        """
        ID number. When `id_number_type` is `us_ssn`, only the last 4 digits are present.
        """
        id_number_type: Optional[Literal["br_cpf", "sg_nric", "us_ssn"]]
        """
        Type of ID number.
        """
        last_name: Optional[str]
        """
        Last name.
        """
        status: Literal["unverified", "verified"]
        """
        Status of this `id_number` check.
        """
        _inner_class_types = {"dob": Dob, "error": Error}

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

        class IdNumber(StripeObject):
            pass

        document: Optional[Document]
        id_number: Optional[IdNumber]
        _inner_class_types = {"document": Document, "id_number": IdNumber}

    class Phone(StripeObject):
        class Error(StripeObject):
            code: Optional[
                Literal[
                    "phone_unverified_other", "phone_verification_declined"
                ]
            ]
            """
            A short machine-readable string giving the reason for the verification failure.
            """
            reason: Optional[str]
            """
            A human-readable message giving the reason for the failure. These messages can be shown to your users.
            """

        error: Optional[Error]
        """
        Details on the verification error. Present when status is `unverified`.
        """
        phone: Optional[str]
        """
        Phone to be verified.
        """
        status: Literal["unverified", "verified"]
        """
        Status of this `phone` check.
        """
        _inner_class_types = {"error": Error}

    class Selfie(StripeObject):
        class Error(StripeObject):
            code: Optional[
                Literal[
                    "selfie_document_missing_photo",
                    "selfie_face_mismatch",
                    "selfie_manipulated",
                    "selfie_unverified_other",
                ]
            ]
            """
            A short machine-readable string giving the reason for the verification failure.
            """
            reason: Optional[str]
            """
            A human-readable message giving the reason for the failure. These messages can be shown to your users.
            """

        document: Optional[str]
        """
        ID of the [File](https://docs.stripe.com/api/files) holding the image of the identity document used in this check.
        """
        error: Optional[Error]
        """
        Details on the verification error. Present when status is `unverified`.
        """
        selfie: Optional[str]
        """
        ID of the [File](https://docs.stripe.com/api/files) holding the image of the selfie used in this check.
        """
        status: Literal["unverified", "verified"]
        """
        Status of this `selfie` check.
        """
        _inner_class_types = {"error": Error}

    client_reference_id: Optional[str]
    """
    A string to reference this user. This can be a customer ID, a session ID, or similar, and can be used to reconcile this verification with your internal systems.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    document: Optional[Document]
    """
    Result from a document check
    """
    email: Optional[Email]
    """
    Result from a email check
    """
    id: str
    """
    Unique identifier for the object.
    """
    id_number: Optional[IdNumber]
    """
    Result from an id_number check
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["identity.verification_report"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    options: Optional[Options]
    phone: Optional[Phone]
    """
    Result from a phone check
    """
    selfie: Optional[Selfie]
    """
    Result from a selfie check
    """
    type: Literal["document", "id_number", "verification_flow"]
    """
    Type of report.
    """
    verification_flow: Optional[str]
    """
    The configuration token of a verification flow from the dashboard.
    """
    verification_session: Optional[str]
    """
    ID of the VerificationSession that created this report.
    """

    @classmethod
    def list(
        cls, **params: Unpack["VerificationReportListParams"]
    ) -> ListObject["VerificationReport"]:
        """
        List all verification reports.
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
        cls, **params: Unpack["VerificationReportListParams"]
    ) -> ListObject["VerificationReport"]:
        """
        List all verification reports.
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
    def retrieve(
        cls, id: str, **params: Unpack["VerificationReportRetrieveParams"]
    ) -> "VerificationReport":
        """
        Retrieves an existing VerificationReport
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["VerificationReportRetrieveParams"]
    ) -> "VerificationReport":
        """
        Retrieves an existing VerificationReport
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "document": Document,
        "email": Email,
        "id_number": IdNumber,
        "options": Options,
        "phone": Phone,
        "selfie": Selfie,
    }
