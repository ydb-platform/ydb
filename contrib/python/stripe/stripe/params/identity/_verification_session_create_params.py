# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class VerificationSessionCreateParams(RequestOptions):
    client_reference_id: NotRequired[str]
    """
    A string to reference this user. This can be a customer ID, a session ID, or similar, and can be used to reconcile this verification with your internal systems.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    options: NotRequired["VerificationSessionCreateParamsOptions"]
    """
    A set of options for the session's verification checks.
    """
    provided_details: NotRequired[
        "VerificationSessionCreateParamsProvidedDetails"
    ]
    """
    Details provided about the user being verified. These details may be shown to the user.
    """
    related_customer: NotRequired[str]
    """
    Customer ID
    """
    related_customer_account: NotRequired[str]
    """
    The ID of the Account representing a customer.
    """
    related_person: NotRequired["VerificationSessionCreateParamsRelatedPerson"]
    """
    Tokens referencing a Person resource and it's associated account.
    """
    return_url: NotRequired[str]
    """
    The URL that the user will be redirected to upon completing the verification flow.
    """
    type: NotRequired[Literal["document", "id_number"]]
    """
    The type of [verification check](https://docs.stripe.com/identity/verification-checks) to be performed. You must provide a `type` if not passing `verification_flow`.
    """
    verification_flow: NotRequired[str]
    """
    The ID of a verification flow from the Dashboard. See https://docs.stripe.com/identity/verification-flows.
    """


class VerificationSessionCreateParamsOptions(TypedDict):
    document: NotRequired[
        "Literal['']|VerificationSessionCreateParamsOptionsDocument"
    ]
    """
    Options that apply to the [document check](https://docs.stripe.com/identity/verification-checks?type=document).
    """


class VerificationSessionCreateParamsOptionsDocument(TypedDict):
    allowed_types: NotRequired[
        List[Literal["driving_license", "id_card", "passport"]]
    ]
    """
    Array of strings of allowed identity document types. If the provided identity document isn't one of the allowed types, the verification check will fail with a document_type_not_allowed error code.
    """
    require_id_number: NotRequired[bool]
    """
    Collect an ID number and perform an [ID number check](https://docs.stripe.com/identity/verification-checks?type=id-number) with the document's extracted name and date of birth.
    """
    require_live_capture: NotRequired[bool]
    """
    Disable image uploads, identity document images have to be captured using the device's camera.
    """
    require_matching_selfie: NotRequired[bool]
    """
    Capture a face image and perform a [selfie check](https://docs.stripe.com/identity/verification-checks?type=selfie) comparing a photo ID and a picture of your user's face. [Learn more](https://docs.stripe.com/identity/selfie).
    """


class VerificationSessionCreateParamsProvidedDetails(TypedDict):
    email: NotRequired[str]
    """
    Email of user being verified
    """
    phone: NotRequired[str]
    """
    Phone number of user being verified
    """


class VerificationSessionCreateParamsRelatedPerson(TypedDict):
    account: str
    """
    A token representing a connected account. If provided, the person parameter is also required and must be associated with the account.
    """
    person: str
    """
    A token referencing a Person resource that this verification is being used to verify.
    """
