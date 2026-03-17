# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Any, Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class FileCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    file: Any
    """
    A file to upload. Make sure that the specifications follow RFC 2388, which defines file transfers for the `multipart/form-data` protocol.
    """
    file_link_data: NotRequired["FileCreateParamsFileLinkData"]
    """
    Optional parameters that automatically create a [file link](https://api.stripe.com#file_links) for the newly created file.
    """
    purpose: Literal[
        "account_requirement",
        "additional_verification",
        "business_icon",
        "business_logo",
        "customer_signature",
        "dispute_evidence",
        "identity_document",
        "issuing_regulatory_reporting",
        "pci_document",
        "platform_terms_of_service",
        "tax_document_user_upload",
        "terminal_android_apk",
        "terminal_reader_splashscreen",
        "terminal_wifi_certificate",
        "terminal_wifi_private_key",
    ]
    """
    The [purpose](https://docs.stripe.com/file-upload#uploading-a-file) of the uploaded file.
    """


class FileCreateParamsFileLinkData(TypedDict):
    create: bool
    """
    Set this to `true` to create a file link for the newly created file. Creating a link is only possible when the file's `purpose` is one of the following: `business_icon`, `business_logo`, `customer_signature`, `dispute_evidence`, `issuing_regulatory_reporting`, `pci_document`, `tax_document_user_upload`, `terminal_android_apk`, or `terminal_reader_splashscreen`.
    """
    expires_at: NotRequired[int]
    """
    The link isn't available after this future timestamp.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
