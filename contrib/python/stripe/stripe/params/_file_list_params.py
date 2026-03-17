# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class FileListParams(RequestOptions):
    created: NotRequired["FileListParamsCreated|int"]
    """
    Only return files that were created during the given date interval.
    """
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    purpose: NotRequired[
        Literal[
            "account_requirement",
            "additional_verification",
            "business_icon",
            "business_logo",
            "customer_signature",
            "dispute_evidence",
            "document_provider_identity_document",
            "finance_report_run",
            "financial_account_statement",
            "identity_document",
            "identity_document_downloadable",
            "issuing_regulatory_reporting",
            "pci_document",
            "platform_terms_of_service",
            "selfie",
            "sigma_scheduled_query",
            "tax_document_user_upload",
            "terminal_android_apk",
            "terminal_reader_splashscreen",
            "terminal_wifi_certificate",
            "terminal_wifi_private_key",
        ]
    ]
    """
    Filter queries by the file purpose. If you don't provide a purpose, the queries return unfiltered files.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """


class FileListParamsCreated(TypedDict):
    gt: NotRequired[int]
    """
    Minimum value to filter by (exclusive)
    """
    gte: NotRequired[int]
    """
    Minimum value to filter by (inclusive)
    """
    lt: NotRequired[int]
    """
    Maximum value to filter by (exclusive)
    """
    lte: NotRequired[int]
    """
    Maximum value to filter by (inclusive)
    """
