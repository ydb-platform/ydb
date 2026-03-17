# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from typing import ClassVar, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file_link import FileLink
    from stripe.params._file_create_params import FileCreateParams
    from stripe.params._file_list_params import FileListParams
    from stripe.params._file_retrieve_params import FileRetrieveParams


class File(CreateableAPIResource["File"], ListableAPIResource["File"]):
    """
    This object represents files hosted on Stripe's servers. You can upload
    files with the [create file](https://api.stripe.com#create_file) request
    (for example, when uploading dispute evidence). Stripe also
    creates files independently (for example, the results of a [Sigma scheduled
    query](https://docs.stripe.com/api#scheduled_queries)).

    Related guide: [File upload guide](https://docs.stripe.com/file-upload)
    """

    OBJECT_NAME: ClassVar[Literal["file"]] = "file"
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    expires_at: Optional[int]
    """
    The file expires and isn't available at this time in epoch seconds.
    """
    filename: Optional[str]
    """
    The suitable name for saving the file to a filesystem.
    """
    id: str
    """
    Unique identifier for the object.
    """
    links: Optional[ListObject["FileLink"]]
    """
    A list of [file links](https://api.stripe.com#file_links) that point at this file.
    """
    object: Literal["file"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    purpose: Literal[
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
    """
    The [purpose](https://docs.stripe.com/file-upload#uploading-a-file) of the uploaded file.
    """
    size: int
    """
    The size of the file object in bytes.
    """
    title: Optional[str]
    """
    A suitable title for the document.
    """
    type: Optional[str]
    """
    The returned file type (for example, `csv`, `pdf`, `jpg`, or `png`).
    """
    url: Optional[str]
    """
    Use your live secret API key to download the file from this URL.
    """

    @classmethod
    def create(cls, **params: Unpack["FileCreateParams"]) -> "File":
        """
        To upload a file to Stripe, you need to send a request of type multipart/form-data. Include the file you want to upload in the request, and the parameters for creating a file.

        All of Stripe's officially supported Client libraries support sending multipart/form-data.
        """
        params["content_type"] = "multipart/form-data"

        return cast(
            "File",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
                base_address="files",
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["FileCreateParams"]
    ) -> "File":
        """
        To upload a file to Stripe, you need to send a request of type multipart/form-data. Include the file you want to upload in the request, and the parameters for creating a file.

        All of Stripe's officially supported Client libraries support sending multipart/form-data.
        """
        params["content_type"] = "multipart/form-data"

        return cast(
            "File",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
                base_address="files",
            ),
        )

    @classmethod
    def list(cls, **params: Unpack["FileListParams"]) -> ListObject["File"]:
        """
        Returns a list of the files that your account has access to. Stripe sorts and returns the files by their creation dates, placing the most recently created files at the top.
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
        cls, **params: Unpack["FileListParams"]
    ) -> ListObject["File"]:
        """
        Returns a list of the files that your account has access to. Stripe sorts and returns the files by their creation dates, placing the most recently created files at the top.
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
        cls, id: str, **params: Unpack["FileRetrieveParams"]
    ) -> "File":
        """
        Retrieves the details of an existing file object. After you supply a unique file ID, Stripe returns the corresponding file object. Learn how to [access file contents](https://docs.stripe.com/docs/file-upload#download-file-contents).
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["FileRetrieveParams"]
    ) -> "File":
        """
        Retrieves the details of an existing file object. After you supply a unique file ID, Stripe returns the corresponding file object. Learn how to [access file contents](https://docs.stripe.com/docs/file-upload#download-file-contents).
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    # This resource can have two different object names. In latter API
    # versions, only `file` is used, but since stripe-python may be used with
    # any API version, we need to support deserializing the older
    # `file_upload` object into the same class.
    OBJECT_NAME_ALT = "file_upload"

    @classmethod
    def class_url(cls):
        return "/v1/files"


# For backwards compatibility, the `File` class is aliased to `FileUpload`.
FileUpload = File
