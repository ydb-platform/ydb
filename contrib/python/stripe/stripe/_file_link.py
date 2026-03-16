# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file import File
    from stripe.params._file_link_create_params import FileLinkCreateParams
    from stripe.params._file_link_list_params import FileLinkListParams
    from stripe.params._file_link_modify_params import FileLinkModifyParams
    from stripe.params._file_link_retrieve_params import FileLinkRetrieveParams


class FileLink(
    CreateableAPIResource["FileLink"],
    ListableAPIResource["FileLink"],
    UpdateableAPIResource["FileLink"],
):
    """
    To share the contents of a `File` object with non-Stripe users, you can
    create a `FileLink`. `FileLink`s contain a URL that you can use to
    retrieve the contents of the file without authentication.
    """

    OBJECT_NAME: ClassVar[Literal["file_link"]] = "file_link"
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    expired: bool
    """
    Returns if the link is already expired.
    """
    expires_at: Optional[int]
    """
    Time that the link expires.
    """
    file: ExpandableField["File"]
    """
    The file object this link points to.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["file_link"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    url: Optional[str]
    """
    The publicly accessible URL to download the file.
    """

    @classmethod
    def create(cls, **params: Unpack["FileLinkCreateParams"]) -> "FileLink":
        """
        Creates a new file link object.
        """
        return cast(
            "FileLink",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["FileLinkCreateParams"]
    ) -> "FileLink":
        """
        Creates a new file link object.
        """
        return cast(
            "FileLink",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["FileLinkListParams"]
    ) -> ListObject["FileLink"]:
        """
        Returns a list of file links.
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
        cls, **params: Unpack["FileLinkListParams"]
    ) -> ListObject["FileLink"]:
        """
        Returns a list of file links.
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
        cls, id: str, **params: Unpack["FileLinkModifyParams"]
    ) -> "FileLink":
        """
        Updates an existing file link object. Expired links can no longer be updated.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "FileLink",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["FileLinkModifyParams"]
    ) -> "FileLink":
        """
        Updates an existing file link object. Expired links can no longer be updated.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "FileLink",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["FileLinkRetrieveParams"]
    ) -> "FileLink":
        """
        Retrieves the file link with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["FileLinkRetrieveParams"]
    ) -> "FileLink":
        """
        Retrieves the file link with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
