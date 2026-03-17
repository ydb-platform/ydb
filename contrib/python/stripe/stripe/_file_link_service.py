# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file_link import FileLink
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._file_link_create_params import FileLinkCreateParams
    from stripe.params._file_link_list_params import FileLinkListParams
    from stripe.params._file_link_retrieve_params import FileLinkRetrieveParams
    from stripe.params._file_link_update_params import FileLinkUpdateParams


class FileLinkService(StripeService):
    def list(
        self,
        params: Optional["FileLinkListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[FileLink]":
        """
        Returns a list of file links.
        """
        return cast(
            "ListObject[FileLink]",
            self._request(
                "get",
                "/v1/file_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["FileLinkListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[FileLink]":
        """
        Returns a list of file links.
        """
        return cast(
            "ListObject[FileLink]",
            await self._request_async(
                "get",
                "/v1/file_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "FileLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "FileLink":
        """
        Creates a new file link object.
        """
        return cast(
            "FileLink",
            self._request(
                "post",
                "/v1/file_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "FileLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "FileLink":
        """
        Creates a new file link object.
        """
        return cast(
            "FileLink",
            await self._request_async(
                "post",
                "/v1/file_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        link: str,
        params: Optional["FileLinkRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FileLink":
        """
        Retrieves the file link with the given ID.
        """
        return cast(
            "FileLink",
            self._request(
                "get",
                "/v1/file_links/{link}".format(link=sanitize_id(link)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        link: str,
        params: Optional["FileLinkRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FileLink":
        """
        Retrieves the file link with the given ID.
        """
        return cast(
            "FileLink",
            await self._request_async(
                "get",
                "/v1/file_links/{link}".format(link=sanitize_id(link)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        link: str,
        params: Optional["FileLinkUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FileLink":
        """
        Updates an existing file link object. Expired links can no longer be updated.
        """
        return cast(
            "FileLink",
            self._request(
                "post",
                "/v1/file_links/{link}".format(link=sanitize_id(link)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        link: str,
        params: Optional["FileLinkUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FileLink":
        """
        Updates an existing file link object. Expired links can no longer be updated.
        """
        return cast(
            "FileLink",
            await self._request_async(
                "post",
                "/v1/file_links/{link}".format(link=sanitize_id(link)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
