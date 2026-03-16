# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file import File
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._file_create_params import FileCreateParams
    from stripe.params._file_list_params import FileListParams
    from stripe.params._file_retrieve_params import FileRetrieveParams


class FileService(StripeService):
    def list(
        self,
        params: Optional["FileListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[File]":
        """
        Returns a list of the files that your account has access to. Stripe sorts and returns the files by their creation dates, placing the most recently created files at the top.
        """
        return cast(
            "ListObject[File]",
            self._request(
                "get",
                "/v1/files",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["FileListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[File]":
        """
        Returns a list of the files that your account has access to. Stripe sorts and returns the files by their creation dates, placing the most recently created files at the top.
        """
        return cast(
            "ListObject[File]",
            await self._request_async(
                "get",
                "/v1/files",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "FileCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "File":
        """
        To upload a file to Stripe, you need to send a request of type multipart/form-data. Include the file you want to upload in the request, and the parameters for creating a file.

        All of Stripe's officially supported Client libraries support sending multipart/form-data.
        """
        if options is None:
            options = {}
        options["content_type"] = "multipart/form-data"
        return cast(
            "File",
            self._request(
                "post",
                "/v1/files",
                base_address="files",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "FileCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "File":
        """
        To upload a file to Stripe, you need to send a request of type multipart/form-data. Include the file you want to upload in the request, and the parameters for creating a file.

        All of Stripe's officially supported Client libraries support sending multipart/form-data.
        """
        if options is None:
            options = {}
        options["content_type"] = "multipart/form-data"
        return cast(
            "File",
            await self._request_async(
                "post",
                "/v1/files",
                base_address="files",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        file: str,
        params: Optional["FileRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "File":
        """
        Retrieves the details of an existing file object. After you supply a unique file ID, Stripe returns the corresponding file object. Learn how to [access file contents](https://docs.stripe.com/docs/file-upload#download-file-contents).
        """
        return cast(
            "File",
            self._request(
                "get",
                "/v1/files/{file}".format(file=sanitize_id(file)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        file: str,
        params: Optional["FileRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "File":
        """
        Retrieves the details of an existing file object. After you supply a unique file ID, Stripe returns the corresponding file object. Learn how to [access file contents](https://docs.stripe.com/docs/file-upload#download-file-contents).
        """
        return cast(
            "File",
            await self._request_async(
                "get",
                "/v1/files/{file}".format(file=sanitize_id(file)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
