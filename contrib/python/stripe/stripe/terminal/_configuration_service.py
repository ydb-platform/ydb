# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.terminal._configuration_create_params import (
        ConfigurationCreateParams,
    )
    from stripe.params.terminal._configuration_delete_params import (
        ConfigurationDeleteParams,
    )
    from stripe.params.terminal._configuration_list_params import (
        ConfigurationListParams,
    )
    from stripe.params.terminal._configuration_retrieve_params import (
        ConfigurationRetrieveParams,
    )
    from stripe.params.terminal._configuration_update_params import (
        ConfigurationUpdateParams,
    )
    from stripe.terminal._configuration import Configuration


class ConfigurationService(StripeService):
    def delete(
        self,
        configuration: str,
        params: Optional["ConfigurationDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        return cast(
            "Configuration",
            self._request(
                "delete",
                "/v1/terminal/configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        configuration: str,
        params: Optional["ConfigurationDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Deletes a Configuration object.
        """
        return cast(
            "Configuration",
            await self._request_async(
                "delete",
                "/v1/terminal/configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        configuration: str,
        params: Optional["ConfigurationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Retrieves a Configuration object.
        """
        return cast(
            "Configuration",
            self._request(
                "get",
                "/v1/terminal/configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        configuration: str,
        params: Optional["ConfigurationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Retrieves a Configuration object.
        """
        return cast(
            "Configuration",
            await self._request_async(
                "get",
                "/v1/terminal/configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        configuration: str,
        params: Optional["ConfigurationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Updates a new Configuration object.
        """
        return cast(
            "Configuration",
            self._request(
                "post",
                "/v1/terminal/configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        configuration: str,
        params: Optional["ConfigurationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Updates a new Configuration object.
        """
        return cast(
            "Configuration",
            await self._request_async(
                "post",
                "/v1/terminal/configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["ConfigurationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Configuration]":
        """
        Returns a list of Configuration objects.
        """
        return cast(
            "ListObject[Configuration]",
            self._request(
                "get",
                "/v1/terminal/configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ConfigurationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Configuration]":
        """
        Returns a list of Configuration objects.
        """
        return cast(
            "ListObject[Configuration]",
            await self._request_async(
                "get",
                "/v1/terminal/configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["ConfigurationCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Creates a new Configuration object.
        """
        return cast(
            "Configuration",
            self._request(
                "post",
                "/v1/terminal/configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["ConfigurationCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Creates a new Configuration object.
        """
        return cast(
            "Configuration",
            await self._request_async(
                "post",
                "/v1/terminal/configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )
