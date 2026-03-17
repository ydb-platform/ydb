# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.billing_portal._configuration import Configuration
    from stripe.params.billing_portal._configuration_create_params import (
        ConfigurationCreateParams,
    )
    from stripe.params.billing_portal._configuration_list_params import (
        ConfigurationListParams,
    )
    from stripe.params.billing_portal._configuration_retrieve_params import (
        ConfigurationRetrieveParams,
    )
    from stripe.params.billing_portal._configuration_update_params import (
        ConfigurationUpdateParams,
    )


class ConfigurationService(StripeService):
    def list(
        self,
        params: Optional["ConfigurationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Configuration]":
        """
        Returns a list of configurations that describe the functionality of the customer portal.
        """
        return cast(
            "ListObject[Configuration]",
            self._request(
                "get",
                "/v1/billing_portal/configurations",
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
        Returns a list of configurations that describe the functionality of the customer portal.
        """
        return cast(
            "ListObject[Configuration]",
            await self._request_async(
                "get",
                "/v1/billing_portal/configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ConfigurationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Creates a configuration that describes the functionality and behavior of a PortalSession
        """
        return cast(
            "Configuration",
            self._request(
                "post",
                "/v1/billing_portal/configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ConfigurationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Configuration":
        """
        Creates a configuration that describes the functionality and behavior of a PortalSession
        """
        return cast(
            "Configuration",
            await self._request_async(
                "post",
                "/v1/billing_portal/configurations",
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
        Retrieves a configuration that describes the functionality of the customer portal.
        """
        return cast(
            "Configuration",
            self._request(
                "get",
                "/v1/billing_portal/configurations/{configuration}".format(
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
        Retrieves a configuration that describes the functionality of the customer portal.
        """
        return cast(
            "Configuration",
            await self._request_async(
                "get",
                "/v1/billing_portal/configurations/{configuration}".format(
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
        Updates a configuration that describes the functionality of the customer portal.
        """
        return cast(
            "Configuration",
            self._request(
                "post",
                "/v1/billing_portal/configurations/{configuration}".format(
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
        Updates a configuration that describes the functionality of the customer portal.
        """
        return cast(
            "Configuration",
            await self._request_async(
                "post",
                "/v1/billing_portal/configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
