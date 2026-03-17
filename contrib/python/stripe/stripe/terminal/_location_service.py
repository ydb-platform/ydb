# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.terminal._location_create_params import (
        LocationCreateParams,
    )
    from stripe.params.terminal._location_delete_params import (
        LocationDeleteParams,
    )
    from stripe.params.terminal._location_list_params import LocationListParams
    from stripe.params.terminal._location_retrieve_params import (
        LocationRetrieveParams,
    )
    from stripe.params.terminal._location_update_params import (
        LocationUpdateParams,
    )
    from stripe.terminal._location import Location


class LocationService(StripeService):
    def delete(
        self,
        location: str,
        params: Optional["LocationDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Deletes a Location object.
        """
        return cast(
            "Location",
            self._request(
                "delete",
                "/v1/terminal/locations/{location}".format(
                    location=sanitize_id(location),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        location: str,
        params: Optional["LocationDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Deletes a Location object.
        """
        return cast(
            "Location",
            await self._request_async(
                "delete",
                "/v1/terminal/locations/{location}".format(
                    location=sanitize_id(location),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        location: str,
        params: Optional["LocationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Retrieves a Location object.
        """
        return cast(
            "Location",
            self._request(
                "get",
                "/v1/terminal/locations/{location}".format(
                    location=sanitize_id(location),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        location: str,
        params: Optional["LocationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Retrieves a Location object.
        """
        return cast(
            "Location",
            await self._request_async(
                "get",
                "/v1/terminal/locations/{location}".format(
                    location=sanitize_id(location),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        location: str,
        params: Optional["LocationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Updates a Location object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Location",
            self._request(
                "post",
                "/v1/terminal/locations/{location}".format(
                    location=sanitize_id(location),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        location: str,
        params: Optional["LocationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Updates a Location object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Location",
            await self._request_async(
                "post",
                "/v1/terminal/locations/{location}".format(
                    location=sanitize_id(location),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["LocationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Location]":
        """
        Returns a list of Location objects.
        """
        return cast(
            "ListObject[Location]",
            self._request(
                "get",
                "/v1/terminal/locations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["LocationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Location]":
        """
        Returns a list of Location objects.
        """
        return cast(
            "ListObject[Location]",
            await self._request_async(
                "get",
                "/v1/terminal/locations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["LocationCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Creates a new Location object.
        For further details, including which address fields are required in each country, see the [Manage locations](https://docs.stripe.com/docs/terminal/fleet/locations) guide.
        """
        return cast(
            "Location",
            self._request(
                "post",
                "/v1/terminal/locations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["LocationCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Location":
        """
        Creates a new Location object.
        For further details, including which address fields are required in each country, see the [Manage locations](https://docs.stripe.com/docs/terminal/fleet/locations) guide.
        """
        return cast(
            "Location",
            await self._request_async(
                "post",
                "/v1/terminal/locations",
                base_address="api",
                params=params,
                options=options,
            ),
        )
