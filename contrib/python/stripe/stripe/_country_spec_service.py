# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._country_spec import CountrySpec
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._country_spec_list_params import CountrySpecListParams
    from stripe.params._country_spec_retrieve_params import (
        CountrySpecRetrieveParams,
    )


class CountrySpecService(StripeService):
    def list(
        self,
        params: Optional["CountrySpecListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CountrySpec]":
        """
        Lists all Country Spec objects available in the API.
        """
        return cast(
            "ListObject[CountrySpec]",
            self._request(
                "get",
                "/v1/country_specs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CountrySpecListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CountrySpec]":
        """
        Lists all Country Spec objects available in the API.
        """
        return cast(
            "ListObject[CountrySpec]",
            await self._request_async(
                "get",
                "/v1/country_specs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        country: str,
        params: Optional["CountrySpecRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CountrySpec":
        """
        Returns a Country Spec for a given Country code.
        """
        return cast(
            "CountrySpec",
            self._request(
                "get",
                "/v1/country_specs/{country}".format(
                    country=sanitize_id(country),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        country: str,
        params: Optional["CountrySpecRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CountrySpec":
        """
        Returns a Country Spec for a given Country code.
        """
        return cast(
            "CountrySpec",
            await self._request_async(
                "get",
                "/v1/country_specs/{country}".format(
                    country=sanitize_id(country),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
