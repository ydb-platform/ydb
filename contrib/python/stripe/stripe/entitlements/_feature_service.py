# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.entitlements._feature import Feature
    from stripe.params.entitlements._feature_create_params import (
        FeatureCreateParams,
    )
    from stripe.params.entitlements._feature_list_params import (
        FeatureListParams,
    )
    from stripe.params.entitlements._feature_retrieve_params import (
        FeatureRetrieveParams,
    )
    from stripe.params.entitlements._feature_update_params import (
        FeatureUpdateParams,
    )


class FeatureService(StripeService):
    def list(
        self,
        params: Optional["FeatureListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Feature]":
        """
        Retrieve a list of features
        """
        return cast(
            "ListObject[Feature]",
            self._request(
                "get",
                "/v1/entitlements/features",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["FeatureListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Feature]":
        """
        Retrieve a list of features
        """
        return cast(
            "ListObject[Feature]",
            await self._request_async(
                "get",
                "/v1/entitlements/features",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "FeatureCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Feature":
        """
        Creates a feature
        """
        return cast(
            "Feature",
            self._request(
                "post",
                "/v1/entitlements/features",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "FeatureCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Feature":
        """
        Creates a feature
        """
        return cast(
            "Feature",
            await self._request_async(
                "post",
                "/v1/entitlements/features",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["FeatureRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Feature":
        """
        Retrieves a feature
        """
        return cast(
            "Feature",
            self._request(
                "get",
                "/v1/entitlements/features/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["FeatureRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Feature":
        """
        Retrieves a feature
        """
        return cast(
            "Feature",
            await self._request_async(
                "get",
                "/v1/entitlements/features/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        id: str,
        params: Optional["FeatureUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Feature":
        """
        Update a feature's metadata or permanently deactivate it.
        """
        return cast(
            "Feature",
            self._request(
                "post",
                "/v1/entitlements/features/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: Optional["FeatureUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Feature":
        """
        Update a feature's metadata or permanently deactivate it.
        """
        return cast(
            "Feature",
            await self._request_async(
                "post",
                "/v1/entitlements/features/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
