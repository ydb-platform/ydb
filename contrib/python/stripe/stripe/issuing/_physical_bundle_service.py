# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._physical_bundle import PhysicalBundle
    from stripe.params.issuing._physical_bundle_list_params import (
        PhysicalBundleListParams,
    )
    from stripe.params.issuing._physical_bundle_retrieve_params import (
        PhysicalBundleRetrieveParams,
    )


class PhysicalBundleService(StripeService):
    def list(
        self,
        params: Optional["PhysicalBundleListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PhysicalBundle]":
        """
        Returns a list of physical bundle objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[PhysicalBundle]",
            self._request(
                "get",
                "/v1/issuing/physical_bundles",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PhysicalBundleListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PhysicalBundle]":
        """
        Returns a list of physical bundle objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[PhysicalBundle]",
            await self._request_async(
                "get",
                "/v1/issuing/physical_bundles",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        physical_bundle: str,
        params: Optional["PhysicalBundleRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PhysicalBundle":
        """
        Retrieves a physical bundle object.
        """
        return cast(
            "PhysicalBundle",
            self._request(
                "get",
                "/v1/issuing/physical_bundles/{physical_bundle}".format(
                    physical_bundle=sanitize_id(physical_bundle),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        physical_bundle: str,
        params: Optional["PhysicalBundleRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PhysicalBundle":
        """
        Retrieves a physical bundle object.
        """
        return cast(
            "PhysicalBundle",
            await self._request_async(
                "get",
                "/v1/issuing/physical_bundles/{physical_bundle}".format(
                    physical_bundle=sanitize_id(physical_bundle),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
