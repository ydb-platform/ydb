# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._mandate import Mandate
    from stripe._request_options import RequestOptions
    from stripe.params._mandate_retrieve_params import MandateRetrieveParams


class MandateService(StripeService):
    def retrieve(
        self,
        mandate: str,
        params: Optional["MandateRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Mandate":
        """
        Retrieves a Mandate object.
        """
        return cast(
            "Mandate",
            self._request(
                "get",
                "/v1/mandates/{mandate}".format(mandate=sanitize_id(mandate)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        mandate: str,
        params: Optional["MandateRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Mandate":
        """
        Retrieves a Mandate object.
        """
        return cast(
            "Mandate",
            await self._request_async(
                "get",
                "/v1/mandates/{mandate}".format(mandate=sanitize_id(mandate)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
