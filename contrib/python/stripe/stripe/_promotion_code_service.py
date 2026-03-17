# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._promotion_code import PromotionCode
    from stripe._request_options import RequestOptions
    from stripe.params._promotion_code_create_params import (
        PromotionCodeCreateParams,
    )
    from stripe.params._promotion_code_list_params import (
        PromotionCodeListParams,
    )
    from stripe.params._promotion_code_retrieve_params import (
        PromotionCodeRetrieveParams,
    )
    from stripe.params._promotion_code_update_params import (
        PromotionCodeUpdateParams,
    )


class PromotionCodeService(StripeService):
    def list(
        self,
        params: Optional["PromotionCodeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PromotionCode]":
        """
        Returns a list of your promotion codes.
        """
        return cast(
            "ListObject[PromotionCode]",
            self._request(
                "get",
                "/v1/promotion_codes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PromotionCodeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PromotionCode]":
        """
        Returns a list of your promotion codes.
        """
        return cast(
            "ListObject[PromotionCode]",
            await self._request_async(
                "get",
                "/v1/promotion_codes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PromotionCodeCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PromotionCode":
        """
        A promotion code points to an underlying promotion. You can optionally restrict the code to a specific customer, redemption limit, and expiration date.
        """
        return cast(
            "PromotionCode",
            self._request(
                "post",
                "/v1/promotion_codes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PromotionCodeCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PromotionCode":
        """
        A promotion code points to an underlying promotion. You can optionally restrict the code to a specific customer, redemption limit, and expiration date.
        """
        return cast(
            "PromotionCode",
            await self._request_async(
                "post",
                "/v1/promotion_codes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        promotion_code: str,
        params: Optional["PromotionCodeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PromotionCode":
        """
        Retrieves the promotion code with the given ID. In order to retrieve a promotion code by the customer-facing code use [list](https://docs.stripe.com/docs/api/promotion_codes/list) with the desired code.
        """
        return cast(
            "PromotionCode",
            self._request(
                "get",
                "/v1/promotion_codes/{promotion_code}".format(
                    promotion_code=sanitize_id(promotion_code),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        promotion_code: str,
        params: Optional["PromotionCodeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PromotionCode":
        """
        Retrieves the promotion code with the given ID. In order to retrieve a promotion code by the customer-facing code use [list](https://docs.stripe.com/docs/api/promotion_codes/list) with the desired code.
        """
        return cast(
            "PromotionCode",
            await self._request_async(
                "get",
                "/v1/promotion_codes/{promotion_code}".format(
                    promotion_code=sanitize_id(promotion_code),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        promotion_code: str,
        params: Optional["PromotionCodeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PromotionCode":
        """
        Updates the specified promotion code by setting the values of the parameters passed. Most fields are, by design, not editable.
        """
        return cast(
            "PromotionCode",
            self._request(
                "post",
                "/v1/promotion_codes/{promotion_code}".format(
                    promotion_code=sanitize_id(promotion_code),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        promotion_code: str,
        params: Optional["PromotionCodeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PromotionCode":
        """
        Updates the specified promotion code by setting the values of the parameters passed. Most fields are, by design, not editable.
        """
        return cast(
            "PromotionCode",
            await self._request_async(
                "post",
                "/v1/promotion_codes/{promotion_code}".format(
                    promotion_code=sanitize_id(promotion_code),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
