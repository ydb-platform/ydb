# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._review import Review
    from stripe.params._review_approve_params import ReviewApproveParams
    from stripe.params._review_list_params import ReviewListParams
    from stripe.params._review_retrieve_params import ReviewRetrieveParams


class ReviewService(StripeService):
    def list(
        self,
        params: Optional["ReviewListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Review]":
        """
        Returns a list of Review objects that have open set to true. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Review]",
            self._request(
                "get",
                "/v1/reviews",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ReviewListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Review]":
        """
        Returns a list of Review objects that have open set to true. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Review]",
            await self._request_async(
                "get",
                "/v1/reviews",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        review: str,
        params: Optional["ReviewRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Review":
        """
        Retrieves a Review object.
        """
        return cast(
            "Review",
            self._request(
                "get",
                "/v1/reviews/{review}".format(review=sanitize_id(review)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        review: str,
        params: Optional["ReviewRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Review":
        """
        Retrieves a Review object.
        """
        return cast(
            "Review",
            await self._request_async(
                "get",
                "/v1/reviews/{review}".format(review=sanitize_id(review)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def approve(
        self,
        review: str,
        params: Optional["ReviewApproveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        return cast(
            "Review",
            self._request(
                "post",
                "/v1/reviews/{review}/approve".format(
                    review=sanitize_id(review),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def approve_async(
        self,
        review: str,
        params: Optional["ReviewApproveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Review":
        """
        Approves a Review object, closing it and removing it from the list of reviews.
        """
        return cast(
            "Review",
            await self._request_async(
                "post",
                "/v1/reviews/{review}/approve".format(
                    review=sanitize_id(review),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
