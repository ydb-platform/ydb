# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._personalization_design import PersonalizationDesign
    from stripe.params.issuing._personalization_design_create_params import (
        PersonalizationDesignCreateParams,
    )
    from stripe.params.issuing._personalization_design_list_params import (
        PersonalizationDesignListParams,
    )
    from stripe.params.issuing._personalization_design_retrieve_params import (
        PersonalizationDesignRetrieveParams,
    )
    from stripe.params.issuing._personalization_design_update_params import (
        PersonalizationDesignUpdateParams,
    )


class PersonalizationDesignService(StripeService):
    def list(
        self,
        params: Optional["PersonalizationDesignListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PersonalizationDesign]":
        """
        Returns a list of personalization design objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[PersonalizationDesign]",
            self._request(
                "get",
                "/v1/issuing/personalization_designs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PersonalizationDesignListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PersonalizationDesign]":
        """
        Returns a list of personalization design objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[PersonalizationDesign]",
            await self._request_async(
                "get",
                "/v1/issuing/personalization_designs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PersonalizationDesignCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Creates a personalization design object.
        """
        return cast(
            "PersonalizationDesign",
            self._request(
                "post",
                "/v1/issuing/personalization_designs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PersonalizationDesignCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Creates a personalization design object.
        """
        return cast(
            "PersonalizationDesign",
            await self._request_async(
                "post",
                "/v1/issuing/personalization_designs",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Retrieves a personalization design object.
        """
        return cast(
            "PersonalizationDesign",
            self._request(
                "get",
                "/v1/issuing/personalization_designs/{personalization_design}".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Retrieves a personalization design object.
        """
        return cast(
            "PersonalizationDesign",
            await self._request_async(
                "get",
                "/v1/issuing/personalization_designs/{personalization_design}".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates a card personalization object.
        """
        return cast(
            "PersonalizationDesign",
            self._request(
                "post",
                "/v1/issuing/personalization_designs/{personalization_design}".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates a card personalization object.
        """
        return cast(
            "PersonalizationDesign",
            await self._request_async(
                "post",
                "/v1/issuing/personalization_designs/{personalization_design}".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
