# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.issuing._personalization_design import PersonalizationDesign
    from stripe.params.test_helpers.issuing._personalization_design_activate_params import (
        PersonalizationDesignActivateParams,
    )
    from stripe.params.test_helpers.issuing._personalization_design_deactivate_params import (
        PersonalizationDesignDeactivateParams,
    )
    from stripe.params.test_helpers.issuing._personalization_design_reject_params import (
        PersonalizationDesignRejectParams,
    )


class PersonalizationDesignService(StripeService):
    def activate(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignActivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates the status of the specified testmode personalization design object to active.
        """
        return cast(
            "PersonalizationDesign",
            self._request(
                "post",
                "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/activate".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def activate_async(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignActivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates the status of the specified testmode personalization design object to active.
        """
        return cast(
            "PersonalizationDesign",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/activate".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def deactivate(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignDeactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates the status of the specified testmode personalization design object to inactive.
        """
        return cast(
            "PersonalizationDesign",
            self._request(
                "post",
                "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/deactivate".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def deactivate_async(
        self,
        personalization_design: str,
        params: Optional["PersonalizationDesignDeactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates the status of the specified testmode personalization design object to inactive.
        """
        return cast(
            "PersonalizationDesign",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/deactivate".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def reject(
        self,
        personalization_design: str,
        params: "PersonalizationDesignRejectParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates the status of the specified testmode personalization design object to rejected.
        """
        return cast(
            "PersonalizationDesign",
            self._request(
                "post",
                "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/reject".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def reject_async(
        self,
        personalization_design: str,
        params: "PersonalizationDesignRejectParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PersonalizationDesign":
        """
        Updates the status of the specified testmode personalization design object to rejected.
        """
        return cast(
            "PersonalizationDesign",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/reject".format(
                    personalization_design=sanitize_id(personalization_design),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
