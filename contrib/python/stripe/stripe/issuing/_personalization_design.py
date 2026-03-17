# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._file import File
    from stripe.issuing._physical_bundle import PhysicalBundle
    from stripe.params.issuing._personalization_design_activate_params import (
        PersonalizationDesignActivateParams,
    )
    from stripe.params.issuing._personalization_design_create_params import (
        PersonalizationDesignCreateParams,
    )
    from stripe.params.issuing._personalization_design_deactivate_params import (
        PersonalizationDesignDeactivateParams,
    )
    from stripe.params.issuing._personalization_design_list_params import (
        PersonalizationDesignListParams,
    )
    from stripe.params.issuing._personalization_design_modify_params import (
        PersonalizationDesignModifyParams,
    )
    from stripe.params.issuing._personalization_design_reject_params import (
        PersonalizationDesignRejectParams,
    )
    from stripe.params.issuing._personalization_design_retrieve_params import (
        PersonalizationDesignRetrieveParams,
    )


class PersonalizationDesign(
    CreateableAPIResource["PersonalizationDesign"],
    ListableAPIResource["PersonalizationDesign"],
    UpdateableAPIResource["PersonalizationDesign"],
):
    """
    A Personalization Design is a logical grouping of a Physical Bundle, card logo, and carrier text that represents a product line.
    """

    OBJECT_NAME: ClassVar[Literal["issuing.personalization_design"]] = (
        "issuing.personalization_design"
    )

    class CarrierText(StripeObject):
        footer_body: Optional[str]
        """
        The footer body text of the carrier letter.
        """
        footer_title: Optional[str]
        """
        The footer title text of the carrier letter.
        """
        header_body: Optional[str]
        """
        The header body text of the carrier letter.
        """
        header_title: Optional[str]
        """
        The header title text of the carrier letter.
        """

    class Preferences(StripeObject):
        is_default: bool
        """
        Whether we use this personalization design to create cards when one isn't specified. A connected account uses the Connect platform's default design if no personalization design is set as the default design.
        """
        is_platform_default: Optional[bool]
        """
        Whether this personalization design is used to create cards when one is not specified and a default for this connected account does not exist.
        """

    class RejectionReasons(StripeObject):
        card_logo: Optional[
            List[
                Literal[
                    "geographic_location",
                    "inappropriate",
                    "network_name",
                    "non_binary_image",
                    "non_fiat_currency",
                    "other",
                    "other_entity",
                    "promotional_material",
                ]
            ]
        ]
        """
        The reason(s) the card logo was rejected.
        """
        carrier_text: Optional[
            List[
                Literal[
                    "geographic_location",
                    "inappropriate",
                    "network_name",
                    "non_fiat_currency",
                    "other",
                    "other_entity",
                    "promotional_material",
                ]
            ]
        ]
        """
        The reason(s) the carrier text was rejected.
        """

    card_logo: Optional[ExpandableField["File"]]
    """
    The file for the card logo to use with physical bundles that support card logos. Must have a `purpose` value of `issuing_logo`.
    """
    carrier_text: Optional[CarrierText]
    """
    Hash containing carrier text, for use with physical bundles that support carrier text.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    lookup_key: Optional[str]
    """
    A lookup key used to retrieve personalization designs dynamically from a static string. This may be up to 200 characters.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: Optional[str]
    """
    Friendly display name.
    """
    object: Literal["issuing.personalization_design"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    physical_bundle: ExpandableField["PhysicalBundle"]
    """
    The physical bundle object belonging to this personalization design.
    """
    preferences: Preferences
    rejection_reasons: RejectionReasons
    status: Literal["active", "inactive", "rejected", "review"]
    """
    Whether this personalization design can be used to create cards.
    """

    @classmethod
    def create(
        cls, **params: Unpack["PersonalizationDesignCreateParams"]
    ) -> "PersonalizationDesign":
        """
        Creates a personalization design object.
        """
        return cast(
            "PersonalizationDesign",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PersonalizationDesignCreateParams"]
    ) -> "PersonalizationDesign":
        """
        Creates a personalization design object.
        """
        return cast(
            "PersonalizationDesign",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["PersonalizationDesignListParams"]
    ) -> ListObject["PersonalizationDesign"]:
        """
        Returns a list of personalization design objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["PersonalizationDesignListParams"]
    ) -> ListObject["PersonalizationDesign"]:
        """
        Returns a list of personalization design objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def modify(
        cls, id: str, **params: Unpack["PersonalizationDesignModifyParams"]
    ) -> "PersonalizationDesign":
        """
        Updates a card personalization object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PersonalizationDesign",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PersonalizationDesignModifyParams"]
    ) -> "PersonalizationDesign":
        """
        Updates a card personalization object.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PersonalizationDesign",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PersonalizationDesignRetrieveParams"]
    ) -> "PersonalizationDesign":
        """
        Retrieves a personalization design object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PersonalizationDesignRetrieveParams"]
    ) -> "PersonalizationDesign":
        """
        Retrieves a personalization design object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["PersonalizationDesign"]):
        _resource_cls: Type["PersonalizationDesign"]

        @classmethod
        def _cls_activate(
            cls,
            personalization_design: str,
            **params: Unpack["PersonalizationDesignActivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            return cast(
                "PersonalizationDesign",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/activate".format(
                        personalization_design=sanitize_id(
                            personalization_design
                        )
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def activate(
            personalization_design: str,
            **params: Unpack["PersonalizationDesignActivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            ...

        @overload
        def activate(
            self, **params: Unpack["PersonalizationDesignActivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            ...

        @class_method_variant("_cls_activate")
        def activate(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["PersonalizationDesignActivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            return cast(
                "PersonalizationDesign",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/activate".format(
                        personalization_design=sanitize_id(
                            self.resource.get("id")
                        )
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_activate_async(
            cls,
            personalization_design: str,
            **params: Unpack["PersonalizationDesignActivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            return cast(
                "PersonalizationDesign",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/activate".format(
                        personalization_design=sanitize_id(
                            personalization_design
                        )
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def activate_async(
            personalization_design: str,
            **params: Unpack["PersonalizationDesignActivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            ...

        @overload
        async def activate_async(
            self, **params: Unpack["PersonalizationDesignActivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            ...

        @class_method_variant("_cls_activate_async")
        async def activate_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["PersonalizationDesignActivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to active.
            """
            return cast(
                "PersonalizationDesign",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/activate".format(
                        personalization_design=sanitize_id(
                            self.resource.get("id")
                        )
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_deactivate(
            cls,
            personalization_design: str,
            **params: Unpack["PersonalizationDesignDeactivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            return cast(
                "PersonalizationDesign",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/deactivate".format(
                        personalization_design=sanitize_id(
                            personalization_design
                        )
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def deactivate(
            personalization_design: str,
            **params: Unpack["PersonalizationDesignDeactivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            ...

        @overload
        def deactivate(
            self, **params: Unpack["PersonalizationDesignDeactivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            ...

        @class_method_variant("_cls_deactivate")
        def deactivate(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["PersonalizationDesignDeactivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            return cast(
                "PersonalizationDesign",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/deactivate".format(
                        personalization_design=sanitize_id(
                            self.resource.get("id")
                        )
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_deactivate_async(
            cls,
            personalization_design: str,
            **params: Unpack["PersonalizationDesignDeactivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            return cast(
                "PersonalizationDesign",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/deactivate".format(
                        personalization_design=sanitize_id(
                            personalization_design
                        )
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def deactivate_async(
            personalization_design: str,
            **params: Unpack["PersonalizationDesignDeactivateParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            ...

        @overload
        async def deactivate_async(
            self, **params: Unpack["PersonalizationDesignDeactivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            ...

        @class_method_variant("_cls_deactivate_async")
        async def deactivate_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["PersonalizationDesignDeactivateParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to inactive.
            """
            return cast(
                "PersonalizationDesign",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/deactivate".format(
                        personalization_design=sanitize_id(
                            self.resource.get("id")
                        )
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_reject(
            cls,
            personalization_design: str,
            **params: Unpack["PersonalizationDesignRejectParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            return cast(
                "PersonalizationDesign",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/reject".format(
                        personalization_design=sanitize_id(
                            personalization_design
                        )
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def reject(
            personalization_design: str,
            **params: Unpack["PersonalizationDesignRejectParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            ...

        @overload
        def reject(
            self, **params: Unpack["PersonalizationDesignRejectParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            ...

        @class_method_variant("_cls_reject")
        def reject(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["PersonalizationDesignRejectParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            return cast(
                "PersonalizationDesign",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/reject".format(
                        personalization_design=sanitize_id(
                            self.resource.get("id")
                        )
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_reject_async(
            cls,
            personalization_design: str,
            **params: Unpack["PersonalizationDesignRejectParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            return cast(
                "PersonalizationDesign",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/reject".format(
                        personalization_design=sanitize_id(
                            personalization_design
                        )
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def reject_async(
            personalization_design: str,
            **params: Unpack["PersonalizationDesignRejectParams"],
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            ...

        @overload
        async def reject_async(
            self, **params: Unpack["PersonalizationDesignRejectParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            ...

        @class_method_variant("_cls_reject_async")
        async def reject_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["PersonalizationDesignRejectParams"]
        ) -> "PersonalizationDesign":
            """
            Updates the status of the specified testmode personalization design object to rejected.
            """
            return cast(
                "PersonalizationDesign",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/personalization_designs/{personalization_design}/reject".format(
                        personalization_design=sanitize_id(
                            self.resource.get("id")
                        )
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "carrier_text": CarrierText,
        "preferences": Preferences,
        "rejection_reasons": RejectionReasons,
    }


PersonalizationDesign.TestHelpers._resource_cls = PersonalizationDesign
