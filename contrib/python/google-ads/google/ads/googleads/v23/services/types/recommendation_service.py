# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations

from typing import MutableSequence

import proto  # type: ignore

from google.ads.googleads.v23.common.types import criteria
from google.ads.googleads.v23.common.types import extensions
from google.ads.googleads.v23.enums.types import (
    ad_group_type as gage_ad_group_type,
)
from google.ads.googleads.v23.enums.types import (
    advertising_channel_type as gage_advertising_channel_type,
)
from google.ads.googleads.v23.enums.types import (
    bidding_strategy_type as gage_bidding_strategy_type,
)
from google.ads.googleads.v23.enums.types import conversion_tracking_status_enum
from google.ads.googleads.v23.enums.types import keyword_match_type
from google.ads.googleads.v23.enums.types import recommendation_type
from google.ads.googleads.v23.enums.types import (
    target_impression_share_location,
)
from google.ads.googleads.v23.resources.types import ad as gagr_ad
from google.ads.googleads.v23.resources.types import asset
from google.ads.googleads.v23.resources.types import recommendation
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v23.services",
    marshal="google.ads.googleads.v23",
    manifest={
        "ApplyRecommendationRequest",
        "ApplyRecommendationOperation",
        "ApplyRecommendationResponse",
        "ApplyRecommendationResult",
        "DismissRecommendationRequest",
        "DismissRecommendationResponse",
        "GenerateRecommendationsRequest",
        "GenerateRecommendationsResponse",
    },
)


class ApplyRecommendationRequest(proto.Message):
    r"""Request message for
    [RecommendationService.ApplyRecommendation][google.ads.googleads.v23.services.RecommendationService.ApplyRecommendation].

    Attributes:
        customer_id (str):
            Required. The ID of the customer with the
            recommendation.
        operations (MutableSequence[google.ads.googleads.v23.services.types.ApplyRecommendationOperation]):
            Required. The list of operations to apply recommendations.
            If partial_failure=false all recommendations should be of
            the same type There is a limit of 100 operations per
            request.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, operations will be carried out
            as a transaction if and only if they are all
            valid. Default is false.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    operations: MutableSequence["ApplyRecommendationOperation"] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message="ApplyRecommendationOperation",
        )
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=3,
    )


class ApplyRecommendationOperation(proto.Message):
    r"""Information about the operation to apply a recommendation and
    any parameters to customize it.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            The resource name of the recommendation to
            apply.
        campaign_budget (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.CampaignBudgetParameters):
            Optional parameters to use when applying a
            campaign budget recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        text_ad (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.TextAdParameters):
            Optional parameters to use when applying a
            text ad recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        keyword (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.KeywordParameters):
            Optional parameters to use when applying
            keyword recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        target_cpa_opt_in (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.TargetCpaOptInParameters):
            Optional parameters to use when applying
            target CPA opt-in recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        target_roas_opt_in (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.TargetRoasOptInParameters):
            Optional parameters to use when applying
            target ROAS opt-in recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        callout_extension (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.CalloutExtensionParameters):
            Parameters to use when applying callout
            extension recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        call_extension (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.CallExtensionParameters):
            Parameters to use when applying call
            extension recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        sitelink_extension (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.SitelinkExtensionParameters):
            Parameters to use when applying sitelink
            recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        move_unused_budget (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.MoveUnusedBudgetParameters):
            Parameters to use when applying move unused
            budget recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        responsive_search_ad (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.ResponsiveSearchAdParameters):
            Parameters to use when applying a responsive
            search ad recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        use_broad_match_keyword (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.UseBroadMatchKeywordParameters):
            Parameters to use when applying a use broad
            match keyword recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        responsive_search_ad_asset (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.ResponsiveSearchAdAssetParameters):
            Parameters to use when applying a responsive
            search ad asset recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        responsive_search_ad_improve_ad_strength (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.ResponsiveSearchAdImproveAdStrengthParameters):
            Parameters to use when applying a responsive
            search ad improve ad strength recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        raise_target_cpa_bid_too_low (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.RaiseTargetCpaBidTooLowParameters):
            Parameters to use when applying a raise
            target CPA bid too low recommendation. The apply
            is asynchronous and can take minutes depending
            on the number of ad groups there is in the
            related campaign.

            This field is a member of `oneof`_ ``apply_parameters``.
        forecasting_set_target_roas (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.ForecastingSetTargetRoasParameters):
            Parameters to use when applying a forecasting
            set target ROAS recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        callout_asset (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.CalloutAssetParameters):
            Parameters to use when applying callout asset
            recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        call_asset (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.CallAssetParameters):
            Parameters to use when applying call asset
            recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        sitelink_asset (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.SitelinkAssetParameters):
            Parameters to use when applying sitelink
            asset recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        raise_target_cpa (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.RaiseTargetCpaParameters):
            Parameters to use when applying raise Target
            CPA recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        lower_target_roas (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.LowerTargetRoasParameters):
            Parameters to use when applying lower Target
            ROAS recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        forecasting_set_target_cpa (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.ForecastingSetTargetCpaParameters):
            Parameters to use when applying forecasting
            set target CPA recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        set_target_cpa (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.ForecastingSetTargetCpaParameters):
            Parameters to use when applying set target
            CPA recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        set_target_roas (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.ForecastingSetTargetRoasParameters):
            Parameters to use when applying set target
            ROAS recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
        lead_form_asset (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.LeadFormAssetParameters):
            Parameters to use when applying lead form
            asset recommendation.

            This field is a member of `oneof`_ ``apply_parameters``.
    """

    class CampaignBudgetParameters(proto.Message):
        r"""Parameters to use when applying a campaign budget
        recommendation.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            new_budget_amount_micros (int):
                New budget amount to set for target budget
                resource. This is a required field.

                This field is a member of `oneof`_ ``_new_budget_amount_micros``.
        """

        new_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class ForecastingSetTargetRoasParameters(proto.Message):
        r"""Parameters to use when applying a forecasting set target roas
        recommendation.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            target_roas (float):
                New target ROAS (revenue per unit of spend)
                to set for a campaign resource.
                The value is between 0.01 and 1000.0, inclusive.

                This field is a member of `oneof`_ ``_target_roas``.
            campaign_budget_amount_micros (int):
                New campaign budget amount to set for a
                campaign resource.

                This field is a member of `oneof`_ ``_campaign_budget_amount_micros``.
        """

        target_roas: float = proto.Field(
            proto.DOUBLE,
            number=1,
            optional=True,
        )
        campaign_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class TextAdParameters(proto.Message):
        r"""Parameters to use when applying a text ad recommendation.

        Attributes:
            ad (google.ads.googleads.v23.resources.types.Ad):
                New ad to add to recommended ad group. All
                necessary fields need to be set in this message.
                This is a required field.
        """

        ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=1,
            message=gagr_ad.Ad,
        )

    class KeywordParameters(proto.Message):
        r"""Parameters to use when applying keyword recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            ad_group (str):
                The ad group resource to add keyword to. This
                is a required field.

                This field is a member of `oneof`_ ``_ad_group``.
            match_type (google.ads.googleads.v23.enums.types.KeywordMatchTypeEnum.KeywordMatchType):
                The match type of the keyword. This is a
                required field.
            cpc_bid_micros (int):
                Optional, CPC bid to set for the keyword. If
                not set, keyword will use bid based on bidding
                strategy used by target ad group.

                This field is a member of `oneof`_ ``_cpc_bid_micros``.
        """

        ad_group: str = proto.Field(
            proto.STRING,
            number=4,
            optional=True,
        )
        match_type: keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType = (
            proto.Field(
                proto.ENUM,
                number=2,
                enum=keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType,
            )
        )
        cpc_bid_micros: int = proto.Field(
            proto.INT64,
            number=5,
            optional=True,
        )

    class TargetCpaOptInParameters(proto.Message):
        r"""Parameters to use when applying Target CPA recommendation.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            target_cpa_micros (int):
                Average CPA to use for Target CPA bidding
                strategy. This is a required field.

                This field is a member of `oneof`_ ``_target_cpa_micros``.
            new_campaign_budget_amount_micros (int):
                Optional, budget amount to set for the
                campaign.

                This field is a member of `oneof`_ ``_new_campaign_budget_amount_micros``.
        """

        target_cpa_micros: int = proto.Field(
            proto.INT64,
            number=3,
            optional=True,
        )
        new_campaign_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=4,
            optional=True,
        )

    class TargetRoasOptInParameters(proto.Message):
        r"""Parameters to use when applying a Target ROAS opt-in
        recommendation.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            target_roas (float):
                Average ROAS (revenue per unit of spend) to use for Target
                ROAS bidding strategy. The value is between 0.01 and 1000.0,
                inclusive. This is a required field, unless
                new_campaign_budget_amount_micros is set.

                This field is a member of `oneof`_ ``_target_roas``.
            new_campaign_budget_amount_micros (int):
                Optional, budget amount to set for the
                campaign.

                This field is a member of `oneof`_ ``_new_campaign_budget_amount_micros``.
        """

        target_roas: float = proto.Field(
            proto.DOUBLE,
            number=1,
            optional=True,
        )
        new_campaign_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class CalloutExtensionParameters(proto.Message):
        r"""Parameters to use when applying callout extension
        recommendation.

        Attributes:
            callout_extensions (MutableSequence[google.ads.googleads.v23.common.types.CalloutFeedItem]):
                Callout extensions to be added. This is a
                required field.
        """

        callout_extensions: MutableSequence[extensions.CalloutFeedItem] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message=extensions.CalloutFeedItem,
            )
        )

    class CallExtensionParameters(proto.Message):
        r"""Parameters to use when applying call extension
        recommendation.

        Attributes:
            call_extensions (MutableSequence[google.ads.googleads.v23.common.types.CallFeedItem]):
                Call extensions to be added. This is a
                required field.
        """

        call_extensions: MutableSequence[extensions.CallFeedItem] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message=extensions.CallFeedItem,
            )
        )

    class SitelinkExtensionParameters(proto.Message):
        r"""Parameters to use when applying sitelink recommendation.

        Attributes:
            sitelink_extensions (MutableSequence[google.ads.googleads.v23.common.types.SitelinkFeedItem]):
                Sitelinks to be added. This is a required
                field.
        """

        sitelink_extensions: MutableSequence[extensions.SitelinkFeedItem] = (
            proto.RepeatedField(
                proto.MESSAGE,
                number=1,
                message=extensions.SitelinkFeedItem,
            )
        )

    class CalloutAssetParameters(proto.Message):
        r"""Parameters to use when applying callout asset
        recommendations.

        Attributes:
            ad_asset_apply_parameters (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.AdAssetApplyParameters):
                Required. Callout assets to be added. This is
                a required field.
        """

        ad_asset_apply_parameters: (
            "ApplyRecommendationOperation.AdAssetApplyParameters"
        ) = proto.Field(
            proto.MESSAGE,
            number=1,
            message="ApplyRecommendationOperation.AdAssetApplyParameters",
        )

    class CallAssetParameters(proto.Message):
        r"""Parameters to use when applying call asset recommendations.

        Attributes:
            ad_asset_apply_parameters (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.AdAssetApplyParameters):
                Required. Call assets to be added. This is a
                required field.
        """

        ad_asset_apply_parameters: (
            "ApplyRecommendationOperation.AdAssetApplyParameters"
        ) = proto.Field(
            proto.MESSAGE,
            number=1,
            message="ApplyRecommendationOperation.AdAssetApplyParameters",
        )

    class SitelinkAssetParameters(proto.Message):
        r"""Parameters to use when applying sitelink asset
        recommendations.

        Attributes:
            ad_asset_apply_parameters (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.AdAssetApplyParameters):
                Required. Sitelink assets to be added. This
                is a required field.
        """

        ad_asset_apply_parameters: (
            "ApplyRecommendationOperation.AdAssetApplyParameters"
        ) = proto.Field(
            proto.MESSAGE,
            number=1,
            message="ApplyRecommendationOperation.AdAssetApplyParameters",
        )

    class RaiseTargetCpaParameters(proto.Message):
        r"""Parameters to use when applying raise Target CPA
        recommendations.

        Attributes:
            target_cpa_multiplier (float):
                Required. Target to set CPA multiplier to.
                This is a required field.
        """

        target_cpa_multiplier: float = proto.Field(
            proto.DOUBLE,
            number=1,
        )

    class LowerTargetRoasParameters(proto.Message):
        r"""Parameters to use when applying lower Target ROAS
        recommendations.

        Attributes:
            target_roas_multiplier (float):
                Required. Target to set ROAS multiplier to.
                This is a required field.
        """

        target_roas_multiplier: float = proto.Field(
            proto.DOUBLE,
            number=1,
        )

    class AdAssetApplyParameters(proto.Message):
        r"""Common parameters used when applying ad asset
        recommendations.

        Attributes:
            new_assets (MutableSequence[google.ads.googleads.v23.resources.types.Asset]):
                The assets to create and attach to a scope. This may be
                combined with existing_assets in the same call.
            existing_assets (MutableSequence[str]):
                The resource names of existing assets to attach to a scope.
                This may be combined with new_assets in the same call.
            scope (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.AdAssetApplyParameters.ApplyScope):
                Required. The scope at which to apply the
                assets. Assets at the campaign scope level will
                be applied to the campaign associated with the
                recommendation. Assets at the customer scope
                will apply to the entire account. Assets at the
                campaign scope will override any attached at the
                customer scope.
        """

        class ApplyScope(proto.Enum):
            r"""Scope to apply the assets to.

            Values:
                UNSPECIFIED (0):
                    The apply scope has not been specified.
                UNKNOWN (1):
                    Unknown.
                CUSTOMER (2):
                    Apply at the customer scope.
                CAMPAIGN (3):
                    Apply at the campaign scope.
            """

            UNSPECIFIED = 0
            UNKNOWN = 1
            CUSTOMER = 2
            CAMPAIGN = 3

        new_assets: MutableSequence[asset.Asset] = proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=asset.Asset,
        )
        existing_assets: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=2,
        )
        scope: (
            "ApplyRecommendationOperation.AdAssetApplyParameters.ApplyScope"
        ) = proto.Field(
            proto.ENUM,
            number=3,
            enum="ApplyRecommendationOperation.AdAssetApplyParameters.ApplyScope",
        )

    class MoveUnusedBudgetParameters(proto.Message):
        r"""Parameters to use when applying move unused budget
        recommendation.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            budget_micros_to_move (int):
                Budget amount to move from excess budget to
                constrained budget. This is a required field.

                This field is a member of `oneof`_ ``_budget_micros_to_move``.
        """

        budget_micros_to_move: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class ResponsiveSearchAdAssetParameters(proto.Message):
        r"""Parameters to use when applying a responsive search ad asset
        recommendation.

        Attributes:
            updated_ad (google.ads.googleads.v23.resources.types.Ad):
                Updated ad. The current ad's content will be
                replaced.
        """

        updated_ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=1,
            message=gagr_ad.Ad,
        )

    class ResponsiveSearchAdImproveAdStrengthParameters(proto.Message):
        r"""Parameters to use when applying a responsive search ad
        improve ad strength recommendation.

        Attributes:
            updated_ad (google.ads.googleads.v23.resources.types.Ad):
                Updated ad. The current ad's content will be
                replaced.
        """

        updated_ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=1,
            message=gagr_ad.Ad,
        )

    class ResponsiveSearchAdParameters(proto.Message):
        r"""Parameters to use when applying a responsive search ad
        recommendation.

        Attributes:
            ad (google.ads.googleads.v23.resources.types.Ad):
                Required. New ad to add to recommended ad
                group.
        """

        ad: gagr_ad.Ad = proto.Field(
            proto.MESSAGE,
            number=1,
            message=gagr_ad.Ad,
        )

    class RaiseTargetCpaBidTooLowParameters(proto.Message):
        r"""Parameters to use when applying a raise target CPA bid too
        low recommendation. The apply is asynchronous and can take
        minutes depending on the number of ad groups there is in the
        related campaign..

        Attributes:
            target_multiplier (float):
                Required. A number greater than 1.0
                indicating the factor by which to increase the
                target CPA. This is a required field.
        """

        target_multiplier: float = proto.Field(
            proto.DOUBLE,
            number=1,
        )

    class UseBroadMatchKeywordParameters(proto.Message):
        r"""Parameters to use when applying a use broad match keyword
        recommendation.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            new_budget_amount_micros (int):
                New budget amount to set for target budget
                resource.

                This field is a member of `oneof`_ ``_new_budget_amount_micros``.
        """

        new_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=1,
            optional=True,
        )

    class ForecastingSetTargetCpaParameters(proto.Message):
        r"""Parameters to use when applying a set target CPA
        recommendation.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            target_cpa_micros (int):
                Average CPA to use for Target CPA bidding
                strategy.

                This field is a member of `oneof`_ ``_target_cpa_micros``.
            campaign_budget_amount_micros (int):
                New campaign budget amount to set for a
                campaign resource.

                This field is a member of `oneof`_ ``_campaign_budget_amount_micros``.
        """

        target_cpa_micros: int = proto.Field(
            proto.INT64,
            number=1,
            optional=True,
        )
        campaign_budget_amount_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )

    class LeadFormAssetParameters(proto.Message):
        r"""Parameters to use when applying a lead form asset
        recommendation.


        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            ad_asset_apply_parameters (google.ads.googleads.v23.services.types.ApplyRecommendationOperation.AdAssetApplyParameters):
                Required. Lead form assets to be added. This
                is a required field.
            set_submit_lead_form_asset_campaign_goal (bool):
                If true, the "Submit Lead Form" goal will be
                set on the target campaign. As a result, ads
                will be shown as lead form creative ads. If
                false, the "Submit Lead Form" goal will not be
                set on the campaign and ads will contain lead
                form assets.

                This field is a member of `oneof`_ ``_set_submit_lead_form_asset_campaign_goal``.
        """

        ad_asset_apply_parameters: (
            "ApplyRecommendationOperation.AdAssetApplyParameters"
        ) = proto.Field(
            proto.MESSAGE,
            number=1,
            message="ApplyRecommendationOperation.AdAssetApplyParameters",
        )
        set_submit_lead_form_asset_campaign_goal: bool = proto.Field(
            proto.BOOL,
            number=2,
            optional=True,
        )

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )
    campaign_budget: CampaignBudgetParameters = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="apply_parameters",
        message=CampaignBudgetParameters,
    )
    text_ad: TextAdParameters = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="apply_parameters",
        message=TextAdParameters,
    )
    keyword: KeywordParameters = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="apply_parameters",
        message=KeywordParameters,
    )
    target_cpa_opt_in: TargetCpaOptInParameters = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="apply_parameters",
        message=TargetCpaOptInParameters,
    )
    target_roas_opt_in: TargetRoasOptInParameters = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="apply_parameters",
        message=TargetRoasOptInParameters,
    )
    callout_extension: CalloutExtensionParameters = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="apply_parameters",
        message=CalloutExtensionParameters,
    )
    call_extension: CallExtensionParameters = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="apply_parameters",
        message=CallExtensionParameters,
    )
    sitelink_extension: SitelinkExtensionParameters = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="apply_parameters",
        message=SitelinkExtensionParameters,
    )
    move_unused_budget: MoveUnusedBudgetParameters = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="apply_parameters",
        message=MoveUnusedBudgetParameters,
    )
    responsive_search_ad: ResponsiveSearchAdParameters = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="apply_parameters",
        message=ResponsiveSearchAdParameters,
    )
    use_broad_match_keyword: UseBroadMatchKeywordParameters = proto.Field(
        proto.MESSAGE,
        number=12,
        oneof="apply_parameters",
        message=UseBroadMatchKeywordParameters,
    )
    responsive_search_ad_asset: ResponsiveSearchAdAssetParameters = proto.Field(
        proto.MESSAGE,
        number=13,
        oneof="apply_parameters",
        message=ResponsiveSearchAdAssetParameters,
    )
    responsive_search_ad_improve_ad_strength: (
        ResponsiveSearchAdImproveAdStrengthParameters
    ) = proto.Field(
        proto.MESSAGE,
        number=14,
        oneof="apply_parameters",
        message=ResponsiveSearchAdImproveAdStrengthParameters,
    )
    raise_target_cpa_bid_too_low: RaiseTargetCpaBidTooLowParameters = (
        proto.Field(
            proto.MESSAGE,
            number=15,
            oneof="apply_parameters",
            message=RaiseTargetCpaBidTooLowParameters,
        )
    )
    forecasting_set_target_roas: ForecastingSetTargetRoasParameters = (
        proto.Field(
            proto.MESSAGE,
            number=16,
            oneof="apply_parameters",
            message=ForecastingSetTargetRoasParameters,
        )
    )
    callout_asset: CalloutAssetParameters = proto.Field(
        proto.MESSAGE,
        number=17,
        oneof="apply_parameters",
        message=CalloutAssetParameters,
    )
    call_asset: CallAssetParameters = proto.Field(
        proto.MESSAGE,
        number=18,
        oneof="apply_parameters",
        message=CallAssetParameters,
    )
    sitelink_asset: SitelinkAssetParameters = proto.Field(
        proto.MESSAGE,
        number=19,
        oneof="apply_parameters",
        message=SitelinkAssetParameters,
    )
    raise_target_cpa: RaiseTargetCpaParameters = proto.Field(
        proto.MESSAGE,
        number=20,
        oneof="apply_parameters",
        message=RaiseTargetCpaParameters,
    )
    lower_target_roas: LowerTargetRoasParameters = proto.Field(
        proto.MESSAGE,
        number=21,
        oneof="apply_parameters",
        message=LowerTargetRoasParameters,
    )
    forecasting_set_target_cpa: ForecastingSetTargetCpaParameters = proto.Field(
        proto.MESSAGE,
        number=22,
        oneof="apply_parameters",
        message=ForecastingSetTargetCpaParameters,
    )
    set_target_cpa: ForecastingSetTargetCpaParameters = proto.Field(
        proto.MESSAGE,
        number=23,
        oneof="apply_parameters",
        message=ForecastingSetTargetCpaParameters,
    )
    set_target_roas: ForecastingSetTargetRoasParameters = proto.Field(
        proto.MESSAGE,
        number=24,
        oneof="apply_parameters",
        message=ForecastingSetTargetRoasParameters,
    )
    lead_form_asset: LeadFormAssetParameters = proto.Field(
        proto.MESSAGE,
        number=25,
        oneof="apply_parameters",
        message=LeadFormAssetParameters,
    )


class ApplyRecommendationResponse(proto.Message):
    r"""Response message for
    [RecommendationService.ApplyRecommendation][google.ads.googleads.v23.services.RecommendationService.ApplyRecommendation].

    Attributes:
        results (MutableSequence[google.ads.googleads.v23.services.types.ApplyRecommendationResult]):
            Results of operations to apply
            recommendations.
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors) we return
            the RPC level error.
    """

    results: MutableSequence["ApplyRecommendationResult"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="ApplyRecommendationResult",
    )
    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=2,
        message=status_pb2.Status,
    )


class ApplyRecommendationResult(proto.Message):
    r"""The result of applying a recommendation.

    Attributes:
        resource_name (str):
            Returned for successful applies.
    """

    resource_name: str = proto.Field(
        proto.STRING,
        number=1,
    )


class DismissRecommendationRequest(proto.Message):
    r"""Request message for
    [RecommendationService.DismissRecommendation][google.ads.googleads.v23.services.RecommendationService.DismissRecommendation].

    Attributes:
        customer_id (str):
            Required. The ID of the customer with the
            recommendation.
        operations (MutableSequence[google.ads.googleads.v23.services.types.DismissRecommendationRequest.DismissRecommendationOperation]):
            Required. The list of operations to dismiss recommendations.
            If partial_failure=false all recommendations should be of
            the same type There is a limit of 100 operations per
            request.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, operations will be carried in
            a single transaction if and only if they are all
            valid. Default is false.
    """

    class DismissRecommendationOperation(proto.Message):
        r"""Operation to dismiss a single recommendation identified by
        resource_name.

        Attributes:
            resource_name (str):
                The resource name of the recommendation to
                dismiss.
        """

        resource_name: str = proto.Field(
            proto.STRING,
            number=1,
        )

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    operations: MutableSequence[DismissRecommendationOperation] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=3,
            message=DismissRecommendationOperation,
        )
    )
    partial_failure: bool = proto.Field(
        proto.BOOL,
        number=2,
    )


class DismissRecommendationResponse(proto.Message):
    r"""Response message for
    [RecommendationService.DismissRecommendation][google.ads.googleads.v23.services.RecommendationService.DismissRecommendation].

    Attributes:
        results (MutableSequence[google.ads.googleads.v23.services.types.DismissRecommendationResponse.DismissRecommendationResult]):
            Results of operations to dismiss
            recommendations.
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors) we return
            the RPC level error.
    """

    class DismissRecommendationResult(proto.Message):
        r"""The result of dismissing a recommendation.

        Attributes:
            resource_name (str):
                Returned for successful dismissals.
        """

        resource_name: str = proto.Field(
            proto.STRING,
            number=1,
        )

    results: MutableSequence[DismissRecommendationResult] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message=DismissRecommendationResult,
    )
    partial_failure_error: status_pb2.Status = proto.Field(
        proto.MESSAGE,
        number=2,
        message=status_pb2.Status,
    )


class GenerateRecommendationsRequest(proto.Message):
    r"""Request message for
    [RecommendationService.GenerateRecommendations][google.ads.googleads.v23.services.RecommendationService.GenerateRecommendations].


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer generating
            recommendations.
        recommendation_types (MutableSequence[google.ads.googleads.v23.enums.types.RecommendationTypeEnum.RecommendationType]):
            Required. List of eligible recommendation_types to generate.
            If the uploaded criteria isn't sufficient to make a
            recommendation, or the campaign is already in the
            recommended state, no recommendation will be returned for
            that type. Generally, a recommendation is returned if all
            required fields for that recommendation_type are uploaded,
            but there are cases where this is still not sufficient.

            The following recommendation_types are supported for
            recommendation generation: CAMPAIGN_BUDGET, KEYWORD,
            MAXIMIZE_CLICKS_OPT_IN, MAXIMIZE_CONVERSIONS_OPT_IN,
            MAXIMIZE_CONVERSION_VALUE_OPT_IN, SET_TARGET_CPA,
            SET_TARGET_ROAS, SITELINK_ASSET, TARGET_CPA_OPT_IN,
            TARGET_ROAS_OPT_IN
        advertising_channel_type (google.ads.googleads.v23.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType):
            Required. Advertising channel type of the campaign. The
            following advertising_channel_types are supported for
            recommendation generation: PERFORMANCE_MAX and SEARCH
        campaign_sitelink_count (int):
            Optional. Number of sitelinks on the campaign. This field is
            necessary for the following recommendation_types:
            SITELINK_ASSET

            This field is a member of `oneof`_ ``_campaign_sitelink_count``.
        conversion_tracking_status (google.ads.googleads.v23.enums.types.ConversionTrackingStatusEnum.ConversionTrackingStatus):
            Optional. Current conversion tracking status. This field is
            necessary for the following recommendation_types:
            MAXIMIZE_CLICKS_OPT_IN, MAXIMIZE_CONVERSIONS_OPT_IN,
            MAXIMIZE_CONVERSION_VALUE_OPT_IN, SET_TARGET_CPA,
            SET_TARGET_ROAS, TARGET_CPA_OPT_IN, TARGET_ROAS_OPT_IN

            This field is a member of `oneof`_ ``_conversion_tracking_status``.
        bidding_info (google.ads.googleads.v23.services.types.GenerateRecommendationsRequest.BiddingInfo):
            Optional. Current bidding information of the campaign. This
            field is necessary for the following recommendation_types:
            CAMPAIGN_BUDGET, MAXIMIZE_CLICKS_OPT_IN,
            MAXIMIZE_CONVERSIONS_OPT_IN,
            MAXIMIZE_CONVERSION_VALUE_OPT_IN, SET_TARGET_CPA,
            SET_TARGET_ROAS, TARGET_CPA_OPT_IN, TARGET_ROAS_OPT_IN

            This field is a member of `oneof`_ ``_bidding_info``.
        ad_group_info (MutableSequence[google.ads.googleads.v23.services.types.GenerateRecommendationsRequest.AdGroupInfo]):
            Optional. Current AdGroup Information. Supports information
            from a single AdGroup. This field is optional for the
            following recommendation_types: KEYWORD This field is
            required for the following recommendation_types:
            CAMPAIGN_BUDGET if AdvertisingChannelType is SEARCH
        seed_info (google.ads.googleads.v23.services.types.GenerateRecommendationsRequest.SeedInfo):
            Optional. Seed information for Keywords. This field is
            necessary for the following recommendation_types: KEYWORD

            This field is a member of `oneof`_ ``_seed_info``.
        budget_info (google.ads.googleads.v23.services.types.GenerateRecommendationsRequest.BudgetInfo):
            Optional. Current budget information. This field is optional
            for the following recommendation_types: CAMPAIGN_BUDGET

            This field is a member of `oneof`_ ``_budget_info``.
        campaign_image_asset_count (int):
            Optional. Current campaign image asset count. This field is
            optional for the following recommendation_types:
            CAMPAIGN_BUDGET

            This field is a member of `oneof`_ ``_campaign_image_asset_count``.
        campaign_call_asset_count (int):
            Optional. Current campaign call asset count. This field is
            optional for the following recommendation_types:
            CAMPAIGN_BUDGET

            This field is a member of `oneof`_ ``_campaign_call_asset_count``.
        country_codes (MutableSequence[str]):
            Optional. Current campaign country codes. This field is
            required for the following recommendation_types:
            CAMPAIGN_BUDGET if AdvertisingChannelType is SEARCH
        language_codes (MutableSequence[str]):
            Optional. Current campaign language codes. This field is
            required for the following recommendation_types:
            CAMPAIGN_BUDGET if AdvertisingChannelType is SEARCH
        positive_locations_ids (MutableSequence[int]):
            Optional. Current campaign positive location ids. One of
            this field OR negative_location_ids is required for the
            following recommendation_types: CAMPAIGN_BUDGET if
            AdvertisingChannelType is SEARCH
        negative_locations_ids (MutableSequence[int]):
            Optional. Current campaign negative location ids. One of
            this field OR positive_location_ids is required for the
            following recommendation_types: CAMPAIGN_BUDGET if
            AdvertisingChannelType is SEARCH
        asset_group_info (MutableSequence[google.ads.googleads.v23.services.types.GenerateRecommendationsRequest.AssetGroupInfo]):
            Optional. Current AssetGroup Information. This field is
            required for the following recommendation_types:
            CAMPAIGN_BUDGET
        target_partner_search_network (bool):
            Optional. If true, the campaign is opted into serving ads on
            the Google Partner Network. This field is optional for the
            following recommendation_types: CAMPAIGN_BUDGET

            This field is a member of `oneof`_ ``_target_partner_search_network``.
        target_content_network (bool):
            Optional. If true, the campaign is opted into serving ads on
            specified placements in the Google Display Network. This
            field is optional for the following recommendation_types:
            CAMPAIGN_BUDGET

            This field is a member of `oneof`_ ``_target_content_network``.
        merchant_center_account_id (int):
            Optional. Merchant Center account ID. This field should only
            be set when advertising_channel_type is PERFORMANCE_MAX.
            Setting this field causes RecommendationService to generate
            recommendations for Performance Max for retail instead of
            standard Performance Max. This field is optional for the
            following recommendation_types: CAMPAIGN_BUDGET

            This field is a member of `oneof`_ ``_merchant_center_account_id``.
        is_new_customer (bool):
            Optional. Whether or not this customer should be treated as
            a "new" customer (that is, a customer who has not yet
            created a campaign).

            Setting this to ``true`` will cause the backend to generate
            recommendations using a dedicated recommendation model for
            onboarding new customers, as opposed to the default model
            for existing customers. This is only recommended for
            customers with 0 campaigns.

            This field is optional for the following
            recommendation_types: CAMPAIGN_BUDGET

            This field is a member of `oneof`_ ``_is_new_customer``.
    """

    class BiddingInfo(proto.Message):
        r"""Current bidding information of the campaign. Provides a
        wrapper for bidding-related signals that inform recommendations.

        This message has `oneof`_ fields (mutually exclusive fields).
        For each oneof, at most one member field can be set at the same time.
        Setting any member of the oneof automatically clears all other
        members.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            bidding_strategy_type (google.ads.googleads.v23.enums.types.BiddingStrategyTypeEnum.BiddingStrategyType):
                Current bidding strategy. This field is necessary for the
                following recommendation_types: CAMPAIGN_BUDGET,
                MAXIMIZE_CLICKS_OPT_IN, MAXIMIZE_CONVERSIONS_OPT_IN,
                MAXIMIZE_CONVERSION_VALUE_OPT_IN, SET_TARGET_CPA,
                SET_TARGET_ROAS, TARGET_CPA_OPT_IN, TARGET_ROAS_OPT_IN

                This field is a member of `oneof`_ ``_bidding_strategy_type``.
            target_cpa_micros (int):
                Current target_cpa in micros. This can be populated for
                campaigns with a bidding strategy type of TARGET_CPA or
                MAXIMIZE_CONVERSIONS.

                This field is a member of `oneof`_ ``bidding_strategy_target_info``.
            target_roas (float):
                Current target_roas. This can be populated for campaigns
                with a bidding strategy type of TARGET_ROAS or
                MAXIMIZE_CONVERSION_VALUE.

                This field is a member of `oneof`_ ``bidding_strategy_target_info``.
            target_impression_share_info (google.ads.googleads.v23.services.types.GenerateRecommendationsRequest.TargetImpressionShareInfo):
                Optional. Current Target Impression Share information of the
                campaign. This field is necessary for the following
                recommendation_types: CAMPAIGN_BUDGET

                This field is a member of `oneof`_ ``bidding_strategy_target_info``.
        """

        bidding_strategy_type: (
            gage_bidding_strategy_type.BiddingStrategyTypeEnum.BiddingStrategyType
        ) = proto.Field(
            proto.ENUM,
            number=1,
            optional=True,
            enum=gage_bidding_strategy_type.BiddingStrategyTypeEnum.BiddingStrategyType,
        )
        target_cpa_micros: int = proto.Field(
            proto.INT64,
            number=2,
            oneof="bidding_strategy_target_info",
        )
        target_roas: float = proto.Field(
            proto.DOUBLE,
            number=3,
            oneof="bidding_strategy_target_info",
        )
        target_impression_share_info: (
            "GenerateRecommendationsRequest.TargetImpressionShareInfo"
        ) = proto.Field(
            proto.MESSAGE,
            number=4,
            oneof="bidding_strategy_target_info",
            message="GenerateRecommendationsRequest.TargetImpressionShareInfo",
        )

    class AdGroupInfo(proto.Message):
        r"""Current AdGroup Information of the campaign.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            ad_group_type (google.ads.googleads.v23.enums.types.AdGroupTypeEnum.AdGroupType):
                Optional. AdGroup Type of the AdGroup. This field is
                necessary for the following recommendation_types if
                ad_group_info is set: KEYWORD

                This field is a member of `oneof`_ ``_ad_group_type``.
            keywords (MutableSequence[google.ads.googleads.v23.common.types.KeywordInfo]):
                Optional. Current keywords. This field is optional for the
                following recommendation_types if ad_group_info is set:
                KEYWORD This field is required for the following
                recommendation_types: CAMPAIGN_BUDGET if
                AdvertisingChannelType is SEARCH
        """

        ad_group_type: gage_ad_group_type.AdGroupTypeEnum.AdGroupType = (
            proto.Field(
                proto.ENUM,
                number=1,
                optional=True,
                enum=gage_ad_group_type.AdGroupTypeEnum.AdGroupType,
            )
        )
        keywords: MutableSequence[criteria.KeywordInfo] = proto.RepeatedField(
            proto.MESSAGE,
            number=2,
            message=criteria.KeywordInfo,
        )

    class SeedInfo(proto.Message):
        r"""A keyword seed and a specific url to generate keywords from.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            url_seed (str):
                A specific url to generate ideas from, for
                example: www.example.com/cars.

                This field is a member of `oneof`_ ``_url_seed``.
            keyword_seeds (MutableSequence[str]):
                Optional. Keywords or phrases to generate
                ideas from, for example: cars or "car dealership
                near me".
        """

        url_seed: str = proto.Field(
            proto.STRING,
            number=2,
            optional=True,
        )
        keyword_seeds: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=3,
        )

    class BudgetInfo(proto.Message):
        r"""Current budget information of the campaign.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            current_budget (int):
                Required. Current budget amount in micros. This field is
                necessary for the following recommendation_types if
                budget_info is set: CAMPAIGN_BUDGET

                This field is a member of `oneof`_ ``_current_budget``.
        """

        current_budget: int = proto.Field(
            proto.INT64,
            number=1,
            optional=True,
        )

    class AssetGroupInfo(proto.Message):
        r"""Current AssetGroup information of the campaign.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            final_url (str):
                Required. Current url of the asset group. This field is
                necessary for the following recommendation_types if
                asset_group_info is set: CAMPAIGN_BUDGET

                This field is a member of `oneof`_ ``_final_url``.
            headline (MutableSequence[str]):
                Optional. Current headlines of the asset group. This field
                is optional for the following recommendation_types if
                asset_group_info is set: CAMPAIGN_BUDGET
            description (MutableSequence[str]):
                Optional. Current descriptions of the asset group. This
                field is optional for the following recommendation_types if
                asset_group_info is set: CAMPAIGN_BUDGET
        """

        final_url: str = proto.Field(
            proto.STRING,
            number=1,
            optional=True,
        )
        headline: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=2,
        )
        description: MutableSequence[str] = proto.RepeatedField(
            proto.STRING,
            number=3,
        )

    class TargetImpressionShareInfo(proto.Message):
        r"""Current Target Impression Share information of the campaign.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            location (google.ads.googleads.v23.enums.types.TargetImpressionShareLocationEnum.TargetImpressionShareLocation):
                Required. The targeted location on the search results page.
                This is required for campaigns where the
                AdvertisingChannelType is SEARCH and the bidding strategy
                type is TARGET_IMPRESSION_SHARE.

                This field is a member of `oneof`_ ``_location``.
            target_impression_share_micros (int):
                Required. The chosen fraction of targeted impression share
                in micros. For example, 1% equals 10,000. It must be a value
                between 1 and 1,000,000. This is required for campaigns with
                an AdvertisingChannelType of SEARCH and a bidding strategy
                type of TARGET_IMPRESSION_SHARE.

                This field is a member of `oneof`_ ``_target_impression_share_micros``.
            max_cpc_bid_ceiling (int):
                Optional. Ceiling of max CPC bids in micros set by automated
                bidders. This is optional for campaigns with an
                AdvertisingChannelType of SEARCH and a bidding strategy type
                of TARGET_IMPRESSION_SHARE.

                This field is a member of `oneof`_ ``_max_cpc_bid_ceiling``.
        """

        location: (
            target_impression_share_location.TargetImpressionShareLocationEnum.TargetImpressionShareLocation
        ) = proto.Field(
            proto.ENUM,
            number=1,
            optional=True,
            enum=target_impression_share_location.TargetImpressionShareLocationEnum.TargetImpressionShareLocation,
        )
        target_impression_share_micros: int = proto.Field(
            proto.INT64,
            number=2,
            optional=True,
        )
        max_cpc_bid_ceiling: int = proto.Field(
            proto.INT64,
            number=3,
            optional=True,
        )

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    recommendation_types: MutableSequence[
        recommendation_type.RecommendationTypeEnum.RecommendationType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=2,
        enum=recommendation_type.RecommendationTypeEnum.RecommendationType,
    )
    advertising_channel_type: (
        gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType
    ) = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
    )
    campaign_sitelink_count: int = proto.Field(
        proto.INT32,
        number=4,
        optional=True,
    )
    conversion_tracking_status: (
        conversion_tracking_status_enum.ConversionTrackingStatusEnum.ConversionTrackingStatus
    ) = proto.Field(
        proto.ENUM,
        number=5,
        optional=True,
        enum=conversion_tracking_status_enum.ConversionTrackingStatusEnum.ConversionTrackingStatus,
    )
    bidding_info: BiddingInfo = proto.Field(
        proto.MESSAGE,
        number=6,
        optional=True,
        message=BiddingInfo,
    )
    ad_group_info: MutableSequence[AdGroupInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=7,
        message=AdGroupInfo,
    )
    seed_info: SeedInfo = proto.Field(
        proto.MESSAGE,
        number=8,
        optional=True,
        message=SeedInfo,
    )
    budget_info: BudgetInfo = proto.Field(
        proto.MESSAGE,
        number=9,
        optional=True,
        message=BudgetInfo,
    )
    campaign_image_asset_count: int = proto.Field(
        proto.INT32,
        number=10,
        optional=True,
    )
    campaign_call_asset_count: int = proto.Field(
        proto.INT32,
        number=11,
        optional=True,
    )
    country_codes: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=13,
    )
    language_codes: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=14,
    )
    positive_locations_ids: MutableSequence[int] = proto.RepeatedField(
        proto.INT64,
        number=15,
    )
    negative_locations_ids: MutableSequence[int] = proto.RepeatedField(
        proto.INT64,
        number=16,
    )
    asset_group_info: MutableSequence[AssetGroupInfo] = proto.RepeatedField(
        proto.MESSAGE,
        number=17,
        message=AssetGroupInfo,
    )
    target_partner_search_network: bool = proto.Field(
        proto.BOOL,
        number=18,
        optional=True,
    )
    target_content_network: bool = proto.Field(
        proto.BOOL,
        number=19,
        optional=True,
    )
    merchant_center_account_id: int = proto.Field(
        proto.INT64,
        number=20,
        optional=True,
    )
    is_new_customer: bool = proto.Field(
        proto.BOOL,
        number=21,
        optional=True,
    )


class GenerateRecommendationsResponse(proto.Message):
    r"""Response message for
    [RecommendationService.GenerateRecommendations][google.ads.googleads.v23.services.RecommendationService.GenerateRecommendations].

    Attributes:
        recommendations (MutableSequence[google.ads.googleads.v23.resources.types.Recommendation]):
            List of generated recommendations from the passed in set of
            requested recommendation_types. If there isn't sufficient
            data to generate a recommendation for the requested
            recommendation_types, the result set won't contain a
            recommendation for that type.
    """

    recommendations: MutableSequence[recommendation.Recommendation] = (
        proto.RepeatedField(
            proto.MESSAGE,
            number=1,
            message=recommendation.Recommendation,
        )
    )


__all__ = tuple(sorted(__protobuf__.manifest))
