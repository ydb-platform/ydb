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

from google.ads.googleads.v22.enums.types import (
    advertising_channel_type as gage_advertising_channel_type,
)
from google.ads.googleads.v22.enums.types import (
    asset_field_type as gage_asset_field_type,
)


__protobuf__ = proto.module(
    package="google.ads.googleads.v22.services",
    marshal="google.ads.googleads.v22",
    manifest={
        "GenerateTextRequest",
        "GenerateTextResponse",
        "GeneratedText",
        "GenerateImagesRequest",
        "FinalUrlImageGenerationInput",
        "FreeformImageGenerationInput",
        "ProductRecontextGenerationImageInput",
        "GenerateImagesResponse",
        "SourceImage",
        "GeneratedImage",
        "AssetGenerationExistingContext",
    },
)


class GenerateTextRequest(proto.Message):
    r"""Request message for
    [AssetGenerationService.GenerateText][google.ads.googleads.v22.services.AssetGenerationService.GenerateText]

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer to generate
            assets for. Required.
        asset_field_types (MutableSequence[google.ads.googleads.v22.enums.types.AssetFieldTypeEnum.AssetFieldType]):
            Required. Which field types text is being generated for.
            Supported values are: HEADLINE, LONG_HEADLINE, DESCRIPTION.
            Required.
        final_url (str):
            Optional. Final url to use as a source for generating
            assets. Required if existing_generation_context is not
            provided or does not have a final url associated with it.
        freeform_prompt (str):
            Optional. A freeform description of what
            assets should be generated. The string length
            must be between 1 and 1500, inclusive.
        keywords (MutableSequence[str]):
            Optional. A freeform list of keywords that
            are relevant, used to inform asset generation.
        existing_generation_context (google.ads.googleads.v22.services.types.AssetGenerationExistingContext):
            Optional. The setting for which assets are
            being generated, such as an existing AssetGroup
            or AdGroupAd.

            This field is a member of `oneof`_ ``context``.
        advertising_channel_type (google.ads.googleads.v22.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType):
            Optional. The advertising channel for which to generate
            assets. Required if existing_context is not provided.

            Supported channel types: SEARCH, PERFORMANCE_MAX, DISPLAY,
            and DEMAND_GEN

            This field is a member of `oneof`_ ``context``.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_field_types: MutableSequence[
        gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=2,
        enum=gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    final_url: str = proto.Field(
        proto.STRING,
        number=5,
    )
    freeform_prompt: str = proto.Field(
        proto.STRING,
        number=6,
    )
    keywords: MutableSequence[str] = proto.RepeatedField(
        proto.STRING,
        number=7,
    )
    existing_generation_context: "AssetGenerationExistingContext" = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="context",
        message="AssetGenerationExistingContext",
    )
    advertising_channel_type: (
        gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType
    ) = proto.Field(
        proto.ENUM,
        number=4,
        oneof="context",
        enum=gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
    )


class GenerateTextResponse(proto.Message):
    r"""Response message for
    [AssetGenerationService.GenerateText][google.ads.googleads.v22.services.AssetGenerationService.GenerateText]

    Attributes:
        generated_text (MutableSequence[google.ads.googleads.v22.services.types.GeneratedText]):
            List of text that was generated and the field
            type to use it as.
    """

    generated_text: MutableSequence["GeneratedText"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="GeneratedText",
    )


class GeneratedText(proto.Message):
    r"""Data and metadata about a piece of generated text.

    Attributes:
        text (str):
            A string of text that was generated.
        asset_field_type (google.ads.googleads.v22.enums.types.AssetFieldTypeEnum.AssetFieldType):
            The type of asset this text is intended to be
            used as.
    """

    text: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_field_type: (
        gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )


class GenerateImagesRequest(proto.Message):
    r"""Request message for
    [AssetGenerationService.GenerateImages][google.ads.googleads.v22.services.AssetGenerationService.GenerateImages]

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer for whom the
            images are being generated. Required.
        asset_field_types (MutableSequence[google.ads.googleads.v22.enums.types.AssetFieldTypeEnum.AssetFieldType]):
            Optional. Output field types for generated images. Supported
            values are MARKETING_IMAGE, SQUARE_MARKETING_IMAGE,
            PORTRAIT_MARKETING_IMAGE, and TALL_PORTRAIT_MARKETING_IMAGE.
            All specified field types must be compatible with the
            ``advertising_channel_type`` or
            ``existing_generation_context`` (whichever is set). If no
            field types are provided, images will be generated for all
            compatible field types.
        advertising_channel_type (google.ads.googleads.v22.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType):
            Optional. The advertising channel type for which the images
            are being generated. This field is required if
            ``existing_generation_context`` is not provided. Supported
            channel types include SEARCH, PERFORMANCE_MAX, DISPLAY, and
            DEMAND_GEN.

            This field is a member of `oneof`_ ``context``.
        final_url_generation (google.ads.googleads.v22.services.types.FinalUrlImageGenerationInput):
            Optional. Generate images from a final url.

            This field is a member of `oneof`_ ``generation_type``.
        freeform_generation (google.ads.googleads.v22.services.types.FreeformImageGenerationInput):
            Optional. Generate images from a freeform
            prompt.

            This field is a member of `oneof`_ ``generation_type``.
        product_recontext_generation (google.ads.googleads.v22.services.types.ProductRecontextGenerationImageInput):
            Optional. Generate new images by
            recontextualizing existing product images.

            This field is a member of `oneof`_ ``generation_type``.
    """

    customer_id: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_field_types: MutableSequence[
        gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType
    ] = proto.RepeatedField(
        proto.ENUM,
        number=4,
        enum=gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    advertising_channel_type: (
        gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        oneof="context",
        enum=gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
    )
    final_url_generation: "FinalUrlImageGenerationInput" = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="generation_type",
        message="FinalUrlImageGenerationInput",
    )
    freeform_generation: "FreeformImageGenerationInput" = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="generation_type",
        message="FreeformImageGenerationInput",
    )
    product_recontext_generation: "ProductRecontextGenerationImageInput" = (
        proto.Field(
            proto.MESSAGE,
            number=7,
            oneof="generation_type",
            message="ProductRecontextGenerationImageInput",
        )
    )


class FinalUrlImageGenerationInput(proto.Message):
    r"""Input for guiding image asset generation with a final url.

    Attributes:
        final_url (str):
            Required. A final url to guide the image
            generation process. Required.
    """

    final_url: str = proto.Field(
        proto.STRING,
        number=1,
    )


class FreeformImageGenerationInput(proto.Message):
    r"""Input for guiding image asset generation with a freeform
    prompt.

    Attributes:
        freeform_prompt (str):
            Required. A freeform text description to
            guide the image generation process. The maximum
            length is 1500 characters. Required.
    """

    freeform_prompt: str = proto.Field(
        proto.STRING,
        number=1,
    )


class ProductRecontextGenerationImageInput(proto.Message):
    r"""Input for generating new images by recontextualizing existing
    product images.

    Attributes:
        prompt (str):
            Optional. A freeform description of the
            assets to be generated. Maximum character limit
            is 1500.
        source_images (MutableSequence[google.ads.googleads.v22.services.types.SourceImage]):
            Required. Product images to use for
            generating new images. 1-3 images must be
            provided.
    """

    prompt: str = proto.Field(
        proto.STRING,
        number=1,
    )
    source_images: MutableSequence["SourceImage"] = proto.RepeatedField(
        proto.MESSAGE,
        number=2,
        message="SourceImage",
    )


class GenerateImagesResponse(proto.Message):
    r"""Response message for
    [AssetGenerationService.GenerateImages][google.ads.googleads.v22.services.AssetGenerationService.GenerateImages]

    Attributes:
        generated_images (MutableSequence[google.ads.googleads.v22.services.types.GeneratedImage]):
            Successfully generated images.
    """

    generated_images: MutableSequence["GeneratedImage"] = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="GeneratedImage",
    )


class SourceImage(proto.Message):
    r"""A source image to be used in the generation process.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        image_data (bytes):
            Optional. Raw bytes of the image to use.

            This field is a member of `oneof`_ ``image``.
    """

    image_data: bytes = proto.Field(
        proto.BYTES,
        number=1,
        oneof="image",
    )


class GeneratedImage(proto.Message):
    r"""Generated image data and metadata.

    Attributes:
        image_temporary_url (str):
            A temporary URL for the generated image.
        asset_field_type (google.ads.googleads.v22.enums.types.AssetFieldTypeEnum.AssetFieldType):
            The intended field type for this generated
            image.
    """

    image_temporary_url: str = proto.Field(
        proto.STRING,
        number=1,
    )
    asset_field_type: (
        gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType
    ) = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )


class AssetGenerationExistingContext(proto.Message):
    r"""The context for which assets are being generated, such as an
    existing AssetGroup or AdGroupAd.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        existing_asset_group (str):
            Resource name of an existing AssetGroup for
            which these assets are being generated.

            This field is a member of `oneof`_ ``existing_context``.
        existing_ad_group_ad (str):
            Resource name of an existing AdGroupAd for
            which these assets are being generated.

            This field is a member of `oneof`_ ``existing_context``.
    """

    existing_asset_group: str = proto.Field(
        proto.STRING,
        number=1,
        oneof="existing_context",
    )
    existing_ad_group_ad: str = proto.Field(
        proto.STRING,
        number=2,
        oneof="existing_context",
    )


__all__ = tuple(sorted(__protobuf__.manifest))
