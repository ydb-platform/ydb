from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class Creatives(Client):
    @sp_endpoint('/sd/creatives', method='GET')
    def list_creatives(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of creatives

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **adGroupIdFilter**:*string* | Optional. The returned array includes only creatives associated with ad group identifiers matching those specified in the comma-delimited string. Cannot be used in conjunction with the creativeIdFilter parameter.

            query **creativeIdFilter**:*string* | Optional. The returned array includes only creatives with identifiers matching those specified in the comma-delimited string. Cannot be used in conjunction with the adGroupIdFilter parameter.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/creatives', method='PUT')
    def edit_creatives(self, **kwargs) -> ApiResponse:
        r"""
        Updates one or more creatives.

        Request Body (required)
            | [
            | CreativeUpdate {
            |   **creativeType** (CreativeTypeInCreativeRequest > String) : The type of the creative. Enum [ IMAGE, VIDEO ]
            |   **properties**\* (CreativeProperties): anyOf
            |       HeadlineCreativeProperties {
            |       User-customizable properties of a creative with headline.
            |           **headline** (string): A marketing phrase to display on the ad. This field is optional and mutable. Maximum number of characters allowed is 50. maxLength: 50
            |       }
            |       LogoCreativeProperties {
            |       User-customizable properties of a creative with a logo.
            |           **brandLogo** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |       CustomImageCreativeProperties {
            |       User-customizable properties of a custom image creative.
            |           **rectCustomImage** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |           **squareCustomImage** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |       }
            |       VideoCreativeProperties {
            |       User-customizable properties of a video creative.
            |           **video** (Video)
            |               **assetId**\* (string): The unique identifier of the video asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular video assetversion.
            |       }
            |   }
            | ]

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)

    @sp_endpoint('/sd/creatives', method='POST')
    def create_creatives(self, **kwargs) -> ApiResponse:
        r"""
        A POST request of one or more creatives.

        Request Body (required)
            | CreateCreative {
            |   **adGroupId**\* (number) : Unique identifier for the ad group associated with the creative.
            |   **creativeType** (CreativeTypeInCreativeRequest > String) : The type of the creative. Enum [ IMAGE, VIDEO ]
            |   **properties**\* (CreativeProperties): anyOf
            |       HeadlineCreativeProperties {
            |       User-customizable properties of a creative with headline.
            |           **headline** (string): A marketing phrase to display on the ad. This field is optional and mutable. Maximum number of characters allowed is 50. maxLength: 50
            |       }
            |       LogoCreativeProperties {
            |       User-customizable properties of a creative with a logo.
            |           **brandLogo** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |       CustomImageCreativeProperties {
            |       User-customizable properties of a custom image creative.
            |           **rectCustomImage** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |           **squareCustomImage** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |       }
            |       VideoCreativeProperties {
            |       User-customizable properties of a video creative.
            |           **video** (Video)
            |               **assetId**\* (string): The unique identifier of the video asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular video assetversion.
            |       }
            | }

        Returns
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/sd/moderation/creatives', method='GET')
    def list_moderation_creatives(self, **kwargs) -> ApiResponse:
        r"""
        Gets a list of creative moderations

            query **language**\*:*string* | The language of the returned creative moderation metadata. Available values : en-US, es-MX, zh-CN, es-ES, it-IT, fr-FR, fr-CA, de-DE, ja-JP, ko-KR, en-GB, en-CA, hi-IN, en-IN, en-DE, en-ES, en-FR, en-IT, en-JP, en-AE, ar-AE

            query **startIndex**:*integer* | Optional. 0-indexed record offset for the result set. Default value : 0

            query **count**:*integer* | Optional. Number of records to include in the paged response. Defaults to max page size.

            query **adGroupIdFilter**:*string* | Optional. The returned array includes only creatives associated with ad group identifiers matching those specified in the comma-delimited string. Cannot be used in conjunction with the creativeIdFilter parameter.

            query **creativeIdFilter**:*string* | Optional. The returned array includes only creatives with identifiers matching those specified in the comma-delimited string. Cannot be used in conjunction with the adGroupIdFilter parameter.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/creatives/preview', method='POST')
    def show_creative_preview(self, **kwargs) -> ApiResponse:
        r"""
        A POST request of one or more creatives.

        Request Body (required)
            | CreativePreviewRequest {
            |   **creative**\* (PreviewCreativeModel){
            |       **creativeType** (CreativeTypeInCreativeRequest > String) : The type of the creative. Enum [ IMAGE, VIDEO ]
            |       **properties** (CreativeProperties): anyOf
            |       HeadlineCreativeProperties {
            |       User-customizable properties of a creative with headline.
            |           **headline** (string): A marketing phrase to display on the ad. This field is optional and mutable. Maximum number of characters allowed is 50. maxLength: 50
            |       }
            |       LogoCreativeProperties {
            |       User-customizable properties of a creative with a logo.
            |           **brandLogo** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |       CustomImageCreativeProperties {
            |       User-customizable properties of a custom image creative.
            |           **rectCustomImage** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |           **squareCustomImage** (Image)
            |           {
            |               **assetId**\* (string): The unique identifier of the image asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular image assetversion.
            |               **croppingCoordinates**
            |               {
            |               Optional cropping coordinates to apply to the image.
            |                   **top**\* (integer): Pixel distance from the top edge of the cropping zone to the top edge of the original image. minimum: 0
            |                   **left**\* (integer): Pixel distance from the left edge of the cropping zone to the left edge of the original image. minimum: 0
            |                   **width**\* (integer): Pixel width of the cropping zone. minimum: 0
            |                   **height**\* (integer): Pixel height of the cropping zone. minimum: 0
            |               }
            |           }
            |       }
            |       }
            |       VideoCreativeProperties {
            |       User-customizable properties of a video creative.
            |           **video** (Video)
            |               **assetId**\* (string): The unique identifier of the video asset. This assetId comes from the Creative Asset Library.
            |               **assetVersion**\* (string): The identifier of the particular video assetversion.
            |       }
            |   }
            |   **previewConfiguration**\* (CreativePreviewConfiguration){
            |   Optional configuration for creative preview.
            |       **size** {
            |       The slot dimension to render the creative. Sponsored Display creatives are responsive to a limited list of width and height pairs, including 300x250, 650x130, 245x250, 414x125, 600x160, 600x300, 728x90, 980x55, 320x50, 970x250 and 270x150.
            |           **width** (integer)
            |           **height** (integer)
            |       }
            |       **products** [
            |       The products to preview. Currently only the first product is previewable.
            |           {
            |               **asin** (string): The ASIN of the product.
            |           }
            |       ]
            |       **landingPageURL** (string): This operation is a PREVIEW ONLY. The URL where customers will land after clicking on its link. Must be provided if a LandingPageType is set. Please note that if a single product ad sets the landing page url, only one product ad can be added to the ad group. Note that this field is not supported when using ASIN or SKU fields.
            |       **landingPageType** (string): This operation is a PREVIEW ONLY. The type of the landingPage used. This field is completely optional and will be set in conjunction with the LandingPageURL to indicate the type of landing page that will be set. Note that this field is not supported when using ASIN or SKU fields. Enum: [ STORE, MOMENT, OFF_AMAZON_LINK ]
            |       **adName** (string): This operation is a PREVIEW ONLY. The name of the ad. Note that this field is not supported when using ASIN or SKU fields.
            |       **isMobile** (boolean): Preview the creative as if it is on a mobile environment.
            |       **isOnAmazon** (boolean): Preview the creative as if it is on an amazon site or third party site. The main difference is whether the preview will contain an AdChoices icon.
            |   }
            | }
        Returns
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)
