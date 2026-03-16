from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class Forecast(Client):
    @sp_endpoint('/sd/forecasts', method='POST')
    def list_forecasts(self, **kwargs) -> ApiResponse:
        r"""
        Return forecasts for an ad group that may or may not exist.

        Request Body
            | SDForecastRequest {
            | }

        Returns
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)
