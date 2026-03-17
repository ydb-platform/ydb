from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Exports(Client):
    """Amazon Ads API Exports Version 3
    """
    @sp_endpoint('/campaigns/export', method='POST')
    def campaigns_export(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        application/vnd.campaignsexport.v1+json
        """
        content_type = 'application/vnd.campaignsexport.v'+ str(version) +'+json'
        accept = 'application/vnd.campaignsexport.v'+ str(version) +'+json'
        headers = {'Content-Type': content_type, 'Accept': accept}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)


    @sp_endpoint('/adGroups/export', method='POST')
    def adgroups_export(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        application/vnd.adgroupsexport.v1+json
        """
        content_type = 'application/vnd.adgroupsexport.v'+ str(version) +'+json'
        accept = 'application/vnd.adgroupsexport.v'+ str(version) +'+json'
        headers = {'Content-Type': content_type, 'Accept': accept}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)


    @sp_endpoint('/targets/export', method='POST')
    def targets_export(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        application/vnd.targetsexport.v1+json
        """
        content_type = 'application/vnd.targetsexport.v'+ str(version) +'+json'
        accept = 'application/vnd.targetsexport.v'+ str(version) +'+json'
        headers = {'Content-Type': content_type, 'Accept': accept}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)


    @sp_endpoint('/exports/{}', method='GET')
    def get_export(self, exportId, typeExport, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        """
        content_type = 'application/vnd.'+typeExport+'export.v' + str(version) + '+json'
        headers = {'Content-Type': content_type, 'Accept': content_type}
        return self._request(fill_query_params(kwargs.pop('path'), exportId), params=kwargs, headers=headers)
