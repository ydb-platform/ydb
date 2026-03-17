from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Snapshots(Client):
    """
    Use the Amazon Advertising API for Sponsored Products for campaign, ad group, keyword, negative keyword, and product ad management operations. For more information about Sponsored Products, see the Sponsored Products Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/v2/sp/{}/snapshot', method='POST')
    def post_snapshot(self, recordType, **kwargs) -> ApiResponse:
        """
        Request a file-based snapshot of all entities of the specified type in the account satisfying the filtering criteria.

        Keyword Args
            | path **recordType** (integer): The type of entity for which the snapshot is generated. Available values : campaigns, adGroups, keywords, negativeKeywords, campaignNegativeKeywords, productAds, targets, negativeTargets [required]

        Request body
            | **stateFilter** (string): [required] [ enabled, paused, archived, enabled, paused, enabled, archived, paused, archived, enabled, paused, archived ].

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), recordType), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/snapshots/{}', method='GET')
    def get_snapshot(self, snapshotId, **kwargs) -> ApiResponse:
        r"""
        Gets the status of a requested snapshot.

        Keyword Args
            | path **snapshotId** (number): The snapshot identifier. [required]

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), snapshotId), params=kwargs)

    def download_snapshot(self, **kwargs) -> ApiResponse:
        r"""
        Downloads the snapshot previously get report specified by location (this is not part of the official Amazon Advertising API, is a helper method to download the snapshot). Take in mind that a direct download of location returned in get_snapshot will return 401 - Unauthorized.

        kwarg parameter **file** if not provided will take the default amazon name from path download (add a path with slash / if you want a specific folder, do not add extension as the return will provide the right extension based on format choosed if needed)

        kwarg parameter **format** if not provided a format will return a url to download the snapshot (this url has a expiration time)

        Keyword Args
            | **url** (string): The location obatined from get_snapshot [required]
            | **file** (string): The path to save the file if mode is download json, zip or gzip. [optional]
            | **format** (string): The mode to download the snapshot: data (list), raw, url, json, zip, gzip. Default (url) [optional]

        Returns:
            ApiResponse
        """
        return self._download(self, params=kwargs)
