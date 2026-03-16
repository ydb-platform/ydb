from office365.sharepoint.sharing.links.share_response import ShareLinkResponse


class ShareLinkPartialSuccessResponse(ShareLinkResponse):
    """"""

    @property
    def entity_type_name(self):
        return "SP.Sharing.ShareLinkPartialSuccessResponse"
