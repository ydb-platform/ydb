from office365.runtime.client_value import ClientValue
from office365.sharepoint.administration.orgassets.org_assets import OrgAssets


class FilePickerOptions(ClientValue):
    def __init__(
        self,
        search_enabled=None,
        central_asset_repository=OrgAssets(),
        org_assets=OrgAssets(),
    ):
        self.BingSearchEnabled = search_enabled
        self.CentralAssetRepository = central_asset_repository
        self.OrgAssets = org_assets

    @property
    def entity_type_name(self):
        return "SP.Publishing.FilePickerOptions"
