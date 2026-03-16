from office365.sharepoint.multigeo.move_job_entity_data import MoveJobEntityData


class SiteMoveJobEntityData(MoveJobEntityData):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MultiGeo.Service.SiteMoveJobEntityData"
