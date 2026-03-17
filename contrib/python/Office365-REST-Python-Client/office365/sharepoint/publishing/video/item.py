from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class VideoItem(Entity):
    def get_video_embed_code(
        self, width, height, autoplay=True, show_info=True, make_responsive=True
    ):
        """

        :type width: int
        :type height: int
        :type autoplay: bool
        :type show_info: bool
        :type make_responsive: bool
        """
        return_type = ClientResult(self.context)
        params = {
            "width": width,
            "height": height,
            "autoplay": autoplay,
            "showInfo": show_info,
            "makeResponsive": make_responsive,
        }
        qry = ServiceOperationQuery(
            self, "GetVideoEmbedCode", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_video_owner(self, owner_id):
        """
        :param int owner_id:
        """
        payload = {"id": owner_id}
        qry = ServiceOperationQuery(self, "SetVideoOwner", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "SP.Publishing.VideoItem"
