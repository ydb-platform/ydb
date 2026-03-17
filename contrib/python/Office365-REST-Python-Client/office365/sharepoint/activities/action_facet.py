from office365.runtime.client_value import ClientValue
from office365.sharepoint.activities.facets.add_to_onedrive import AddToOneDriveFacet
from office365.sharepoint.activities.facets.checkin import CheckinFacet
from office365.sharepoint.activities.facets.checkout import CheckoutFacet
from office365.sharepoint.activities.facets.create import CreateFacet
from office365.sharepoint.activities.facets.delete import DeleteFacet
from office365.sharepoint.activities.facets.discard_checkout import DiscardCheckoutFacet
from office365.sharepoint.activities.facets.edit import EditFacet
from office365.sharepoint.activities.facets.get_comment import GetCommentFacet
from office365.sharepoint.activities.facets.get_mention import GetMentionFacet
from office365.sharepoint.activities.facets.move import MoveFacet
from office365.sharepoint.activities.facets.point_in_time_restore import (
    PointInTimeRestoreFacet,
)
from office365.sharepoint.activities.facets.rename import RenameFacet
from office365.sharepoint.activities.facets.sharing import SharingFacet
from office365.sharepoint.activities.facets.task_completed import TaskCompletedFacet
from office365.sharepoint.activities.facets.version import VersionFacet


class ActionFacet(ClientValue):
    def __init__(
        self,
        add_to_one_drive=AddToOneDriveFacet(),
        checkin=CheckinFacet(),
        checkout=CheckoutFacet(),
        comment=GetCommentFacet(),
        create=CreateFacet(),
        delete=DeleteFacet(),
        discard_checkout=DiscardCheckoutFacet(),
        edit=EditFacet(),
        mention=GetMentionFacet(),
        move=MoveFacet(),
        pointInTimeRestore=PointInTimeRestoreFacet(),
        rename=RenameFacet(),
        share=SharingFacet(),
        taskCompleted=TaskCompletedFacet(),
        version=VersionFacet(),
    ):
        """
        :param AddToOneDriveFacet add_to_one_drive:
        :param CheckinFacet checkin:
        :param CheckoutFacet checkout:
        :param GetCommentFacet comment:
        :param CreateFacet create:
        :param RenameFacet rename:
        :param SharingFacet share:
        """
        self.addToOneDrive = add_to_one_drive
        self.checkin = checkin
        self.checkout = checkout
        self.comment = comment
        self.create = create
        self.delete = delete
        self.discardCheckout = discard_checkout
        self.edit = edit
        self.mention = mention
        self.move = move
        self.pointInTimeRestore = pointInTimeRestore
        self.rename = rename
        self.share = share
        self.taskCompleted = taskCompleted
        self.version = version

    def __repr__(self):
        return self.facet_type

    @property
    def facet_type(self):
        return next((n for n, v in self if v), None)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.ActionFacet"
