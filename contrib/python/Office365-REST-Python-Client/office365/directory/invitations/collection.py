from office365.directory.invitations.invitation import Invitation
from office365.entity_collection import EntityCollection
from office365.runtime.queries.create_entity import CreateEntityQuery


class InvitationCollection(EntityCollection[Invitation]):
    """Invitation's collection"""

    def __init__(self, context, resource_path=None):
        super(InvitationCollection, self).__init__(context, Invitation, resource_path)

    def create(self, invited_user_email_address, invite_redirect_url=None):
        """
        Use this API to create a new invitation. Invitation adds an external user to the organization.

        When creating a new invitation you have several options available:

          - On invitation creation, Microsoft Graph can automatically send an invitation email directly to
          the invited user, or your app can use the inviteRedeemUrl returned in the creation response to craft your
          own invitation (through your communication mechanism of choice) to the invited user.
          If you decide to have Microsoft Graph send an invitation email automatically, you can control the content
          and language of the email using invitedUserMessageInfo.
          - When the user is invited, a user entity (of userType Guest) is created and can now be used to control
          access to resources. The invited user has to go through the redemption process to access any resources
          they have been invited to.

          :param str invited_user_email_address: The email address of the user you are inviting.
          :param str invite_redirect_url: The URL that the user will be redirected to after redemption.
        """
        return_type = Invitation(self.context)
        properties = {
            "invitedUserEmailAddress": invited_user_email_address,
            "inviteRedirectUrl": invite_redirect_url,
        }
        qry = CreateEntityQuery(self, properties, return_type)
        self.context.add_query(qry)
        self.add_child(return_type)
        return return_type
