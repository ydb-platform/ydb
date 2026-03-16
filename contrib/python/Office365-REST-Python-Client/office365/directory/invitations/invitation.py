from typing import Optional

from office365.directory.invitations.message_info import InvitedUserMessageInfo
from office365.directory.users.user import User
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class Invitation(Entity):
    """Represents an invitation that is used to add external users to an organization.

    The invitation process uses the following flow:

      - An invitation is created
      - An invitation is sent to the invited user (containing an invitation link)
      - The invited user clicks on the invitation link, signs in and redeems the invitation and creation of the
        user entity representing the invited user completes
      - The user is redirected to a specific page after redemption completes

     Creating an invitation will return a redemption URL in the response (inviteRedeemUrl).
     The create invitation API can automatically send an email containing the redemption URL to the invited user,
     by setting the sendInvitationMessage to true. You can also customize the message that will be sent to
     the invited user. Instead, if you wish to send the redemption URL through some other means, you can set the
     sendInvitationMessage to false and use the redeem URL from the response to craft your own communication.
     Currently, there is no API to perform the redemption process. The invited user has to click on the inviteRedeemUrl
     link sent in the communication in the step above, and go through the interactive redemption process in a browser.
     Once completed, the invited user becomes an external user in the organization.
    """

    @property
    def invited_user_display_name(self):
        # type: () -> Optional[str]
        """The display name of the user being invited."""
        return self.properties.get("invitedUserDisplayName", None)

    @property
    def invited_user_email_address(self):
        # type: () -> Optional[str]
        """The email address of the user being invited."""
        return self.properties.get("invitedUserEmailAddress", None)

    @property
    def invited_user_message_info(self):
        """"""
        return self.properties.get("invitedUserMessageInfo", InvitedUserMessageInfo())

    @property
    def invited_user(self):
        """The user created as part of the invitation creation."""
        return self.properties.get(
            "invitedUser",
            User(self.context, ResourcePath("invitedUser", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "invitedUserMessageInfo": self.invited_user_message_info,
                "invitedUser": self.invited_user,
            }
            default_value = property_mapping.get(name, None)
        return super(Invitation, self).get_property(name, default_value)
