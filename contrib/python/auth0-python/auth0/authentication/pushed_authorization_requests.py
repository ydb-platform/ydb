from typing import Any

from .base import AuthenticationBase



class PushedAuthorizationRequests(AuthenticationBase):
    """Pushed Authorization Request (PAR) endpoint"""

    def pushed_authorization_request(
        self, response_type: str, redirect_uri: str, **kwargs
    ) -> Any:
        """Send a Pushed Authorization Request (PAR).

        Args:
             response_type (str): Indicates to Auth0 which OAuth 2.0 flow you want to perform.
             redirect_uri (str): The URL to which Auth0 will redirect the browser after authorization has been granted
             by the user.
             **kwargs: Other fields to send along with the PAR.
             For RAR requests, authorization_details parameter should be added in a proper format. See:https://datatracker.ietf.org/doc/html/rfc9396
             For JAR requests, requests parameter should be send with the JWT as the value. See: https://datatracker.ietf.org/doc/html/rfc9126#name-the-request-request-paramet

        See: https://www.rfc-editor.org/rfc/rfc9126.html
        """
        return self.authenticated_post(
            f"{self.protocol}://{self.domain}/oauth/par",
            data={
                "client_id":self.client_id,
                "client_secret":self.client_secret,
                "response_type": response_type,
                "redirect_uri": redirect_uri,
                **kwargs,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )