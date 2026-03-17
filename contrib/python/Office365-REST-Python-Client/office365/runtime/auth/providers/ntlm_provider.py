from office365.runtime.auth.authentication_provider import AuthenticationProvider

try:
    from requests_ntlm import HttpNtlmAuth
except ImportError:
    raise ImportError(
        "To use NTLM authentication the package 'requests_ntlm' needs to be installed."
    )


class NtlmProvider(AuthenticationProvider):
    def __init__(self, username, password):
        """
        Provides NTLM authentication (intended for SharePoint On-Premises)

        Note: due to Outlook REST API v1.0 BasicAuth Deprecation
        (refer https://developer.microsoft.com/en-us/office/blogs/outlook-rest-api-v1-0-basicauth-deprecation/)
        NetworkCredentialContext class should be no longer utilized for Outlook REST API v1.0

        :type username: str
        :type password: str
        """
        super(NtlmProvider, self).__init__()
        self.auth = HttpNtlmAuth(username, password)

    def authenticate_request(self, request):
        request.auth = self.auth
