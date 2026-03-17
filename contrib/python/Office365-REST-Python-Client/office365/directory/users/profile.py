from office365.directory.users.password_profile import PasswordProfile
from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class UserProfile(ClientValue):
    def __init__(
        self,
        principal_name,
        password,
        display_name=None,
        given_name=None,
        company_name=None,
        business_phones=None,
        office_location=None,
        city=None,
        country=None,
        account_enabled=False,
    ):
        """
        User profile

        :type principal_name: str
        :type password: str
        :type display_name: str
        :type account_enabled: bool
        :type given_name: str
        :type company_name: str
        :type business_phones: list[str]
        :type office_location: str
        :type city: str
        :type country: str
        """
        super(UserProfile, self).__init__()
        self.userPrincipalName = principal_name
        self.passwordProfile = PasswordProfile(password)
        self.mailNickname = principal_name.split("@")[0]
        self.displayName = display_name or principal_name.split("@")[0]
        self.accountEnabled = account_enabled
        self.givenName = given_name
        self.companyName = company_name
        self.businessPhones = StringCollection(business_phones)
        self.officeLocation = office_location
        self.city = city
        self.country = country
