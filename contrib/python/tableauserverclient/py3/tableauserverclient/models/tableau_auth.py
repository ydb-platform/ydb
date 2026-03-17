import abc
from typing import Optional


class Credentials(abc.ABC):
    def __init__(self, site_id: Optional[str] = None, user_id_to_impersonate: Optional[str] = None) -> None:
        self.site_id = site_id or ""
        self.user_id_to_impersonate = user_id_to_impersonate or None

    @property
    @abc.abstractmethod
    def credentials(self) -> dict[str, str]:
        credentials = (
            "Credentials can be username/password, Personal Access Token, or JWT"
            "This method returns values to set as an attribute on the credentials element of the request"
        )
        return {"key": "value"}

    @abc.abstractmethod
    def __repr__(self):
        return "All Credentials types must have a debug display that does not print secrets"


def deprecate_site_attribute():
    import warnings

    warnings.warn(
        "TableauAuth(..., site=...) is deprecated, " "please use TableauAuth(..., site_id=...) instead.",
        DeprecationWarning,
    )


# The traditional auth type: username/password
class TableauAuth(Credentials):
    """
    The TableauAuth class defines the information you can set in a sign-in
    request. The class members correspond to the attributes of a server request
    or response payload. To use this class, create a new instance, supplying
    user name, password, and site information if necessary, and pass the
    request object to the Auth.sign_in method.

    Parameters
    ----------
    username : str
        The user name for the sign-in request.

    password : str
        The password for the sign-in request.

    site_id : str, optional
        This corresponds to the contentUrl attribute in the Tableau REST API.
        The site_id is the portion of the URL that follows the /site/ in the
        URL. For example, "MarketingTeam" is the site_id in the following URL
        MyServer/#/site/MarketingTeam/projects. To specify the default site on
        Tableau Server, you can use an empty string '' (single quotes, no
        space). For Tableau Cloud, you must provide a value for the site_id.

    user_id_to_impersonate : str, optional
        Specifies the id (not the name) of the user to sign in as. This is not
        available for Tableau Online.

    Examples
    --------
    >>> import tableauserverclient as TSC

    >>> tableau_auth = TSC.TableauAuth('USERNAME', 'PASSWORD', site_id='CONTENTURL')
    >>> server = TSC.Server('https://SERVER_URL', use_server_version=True)
    >>> server.auth.sign_in(tableau_auth)

    """

    def __init__(
        self, username: str, password: str, site_id: Optional[str] = None, user_id_to_impersonate: Optional[str] = None
    ) -> None:
        super().__init__(site_id, user_id_to_impersonate)
        if password is None:
            raise TabError("Must provide a password when using traditional authentication")
        self.password = password
        self.username = username

    @property
    def credentials(self) -> dict[str, str]:
        return {"name": self.username, "password": self.password}

    def __repr__(self):
        if self.user_id_to_impersonate:
            uid = f", user_id_to_impersonate=f{self.user_id_to_impersonate}"
        else:
            uid = ""
        return f"<{self.__class__.__qualname__} username={self.username} password=redacted (site={self.site_id}{uid})>"


# A Tableau-generated Personal Access Token
class PersonalAccessTokenAuth(Credentials):
    """
    The PersonalAccessTokenAuth class defines the information you can set in a sign-in
    request. The class members correspond to the attributes of a server request
    or response payload. To use this class, create a new instance, supplying
    token name, token secret, and site information if necessary, and pass the
    request object to the Auth.sign_in method.

    Parameters
    ----------
    token_name : str
        The name of the personal access token.

    personal_access_token : str
        The personal access token secret for the sign in request.

    site_id : str, optional
        This corresponds to the contentUrl attribute in the Tableau REST API.
        The site_id is the portion of the URL that follows the /site/ in the
        URL. For example, "MarketingTeam" is the site_id in the following URL
        MyServer/#/site/MarketingTeam/projects. To specify the default site on
        Tableau Server, you can use an empty string '' (single quotes, no
        space). For Tableau Cloud, you must provide a value for the site_id.

    user_id_to_impersonate : str, optional
        Specifies the id (not the name) of the user to sign in as. This is not
        available for Tableau Online.

    Examples
    --------
    >>> import tableauserverclient as TSC

    >>> tableau_auth = TSC.PersonalAccessTokenAuth("token_name", "token_secret", site_id='CONTENTURL')
    >>> server = TSC.Server('https://SERVER_URL', use_server_version=True)
    >>> server.auth.sign_in(tableau_auth)

    """

    def __init__(
        self,
        token_name: str,
        personal_access_token: str,
        site_id: Optional[str] = None,
        user_id_to_impersonate: Optional[str] = None,
    ) -> None:
        if personal_access_token is None or token_name is None:
            raise TabError("Must provide a token and token name when using PAT authentication")
        super().__init__(site_id=site_id, user_id_to_impersonate=user_id_to_impersonate)
        self.token_name = token_name
        self.personal_access_token = personal_access_token

    @property
    def credentials(self) -> dict[str, str]:
        return {
            "personalAccessTokenName": self.token_name,
            "personalAccessTokenSecret": self.personal_access_token,
        }

    def __repr__(self):
        if self.user_id_to_impersonate:
            uid = f", user_id_to_impersonate=f{self.user_id_to_impersonate}"
        else:
            uid = ""
        return (
            f"<{self.__class__.__qualname__}(name={self.token_name} token={self.personal_access_token[:2]}..."
            f"site={self.site_id}{uid}) >"
        )


# A standard JWT generated specifically for Tableau
class JWTAuth(Credentials):
    """
    The JWTAuth class defines the information you can set in a sign-in
    request. The class members correspond to the attributes of a server request
    or response payload. To use this class, create a new instance, supplying
    an encoded JSON Web Token, and site information if necessary, and pass the
    request object to the Auth.sign_in method.

    Parameters
    ----------
    token : str
        The encoded JSON Web Token.

    site_id : str, optional
        This corresponds to the contentUrl attribute in the Tableau REST API.
        The site_id is the portion of the URL that follows the /site/ in the
        URL. For example, "MarketingTeam" is the site_id in the following URL
        MyServer/#/site/MarketingTeam/projects. To specify the default site on
        Tableau Server, you can use an empty string '' (single quotes, no
        space). For Tableau Cloud, you must provide a value for the site_id.

    user_id_to_impersonate : str, optional
        Specifies the id (not the name) of the user to sign in as. This is not
        available for Tableau Online.

    Examples
    --------
    >>> import jwt
    >>> import tableauserverclient as TSC

    >>> jwt_token = jwt.encode(...)
    >>> tableau_auth = TSC.JWTAuth(token, site_id='CONTENTURL')
    >>> server = TSC.Server('https://SERVER_URL', use_server_version=True)
    >>> server.auth.sign_in(tableau_auth)

    """

    def __init__(
        self,
        jwt: str,
        isUat: bool = False,
        site_id: Optional[str] = None,
        user_id_to_impersonate: Optional[str] = None,
    ) -> None:
        if jwt is None:
            raise TabError("Must provide a JWT token when using JWT authentication")
        super().__init__(site_id, user_id_to_impersonate)
        self.jwt = jwt
        self.isUat = isUat

    @property
    def credentials(self) -> dict[str, str]:
        return {"jwt": self.jwt, "isUat": str(self.isUat).lower()}

    def __repr__(self):
        if self.user_id_to_impersonate:
            uid = f", user_id_to_impersonate=f{self.user_id_to_impersonate}"
        else:
            uid = ""
        return f"<{self.__class__.__qualname__} jwt={self.jwt[:5]}... isUat={self.isUat} (site={self.site_id}{uid})>"
