"""
github3.api
===========

:copyright: (c) 2012-2014 by Ian Cordasco
:license: Modified BSD, see LICENSE for more details

"""
from .github import GitHub
from .github import GitHubEnterprise

gh = GitHub()


def login(username=None, password=None, token=None, two_factor_callback=None):
    """Construct and return an authenticated GitHub session.

    .. note::

        To allow you to specify either a username and password combination or
        a token, none of the parameters are required. If you provide none of
        them, you will receive ``None``.

    :param str username: login name
    :param str password: password for the login
    :param str token: OAuth token
    :param func two_factor_callback: (optional), function you implement to
        provide the Two-factor Authentication code to GitHub when necessary
    :returns: :class:`GitHub <github3.github.GitHub>`

    """
    g = None

    if (username and password) or token:
        g = GitHub()
        g.login(username, password, token, two_factor_callback)

    return g


def enterprise_login(
    username=None,
    password=None,
    token=None,
    url=None,
    two_factor_callback=None,
):
    """Construct and return an authenticated GitHubEnterprise session.

    .. note::

        To allow you to specify either a username and password combination or
        a token, none of the parameters are required. If you provide none of
        them, you will receive ``None``.

    :param str username: login name
    :param str password: password for the login
    :param str token: OAuth token
    :param str url: URL of a GitHub Enterprise instance
    :param func two_factor_callback: (optional), function you implement to
        provide the Two-factor Authentication code to GitHub when necessary
    :returns: :class:`GitHubEnterprise <github3.github.GitHubEnterprise>`

    """
    if not url:
        raise ValueError(
            "GitHub Enterprise requires you provide the URL of"
            " the instance"
        )

    g = None

    if (username and password) or token:
        g = GitHubEnterprise(url)
        g.login(username, password, token, two_factor_callback)

    return g
