"""
github3
=======

See https://github3.readthedocs.io/ for documentation.

:copyright: (c) 2012-2016 by Ian Cordasco
:license: Modified BSD, see LICENSE for more details

"""
from .__about__ import __author__
from .__about__ import __author_email__
from .__about__ import __copyright__
from .__about__ import __license__
from .__about__ import __package_name__
from .__about__ import __title__
from .__about__ import __url__
from .__about__ import __version__
from .__about__ import __version_info__
from .api import enterprise_login
from .api import login
from .exceptions import GitHubError
from .github import GitHub
from .github import GitHubEnterprise

__all__ = (
    "GitHub",
    "GitHubEnterprise",
    "GitHubError",
    "authorize",
    "login",
    "enterprise_login",
    "emojis",
    "gist",
    "gitignore_template",
    "create_gist",
    "issue",
    "markdown",
    "octocat",
    "organization",
    "pull_request",
    "followers_of",
    "followed_by",
    "public_gists",
    "gists_by",
    "issues_on",
    "gitignore_templates",
    "all_repositories",
    "all_users",
    "all_events",
    "organizations_with",
    "repositories_by",
    "starred_by",
    "subscriptions_for",
    "rate_limit",
    "repository",
    "search_code",
    "search_repositories",
    "search_users",
    "search_issues",
    "user",
    "zen",
    # Metadata attributes
    "__package_name__",
    "__title__",
    "__author__",
    "__author_email__",
    "__license__",
    "__copyright__",
    "__version__",
    "__version_info__",
    "__url__",
)
