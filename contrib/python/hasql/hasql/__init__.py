__version_info__ = (0, 8, 1)
__version__ = ".".join(map(str, __version_info__))

package_info = (
    "hasql is a module for acquiring actual connections with masters "
    "and replicas"
)

authors = (
    ("Vladislav Bakaev", "vlad@bakaev.tech"),
    ("Dmitry Orlov", "me@mosquito.su"),
    ("Pavel Mosein", "me@pavkazzz.ru"),
)

authors_email = ", ".join(email for _, email in authors)

__license__ = "Apache 2"
__author__ = ", ".join(f"{name} <{email}>" for name, email in authors)

__maintainer__ = __author__

__all__ = (
    "__author__",
    "__license__",
    "__maintainer__",
    "__version__",
)
