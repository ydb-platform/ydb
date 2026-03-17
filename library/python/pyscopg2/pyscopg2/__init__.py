try:
    from .version import __version__, version_info
except ImportError:
    version_info = (0, 0, 0, "a")
    __version__ = "{}.{}.{}+{}".format(*version_info)

package_info = (
    "Pyscopg2 is a module for acquiring actual connections "
    "with masters and replicas"
)

authors = (("Vladislav Bakaev", "bakaev-vlad@edadeal.ru"),)

authors_email = ", ".join(email for _, email in authors)

__license__ = ("Proprietary License",)
__author__ = ", ".join(f"{name} <{email}>" for name, email in authors)

__maintainer__ = __author__

__all__ = (
    "__author__",
    "__license__",
    "__maintainer__",
    "__version__",
    "version_info",
)
