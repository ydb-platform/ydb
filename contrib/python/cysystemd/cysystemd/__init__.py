package_info = "systemd wrapper in Cython"
version_info = (2, 0, 1)


author_info = (("Dmitry Orlov", "me@mosquito.su"),)

author_email = ", ".join("{}".format(info[1]) for info in author_info)

license = "Apache 2.0"

__version__ = ".".join(str(x) for x in version_info)
__author__ = ", ".join("{} <{}>".format(*info) for info in author_info)


__all__ = (
    "__author__",
    "__version__",
    "author_info",
    "license",
    "package_info",
    "version_info",
)
