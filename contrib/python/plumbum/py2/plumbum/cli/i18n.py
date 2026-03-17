# -*- coding: utf-8 -*-
import locale

# High performance method for English (no translation needed)
loc = locale.getlocale()[0]
if loc is None or loc.startswith("en"):

    class NullTranslation(object):
        def gettext(self, str):
            return str

        def ngettext(self, str1, strN, n):
            if n == 1:
                return str1.replace("{0}", str(n))
            else:
                return strN.replace("{0}", str(n))

    def get_translation_for(package_name):
        return NullTranslation()

else:
    import gettext
    import os

    # If not installed with setuptools, this might not be available
    try:
        import pkg_resources
    except ImportError:
        pkg_resources = None

    try:
        from typing import Callable, List, Tuple
    except ImportError:
        pass

    local_dir = os.path.basename(__file__)

    def get_translation_for(package_name):  # type: (str) -> gettext.NullTranslations
        """Find and return gettext translation for package
        (Try to find folder manually if setuptools does not exist)
        """

        if "." in package_name:
            package_name = ".".join(package_name.split(".")[:-1])
        localedir = None

        if pkg_resources is None:
            mydir = os.path.join(local_dir, "i18n")
        else:
            mydir = pkg_resources.resource_filename(package_name, "i18n")

        for localedir in mydir, None:
            localefile = gettext.find(package_name, localedir)
            if localefile:
                break

        return gettext.translation(package_name, localedir=localedir, fallback=True)
