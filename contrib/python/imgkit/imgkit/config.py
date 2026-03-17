# -*- coding: utf-8 -*-
import subprocess
from subprocess import CalledProcessError

from six import raise_from


class Config:

    """Config class to configure wkhtmltoimage, xvfb-run and meta tag prefix"""

    def __init__(self, wkhtmltoimage="", xvfb="", meta_tag_prefix="imgkit-"):
        """
        Configure wkhtmltoimage, xvfb, meta_tag_prefix.

        :param wkhtmltoimage: wkhtmltoimage path
        :param xvfb: xvfb path
        :param meta_tag_prefix: the prefix for `imgkit` specific meta tags - by default this is `imgkit-`
        """
        self.wkhtmltoimage = wkhtmltoimage
        self.xvfb = xvfb
        self.meta_tag_prefix = meta_tag_prefix

    def get_wkhtmltoimage(self):
        """Get wkhtmltoimage binary path"""

        if not self.wkhtmltoimage:
            # get wkhtmltoimage in *nix/windows server
            # see https://github.com/jarrekk/imgkit/issues/57 for windows condition
            for find_cmd in ("where", "which"):
                try:
                    self.wkhtmltoimage = (
                        subprocess.check_output([find_cmd, "wkhtmltoimage"])
                        .strip()
                        .decode("utf-8")
                    )
                    break
                except CalledProcessError:
                    self.wkhtmltoimage = "command not found"
                except OSError:
                    self.wkhtmltoimage = "command not found"

        wkhtmltoimage_error = """
No wkhtmltoimage executable found: "{0}"\nIf this file exists please check that this process can read it.
Otherwise please install wkhtmltopdf - http://wkhtmltopdf.org\n
        """.format(
            self.wkhtmltoimage
        )

        if self.wkhtmltoimage != "command not found":
            try:
                with open(self.wkhtmltoimage):
                    pass
            except IOError as io_error:
                raise_from(OSError(wkhtmltoimage_error), io_error)
        else:
            raise OSError(wkhtmltoimage_error)

        return self.wkhtmltoimage

    def get_xvfb(self):
        """Get xvfb-run binary path"""

        if not self.xvfb:
            # get xvfb in *nix/windows server
            # see https://github.com/jarrekk/imgkit/issues/57 for windows condition
            for find_cmd in ("where", "which"):
                try:
                    self.xvfb = (
                        subprocess.check_output([find_cmd, "xvfb-run"])
                        .strip()
                        .decode("utf-8")
                    )
                    break
                except CalledProcessError:
                    self.xvfb = "command not found"
                except OSError:
                    self.xvfb = "command not found"

        xvfb_error = """
No xvfb executable found: "{0}"\nIf this file exists please check that this process can read it.
Otherwise please install xvfb.\n
        """.format(
            self.xvfb
        )

        if self.xvfb != "command not found":
            try:
                with open(self.xvfb):
                    pass
            except IOError as io_error:
                raise_from(OSError(xvfb_error), io_error)
        else:
            raise OSError(xvfb_error)
        return self.xvfb
