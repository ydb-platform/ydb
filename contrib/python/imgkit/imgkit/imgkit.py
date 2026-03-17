# -*- coding: utf-8 -*-
import codecs
import io
import re
import subprocess
import sys

from six import raise_from, string_types

from .config import Config
from .source import Source


class IMGKit:

    """Main class for imgkit"""

    class SourceError(Exception):

        """Wrong source type for stylesheets"""

        def __init__(self, message):
            """SourceError message"""

            Exception.__init__(self)
            self.message = message

    def __init__(self, url_or_file, source_type, options=None, config=None, **kwargs):
        """Deliver parameters into IMGkit"""

        params = ["toc", "cover", "cover_first", "css"]
        for param in params:
            setattr(self, param, kwargs.get(param, None))
        self.source = Source(url_or_file, source_type)
        self.config = Config() if not config else config
        self.wkhtmltoimage = self.config.get_wkhtmltoimage()

        self.options = {}
        if self.source.isString():
            self.options.update(self._find_options_in_meta(url_or_file))

        if options:
            self.options.update(options)

        if options and "xvfb" in options:
            self.xvfb = self.config.get_xvfb()

        self.toc = self.toc if self.toc else {}
        self.cover = self.cover
        self.cover_first = self.cover_first
        self.css = self.css
        self.stylesheets = []

    def _gegetate_args(self, options):
        """Generator of args parts based on options specification."""

        for optkey, optval in self._normalize_options(options):
            yield optkey

            if isinstance(optval, (list, tuple)):
                assert (
                    len(optval) == 2 and optval[0] and optval[1]
                ), "Option value can only be either a string or a (tuple, list) of 2 items"
                yield optval[0]
                yield optval[1]
            else:
                yield optval

    def _command(self, path=None):
        """Generator of all command parts"""

        options = self._gegetate_args(self.options)
        options = [x for x in options]
        if self.css:
            self._prepend_css(self.css)

        if "--xvfb" in options:
            options.remove("--xvfb")
            yield self.xvfb
            # auto servernum option to prevent failure on concurrent runs
            # https://bugs.launchpad.net/ubuntu/+source/xorg-server/+bug/348052
            yield "-a"

        yield self.wkhtmltoimage

        for argpart in options:
            if argpart:
                yield argpart

        if self.cover and self.cover_first:
            yield "cover"
            yield self.cover

        if self.toc:
            yield "toc"
            for argpart in self._gegetate_args(self.toc):
                if argpart:
                    yield argpart

        if self.cover and not self.cover_first:
            yield "cover"
            yield self.cover

        # If the source is a string then we will pipe it into wkhtmltoimage
        # If the source is file-like then we will read from it and pipe it in
        if self.source.isString() or self.source.isFileObj():
            yield "-"
        else:
            if isinstance(self.source.source, string_types):
                yield self.source.to_s()
            else:
                for s in self.source.source:
                    yield s

        # If output_path evaluates to False append '-' to end of args
        # and wkhtmltoimage will pass generated IMG to stdout
        if path:
            yield path
        else:
            yield "-"

    def command(self, path=None):
        """Generate command"""

        return list(self._command(path))

    def _normalize_options(self, options):
        """
        Generator of 2-tuples (option-key, option-value).
        When options spec is a list, generate a 2-tuples per list item.

        :param options: dict {option: value}

        returns:
          iterator (option-key, option-value)
          - option names lower cased and prepended with
          '--' if necessary. Non-empty values cast to str
        """

        for key, value in list(options.items()):
            if "--" in key:
                normalized_key = self._normalize_arg(key)
            else:
                normalized_key = "--{}".format(self._normalize_arg(key))

            if isinstance(value, (list, tuple)):
                for opt_val in value:
                    yield normalized_key, opt_val
            else:
                yield normalized_key, str(value) if value else value

    @staticmethod
    def _normalize_arg(arg):
        return arg.lower()

    @staticmethod
    def _style_tag(stylesheet):
        return "<style>{}</style>".format(stylesheet)

    def _prepend_css(self, path):
        if self.source.isUrl() or isinstance(self.source.source, list):
            raise self.SourceError(
                "CSS files can be added only to a single file or string"
            )

        if not isinstance(path, list):
            path = [path]

        css_data = []
        for css_file in path:
            with codecs.open(css_file, encoding="UTF-8") as f:
                css_data.append(f.read())
        css_data = "\n".join(css_data)

        if self.source.isFile():
            with codecs.open(self.source.to_s(), encoding="UTF-8") as f:
                inp = f.read()
            self.source = Source(
                inp.replace("</head>", self._style_tag(css_data) + "</head>"), "string"
            )

        elif self.source.isString():
            if "</head>" in self.source.to_s():
                self.source.source = self.source.to_s().replace(
                    "</head>", self._style_tag(css_data) + "</head>"
                )
            else:
                self.source.source = self._style_tag(css_data) + self.source.to_s()

    def _find_options_in_meta(self, content):
        """Reads 'content' and extracts options encoded in HTML meta tags

        :param content: str or file-like object - contains HTML to parse

        returns:
          dict: {config option: value}
        """

        if (
            isinstance(content, io.IOBase)
            or content.__class__.__name__ == "StreamReaderWriter"
        ):
            content = content.read()

        found = {}

        for con in re.findall("<meta [^>]*>", content):
            if re.search("name=[\"']{}".format(self.config.meta_tag_prefix), con):
                name = re.findall(
                    "name=[\"']{}([^\"']*)".format(self.config.meta_tag_prefix), con
                )[0]
                found[name] = re.findall("content=[\"']([^\"']*)", con)[0]

        return found

    def to_img(self, path=None):
        """Generate image to path"""

        args = self.command(path)

        result = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # If the source is a string then we will pipe it into wkhtmltoimage.
        # If we want to add custom CSS to file then we read input file to
        # string and prepend css to it and then pass it to stdin.
        # This is a workaround for a bug in wkhtmltoimage (look closely in README)
        if self.source.isString() or (self.source.isFile() and self.css):
            # HTML charset should be UTF-8 as encoding via utf-8
            charset_meta = '<meta charset="UTF-8">'
            string = (charset_meta + self.source.to_s()).encode("utf-8")
        elif self.source.isFileObj():
            string = self.source.source.read().encode("utf-8")
        else:
            string = None
        stdout, stderr = result.communicate(input=string)
        stderr = stderr or stdout
        try: 
            stderr = stderr.decode("utf-8") 
        except UnicodeDecodeError: 
            stderr = ""
        exit_code = result.returncode

        if "cannot connect to X server" in stderr:
            raise OSError(
                "{}\n"
                'You will need to run wkhtmltoimage within a "virtual" X server.\n'
                "Go to the link below for more information\n"
                "http://wkhtmltopdf.org".format(stderr)
            )

        if "Error" in stderr:
            raise OSError("wkhtmltoimage reported an error:\n" + stderr)

        if exit_code != 0:
            xvfb_error = ""
            if "QXcbConnection" in stderr:
                xvfb_error = 'You need to install xvfb(sudo apt-get install xvfb, yum install xorg-x11-server-Xvfb, etc), then add option: {"xvfb": ""}.'
            raise OSError(
                "wkhtmltoimage exited with non-zero code {0}. error:\n{1}\n\n{2}".format(
                    exit_code, stderr, xvfb_error
                )
            )

        # Since wkhtmltoimage sends its output to stderr we will capture it
        # and properly send to stdout
        if "--quiet" not in args:
            sys.stdout.write(stderr)

        if not path:
            return stdout
        try:
            with codecs.open(path, mode="rb") as f:
                text = f.read(4)
                if text == "":
                    raise OSError(
                        "Command failed: {}\n"
                        "Check whhtmltoimage output without "
                        "'quiet' option".format(" ".join(args))
                    )
                return True
        except IOError as io_error:
            raise_from(
                OSError(
                    "Command failed: {0}\n"
                    "Check whhtmltoimage output without "
                    "'quiet' option\n{1} ".format(" ".join(args), io_error)
                ),
                io_error,
            )
