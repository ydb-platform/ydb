# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

import codecs
import os
import re
import sys

from ._compat import escape, PY3, URLError, urllib
from .core import *
from .utils import get_stoplist, get_stoplists


def usage():
    return """Usage: %(progname)s -s STOPLIST [OPTIONS] [HTML_FILE]
Convert HTML to plain text and remove boilerplate.

  -o OUTPUT_FILE   if not specified, output is written to stdout
  --encoding=...   default character encoding to be used if not specified
                   in the HTML meta tags (default: %(default_encoding)s)
  --enc-force      force specified encoding, ignore HTML meta tags
  --enc-errors=... errors handling for character encoding conversion:
                     strict: fail on error
                     ignore: ignore characters which can't be converted
                     replace: replace characters which can't be converted
                              with U+FFFD unicode replacement characters
                   (default: %(default_enc_errors)s)
  --format=...     output format; possible values:
                     default: one paragraph per line, each preceded with
                              <p> or <h> (headings)
                     boilerplate: same as default, except for boilerplate
                                  paragraphs are included, too, preceded
                                  with <b>
                     detailed: one paragraph per line, each preceded with
                               <p> tag containing detailed information
                               about classification as attributes
                     krdwrd: KrdWrd compatible format
  --no-headings    disable special handling of headings
  --list-stoplists print a list of inbuilt stoplists and exit
  -V, --version    print version information and exit
  -h, --help       display this help and exit

If no HTML_FILE specified, input is read from stdin.

STOPLIST must be one of the following:
  - one of the inbuilt stoplists; see:
      %(progname)s --list-stoplists
  - path to a file with the most frequent words for given language,
    one per line, in UTF-8 encoding
  - None - this activates a language-independent mode

Advanced options:
  --length-low=INT (default %(length_low)i)
  --length-high=INT (default %(length_high)i)
  --stopwords-low=FLOAT (default %(stopwords_low)f)
  --stopwords-high=FLOAT (default %(stopwords_high)f)
  --max-link-density=FLOAT (default %(max_link_density)f)
  --max-heading-distance=INT (default %(max_heading_distance)i)
""" % {
    'progname': os.path.basename(os.path.basename(sys.argv[0])),
    'length_low': LENGTH_LOW_DEFAULT,
    'length_high': LENGTH_HIGH_DEFAULT,
    'stopwords_low': STOPWORDS_LOW_DEFAULT,
    'stopwords_high': STOPWORDS_HIGH_DEFAULT,
    'max_link_density': MAX_LINK_DENSITY_DEFAULT,
    'max_heading_distance': MAX_HEADING_DISTANCE_DEFAULT,
    'default_encoding': DEFAULT_ENCODING,
    'default_enc_errors': DEFAULT_ENC_ERRORS,
}


def output_default(paragraphs, fp=sys.stdout, no_boilerplate=True):
    """
    Outputs the paragraphs as:
    <tag> text of the first paragraph
    <tag> text of the second paragraph
    ...
    where <tag> is <p>, <h> or <b> which indicates
    standard paragraph, heading or boilerplate respecitvely.
    """
    for paragraph in paragraphs:
        if paragraph.class_type == 'good':
            if paragraph.heading:
                tag = 'h'
            else:
                tag = 'p'
        elif no_boilerplate:
            continue
        else:
            tag = 'b'

        print('<%s> %s' % (tag, escape(paragraph.text, quote=False)), file=fp)


def output_detailed(paragraphs, fp=sys.stdout):
    """
    Same as output_default, but only <p> tags are used and the following
    attributes are added: class, cfclass and heading.
    """
    for paragraph in paragraphs:
        output = '<p class="%s" cfclass="%s" heading="%i" xpath="%s"> %s' % (
            paragraph.class_type,
            paragraph.cf_class,
            int(paragraph.heading),
            paragraph.xpath,
            escape(paragraph.text, quote=False)
        )
        print(output, file=fp)


def output_krdwrd(paragraphs, fp=sys.stdout):
    """
    Outputs the paragraphs in a KrdWrd compatible format:
    class<TAB>first text node
    class<TAB>second text node
    ...
    where class is 1, 2 or 3 which means
    boilerplate, undecided or good respectively. Headings are output as
    undecided.
    """
    for paragraph in paragraphs:
        if paragraph.class_type in ('good', 'neargood'):
            if paragraph.heading:
                cls = 2
            else:
                cls = 3
        else:
            cls = 1

        for text_node in paragraph.text_nodes:
            print('%i\t%s' % (cls, text_node.strip()), file=fp)


def main():
    import getopt
    from justext import __version__ as VERSION

    try:
        opts, args = getopt.getopt(sys.argv[1:], "o:s:hV", ["encoding=",
            "enc-force", "enc-errors=", "format=",
            "no-headings", "help", "version", "length-low=", "length-high=",
            "stopwords-low=", "stopwords-high=", "max-link-density=",
            "max-heading-distance=", "list-stoplists"])
    except getopt.GetoptError as e:
        print(e, file=sys.stderr)
        print(usage(), file=sys.stderr)
        sys.exit(1)

    stream_writer = codecs.lookup('utf8')[-1]
    fp_in = sys.stdin
    if PY3:
        fp_out = stream_writer(sys.stdout.buffer)
    else:
        fp_out = stream_writer(sys.stdout)
    stoplist = None
    format = 'default'
    no_headings = False
    length_low = LENGTH_LOW_DEFAULT
    length_high = LENGTH_HIGH_DEFAULT
    stopwords_low = STOPWORDS_LOW_DEFAULT
    stopwords_high = STOPWORDS_HIGH_DEFAULT
    max_link_density = MAX_LINK_DENSITY_DEFAULT
    max_heading_distance = MAX_HEADING_DISTANCE_DEFAULT
    encoding = None
    default_encoding = DEFAULT_ENCODING
    force_default_encoding = False
    enc_errors = DEFAULT_ENC_ERRORS

    try:
        for o, a in opts:
            if o in ("-h", "--help"):
                print(usage())
                sys.exit(0)
            if o in ("-V", "--version"):
                print("%s: jusText v%s\n\nCopyright (c) 2011 Jan Pomikalek <jan.pomikalek@gmail.com>" % (
                    os.path.basename(sys.argv[0]), VERSION))
                sys.exit(0)
            elif o == "--list-stoplists":
                print("\n".join(sorted(get_stoplists())))
                sys.exit(0)
            elif o == "-o":
                try:
                    fp_out = codecs.open(a, 'w', 'utf8')
                except IOError as e:
                    raise JustextInvalidOptions(
                        "Can't open %s for writing: %s" % (a, e))
            elif o == "-s":
                if a.lower() == 'none':
                    stoplist = set()
                else:
                    if os.path.isfile(a):
                        try:
                            fp_stoplist = codecs.open(a, 'r', 'utf8')
                            stoplist = set([l.strip() for l in fp_stoplist])
                            fp_stoplist.close()
                        except IOError as e:
                            raise JustextInvalidOptions(
                                "Can't open %s for reading: %s" % (a, e))
                        except UnicodeDecodeError as e:
                            raise JustextInvalidOptions(
                                "Unicode decoding error when reading "
                                "the stoplist (probably not in UTF-8): %s" % e)
                    elif a in get_stoplists():
                        stoplist = get_stoplist(a)
                    else:
                        if re.match(r'^\w*$', a):
                            # only alphabetical chars, probably misspelled or
                            # unsupported language
                            raise JustextInvalidOptions(
                                "Unknown stoplist: %s\nAvailable stoplists:\n%s" % (
                                    a, '\n'.join(sorted(get_stoplists()))))
                        else:
                            # probably incorrectly specified path
                            raise JustextInvalidOptions("File not found: %s" % a)
            elif o == "--encoding":
                try:
                    default_encoding = a
                    ''.encode(default_encoding)
                except LookupError:
                    raise JustextInvalidOptions("Uknown character encoding: %s" % a)
            elif o == "--enc-force":
                force_default_encoding = True
            elif o == "--enc-errors":
                if a.lower() in ['strict', 'ignore', 'replace']:
                    enc_errors = a.lower()
                else:
                    raise JustextInvalidOptions("Invalid --enc-errors value: %s" % a)
            elif o == "--format":
                if a in ['default', 'boilerplate', 'detailed', 'krdwrd']:
                    format = a
                else:
                    raise JustextInvalidOptions("Uknown output format: %s" % a)
            elif o == "--no-headings":
                no_headings = True
            elif o == "--length-low":
                try:
                    length_low = int(a)
                except ValueError:
                    raise JustextInvalidOptions(
                        "Invalid value for %s: '%s'. Integer expected." % (o, a))
            elif o == "--length-high":
                try:
                    length_high = int(a)
                except ValueError:
                    raise JustextInvalidOptions(
                        "Invalid value for %s: '%s'. Integer expected." % (o, a))
            elif o == "--stopwords-low":
                try:
                    stopwords_low = float(a)
                except ValueError:
                    raise JustextInvalidOptions(
                        "Invalid value for %s: '%s'. Float expected." % (o, a))
            elif o == "--stopwords-high":
                try:
                    stopwords_high = float(a)
                except ValueError:
                    raise JustextInvalidOptions(
                        "Invalid value for %s: '%s'. Float expected." % (o, a))
            elif o == "--max-link-density":
                try:
                    max_link_density = float(a)
                except ValueError:
                    raise JustextInvalidOptions(
                        "Invalid value for %s: '%s'. Float expected." % (o, a))
            elif o == "--max-heading-distance":
                try:
                    max_heading_distance = int(a)
                except ValueError:
                    raise JustextInvalidOptions(
                        "Invalid value for %s: '%s'. Integer expected." % (o, a))

        if force_default_encoding:
            encoding = default_encoding

        if stoplist is None:
            raise JustextInvalidOptions("No stoplist specified.")

        if not stoplist:
            # empty stoplist, switch to language-independent mode
            stopwords_high = 0
            stopwords_low = 0

        if args:
            try:
                if re.match(r"[^:/]+://", args[0]):
                    fp_in = urllib.urlopen(args[0])
                else:
                    fp_in = open(args[0], 'r')
            except (IOError, URLError) as e:
                raise JustextInvalidOptions(
                    "Can't open %s for reading: %s" % (args[0], e))

        html_text = fp_in.read()
        if fp_in is not sys.stdin:
            fp_in.close()

        paragraphs = justext(html_text, stoplist, length_low, length_high,
            stopwords_low, stopwords_high, max_link_density, max_heading_distance,
            no_headings, encoding, default_encoding, enc_errors)
        if format == "default":
            output_default(paragraphs, fp_out)
        elif format == "boilerplate":
            output_default(paragraphs, fp_out, no_boilerplate=False)
        elif format == "detailed":
            output_detailed(paragraphs, fp_out)
        elif format == "krdwrd":
            output_krdwrd(paragraphs, fp_out)
        else:
            # this should not happen; format checked when parsing options
            raise AssertionError("Unknown format: %s" % format)

    except JustextError as e:
        print("%s: %s" % (os.path.basename(sys.argv[0]), e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
