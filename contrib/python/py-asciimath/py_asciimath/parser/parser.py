import logging
import re

import lxml.etree

from .. import PROJECT_ROOT

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


class MathMLParser(object):

    xml_decl_pattern = re.compile(
        r"(\s*)(<\?xml.*?(encoding=(?:'|\")(.*?)(?:'|\"))?\?>)"
    )
    doctype_pattern = re.compile(
        r"(<!DOCTYPE math ([A-Z]+).*?mathml(\d)?\.dtd\">)", re.MULTILINE
    )

    def __init__(self, *args, **kwargs):  # pragma: no cover
        super(MathMLParser, self).__init__()

    @classmethod
    def get_encoding(cls, s):
        r"""Get the encoding from the XML declaration

        The XML declaration is supposed to match the following pattern: `(<\\?xml.*?(encoding=(?:'|\\")(.*?)(?:'|\\"))?\\?>)`.
        If the XML declaration is not found, then `None` will be returned

        Args:
            s (str): XML string

        Raises:
            Exception: If the XML declaration is not at the beginning of the string

        Returns:
            str: Encoding of the XML document
        """
        xml_decl_match = list(re.finditer(cls.xml_decl_pattern, s))
        if len(xml_decl_match) > 1:
            raise Exception("Multiple XML declarations found")
        elif len(xml_decl_match) == 1:
            xml_decl_match = xml_decl_match[0]
            if xml_decl_match.span()[0] != 0:
                raise Exception(
                    "XML declaration must be at the beginning of the file"
                )
            encoding = xml_decl_match.group(4)
            logging.info("Encoding from XML declaration: " + encoding)
        else:
            encoding = None
            logging.warning(
                "No XML declaration with 'encoding' attribute set: "
                "default encoding to None"
            )
        return encoding

    @classmethod
    def set_doctype(cls, s, network, dtd=None):
        r"""Set MathML DOCTYPE of the XML document

        The DOCTYPE field is supposed to match the following
        pattern: `(<!DOCTYPE math ([A-Z]+).*?mathml(\\d)?\\.dtd\\">)`.
        If the MathML DOCTYPE is not found, then the MathML3 DTD
        will be returned

        Args:
            s (str): XML string
            network (bool): If True, return the PUBLIC MathML DTD;
                otherwise use the local MathML DTD
            dtd (str): Version of the default MathML DTD, If the DTD is not
                found. If None will be used MathML3 DTD
                Defaults to None

        Raises:
            Exception: If the XML declaration is not at the beginning
                of the string

        Returns:
            str, str: s replaced with the DTD following the specifications
                and the MathML version
        """
        doctype_match = list(re.finditer(cls.doctype_pattern, s))
        if len(doctype_match) > 1:
            raise Exception("Multiple DOCTYPE declarations found")
        elif doctype_match != []:
            doctype_match = doctype_match[0]
            if (
                doctype_match.group(2) == "PUBLIC"
                or (
                    doctype_match.group(2) == "SYSTEM"
                    and "http" in doctype_match.group(1)
                )
            ) and not network:
                logging.warning(
                    "Remote DTD found and network is False: "
                    "replacing with local DTD"
                )
                mml_version = doctype_match.group(3)
                s = (
                    s[: doctype_match.span(1)[0]]
                    + cls.get_doctype(
                        "mathml"
                        + (mml_version if mml_version is not None else "1"),
                        False,
                    )
                    + s[doctype_match.span(1)[1] :]
                )
            elif doctype_match.group(2) == "SYSTEM" and network:
                logging.warning(
                    "Local DTD found and network is True: "
                    "no need to bother your ISP"
                )
        else:
            dtd = dtd if dtd is not None else "mathml3"
            logging.warning(
                "No DTD declaration found: "
                "set to {} {} DTD".format(
                    "remote" if network else "local", dtd
                )
            )
            xml_decl_match = list(re.finditer(cls.xml_decl_pattern, s))
            if len(xml_decl_match) > 1:
                raise Exception("Multiple XML declarations found")
            elif len(xml_decl_match) == 1:
                xml_decl_match = xml_decl_match[0]
                if xml_decl_match.span()[0] != 0:
                    raise Exception(
                        "XML declaration must be at the beginning of the file"
                    )
                start = xml_decl_match.span()[1]
            else:
                start = 0
            doctype = cls.get_doctype(dtd, network)
            s = s[:start] + doctype + s[start:]
        return s

    @classmethod
    def get_doctype_version(cls, xml):
        """Get the MathML DTD version from DOCTYPE declaration

        Args:
            xml (str): XML document

        Raises:
            Exception: If multiple DOCTYPE declarations are found

        Returns:
            str: MathML DTD version (1,2,3); None if DOCTYPE is not found
        """
        doctype_match = list(re.finditer(cls.doctype_pattern, xml))
        if len(doctype_match) > 1:
            raise Exception("Multiple DOCTYPE declarations found")
        elif doctype_match != []:
            version = doctype_match[0].group(3)
            return version if version is not None else "1"
        else:
            return None

    @staticmethod
    def get_doctype(dtd, network):
        """Get a MathML DOCTYPE declaration

        Args:
            dtd (str): MathML DTD type. Must be on of the following:
                `mathml1`, `mathml2` or `mathml3`
            network (bool): If True, get a public DTD; otherwise a local
                DTD declaration will be set

        Raises:
            NotImplementedError: Other DTD declarations but MathML
                will be discarded

        Returns:
            str: DOCTYPE declaration following the specifics
        """
        doctype = "<!DOCTYPE math {}>"
        if dtd is None or dtd.lower() == "mathml3":
            doctype = doctype.format(
                'PUBLIC "-//W3C//DTD MathML 3.0//EN" '
                + '"http://www.w3.org/Math/DTD/mathml3/mathml3.dtd"'
                if network
                else "SYSTEM "
                + '"'
                + PROJECT_ROOT
                + '/dtd/mathml3/mathml3.dtd"'
            )
        elif dtd.lower() == "mathml1":
            doctype = doctype.format(
                "SYSTEM "
                + (
                    '"http://www.w3.org/Math/DTD/mathml1/mathml.dtd"'
                    if network
                    else '"' + PROJECT_ROOT + '/dtd/mathml1/mathml1.dtd"'
                )
            )
        elif dtd.lower() == "mathml2":
            doctype = doctype.format(
                'PUBLIC "-//W3C//DTD MathML 2.0//EN" '
                + '"http://www.w3.org/Math/DTD/mathml2/mathml2.dtd"'
                if network
                else "SYSTEM "
                + '"'
                + PROJECT_ROOT
                + '/dtd/mathml2/mathml2.dtd"'
            )
        else:
            raise NotImplementedError(
                "DTD validation only against MathML DTD 1, 2 or 3"
            )
        return doctype

    @staticmethod
    def get_parser(
        dtd_validation=True,
        network=False,
        ns_clean=True,
        resolve_entities=False,
        **kwargs
    ):  # pragma: no cover
        """Create a MathML XML parser

        Args:
            dtd_validation (bool, optional): Validate XML against DTD during
                parsing. Defaults to True.
            network (bool, optional): Validate against remote DTD.
                Defaults to False.
            ns_clean (bool, optional): Clean up redundant namespace
                declarations. Defaults to True.
            resolve_entities (bool, optional): replace entities by their text
                value. Defaults to False.
            **kwargs: Additional ~lxml.extree.XMLParser options

        Returns:
            lxml.etree.XMLParser: MathML parser following the specifications
        """
        return lxml.etree.XMLParser(
            dtd_validation=dtd_validation,
            no_network=(not network),
            resolve_entities=resolve_entities,
            ns_clean=ns_clean,
            **kwargs
        )

    @staticmethod
    def parse(
        xml,
        dtd=None,
        dtd_validation=True,
        from_file=False,
        network=False,
        ns_clean=True,
        resolve_entities=False,
        **kwargs
    ):  # pragma: no cover
        """Parse a MathML XML

        Args:
            xml (str): String representing a MathML XML.
            dtd (str): MathML DTD type. Must be on of the following:
                `mathml1`, `mathml2` or `mathml3`
            dtd_validation (bool, optional): Validate XML against DTD during
                parsing. Defaults to True.
            from_file (bool, optional): If True, load the XML file from the
                path specified by `xml`.
                Defaults to False
            network (bool, optional): Validate against remote DTD.
                Defaults to False.
            ns_clean (bool, optional): Clean up redundant namespace
                declarations. Defaults to True.
            resolve_entities (bool, optional): replace entities by their text
                value. Defaults to False.
            **kwargs: Additional ~lxml.extree.XMLParser options

        Returns:
            lxml.etree._Element: Root of the parsed and possibly
                validated MathML XML
        """
        if not from_file:
            encoding = MathMLParser.get_encoding(xml)
            if encoding is None:
                logging.warning("The XML encoding is None: default to UTF-8")
                encoding = "UTF-8"
            if dtd_validation:
                xml = MathMLParser.set_doctype(xml, network, dtd=dtd)
            xml = xml.encode(encoding)
        if dtd_validation:
            logging.info("Loading dtd and validating...")
        mathml_parser = MathMLParser.get_parser(
            dtd_validation=dtd_validation,
            network=network,
            ns_clean=ns_clean,
            resolve_entities=resolve_entities,
            **kwargs
        )
        if from_file:
            return lxml.etree.parse(xml, mathml_parser)
        else:
            return lxml.etree.fromstring(xml, mathml_parser)
