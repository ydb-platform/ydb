import logging
import os
from abc import ABCMeta, abstractmethod

import lxml.etree
from lark import Lark
from .. import PROJECT_ROOT
from ..grammar.asciimath_grammar import asciimath_grammar
from ..grammar.latex_grammar import latex_grammar
from ..parser.parser import MathMLParser
from ..transformer.transformer import (
    ASCIIMath2TexTransformer,
    ASCIIMath2MathMLTransformer,
    Tex2ASCIIMathTransformer,
)
from ..utils.utils import check_connection

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


class Translator(metaclass=ABCMeta):
    """Abstract Translator class

    Abstract class of type Translator. Every subclass must implement
    the `_translate(self, exp, **kwargs)` method in order to
    correctly expose the `translate(exp, **kwargs)` method
    """

    def _from_file(self, from_file):
        if os.path.exists(from_file):
            logging.info("Loading file '" + from_file + "'...")
            with open(from_file) as f:
                exp = f.read()
                f.close()
            return exp
        else:
            raise FileNotFoundError("File '" + from_file + "' not found")

    def _to_file(self, exp, to_file):
        logging.info("Writing translation to '" + to_file + "'...")
        with open(to_file, "w") as f:
            f.write(exp)
            f.close()

    @abstractmethod
    def _translate(self, exp, **kwargs):
        pass

    def translate(self, exp, from_file=False, to_file=None, **kwargs):
        """Translates an input expression s

        Args:
            exp (str): String to translate. If from_file is True, then s
                must represent the file's path
            from_file (bool, optional): If True, load the string to translate
                from the file specified by s. Defaults to False.
            to_file (str, optional): If specified, save the translation to
                `to_file`. Defaults to None.

        Returns:
            str: Translated expression
        """
        if from_file:
            exp = self._from_file(exp)
        logging.info("Translating...")
        exp = self._translate(exp, **kwargs)
        if to_file is not None:
            self._to_file(exp, to_file)
        return exp


class LarkTranslator(Translator):
    """Class that handle the translation from a Lark parsed
    language to the one specified by a Transformer.

    A LarkTranslator translates a string, parsed with Lark, into another
    language, specified by the `transformer` parameter

    Args:
        grammar (str): BNF grammar to parse the input
        transformer (lark.Transformer): A transformer instance to transform
            parsed input. See :class:`~lark.Transformer`
        inplace (bool, optional): If True, parse the input inplace.
            See :class:`~lark.Lark`. Defaults to True.
        lexer (str, optional): Lexer used during parsing. See :class:`~lark.Lark`.
            Defaults to "contextual".
        parser (str, optional): Parser algorithm. See :class:`~lark.Lark`.
            Defaults to "lalr".
        **kwargs: Additional keyword arguments to the :class:`~lark.Lark` class.
    """

    def __init__(
        self,
        grammar,
        transformer,
        inplace=True,
        lexer="contextual",
        parser="lalr",
        **kwargs
    ):
        super(LarkTranslator, self).__init__()
        self.inplace = inplace
        self.grammar = grammar
        self.transformer = transformer
        if inplace:
            kwargs.update({"transformer": transformer})
        self.parser = Lark(grammar, parser=parser, lexer=lexer, **kwargs)

    def _translate(self, exp, pprint=False):
        if not self.inplace:
            parsed = self.parser.parse(exp)
            if pprint:
                print(parsed.pretty())
            return self.transformer.transform(parsed)
        else:
            return self.parser.parse(exp)

    def translate(
        self, exp, from_file=False, to_file=None, pprint=False, **kwargs
    ):
        """Translates an input expression exp applying the transformation
        specified by `self.transformer`

        Args:
            exp (str): String to translate. If from_file is True, then s
                must represent the file's path
            from_file (bool, optional): If True, load the string to translate
                from the file specified by s. Defaults to False.
            to_file (str, optional): If specified, save the translation to
                `to_file`. Defaults to None.
            pprint (bool, optional): Abstract Syntax Tree pretty print.
                Defaults to False.

        Returns:
            str: Translated expression
        """
        return super(LarkTranslator, self).translate(
            exp, from_file=from_file, to_file=to_file, pprint=pprint, **kwargs
        )


class ASCIIMath2Tex(LarkTranslator):
    """Class that handle the translation from ASCIIMath to LaTeX

    Args:
        inplace (bool, optional): If True, parse the input inplace.
            See :class:`~lark.Lark`. Defaults to True.
        lexer (str, optional): Lexer used during parsing. See :class:`~lark.Lark`.
            Defaults to "contextual".
        log (bool, optional): If True log the parsing process.
            Defaults to False.
        parser (str, optional): Parser algorithm. See :class:`~lark.Lark`.
            Defaults to "lalr".
        **kwargs: Additional keyword arguments to the :class:`~lark.Lark` class.
    """

    def __init__(self, log=False, **kwargs):
        super(ASCIIMath2Tex, self).__init__(
            asciimath_grammar, ASCIIMath2TexTransformer(log=log), **kwargs
        )

    def _translate(self, exp, displaystyle=False, pprint=False):
        if displaystyle:
            return (
                "\\["
                + super(ASCIIMath2Tex, self)._translate(exp, pprint=pprint)
                + "\\]"
            )
        else:
            return (
                "$"
                + super(ASCIIMath2Tex, self)._translate(exp, pprint=pprint)
                + "$"
            )

    def translate(
        self,
        exp,
        displaystyle=False,
        from_file=False,
        pprint=False,
        to_file=None,
    ):
        """Translates an ASCIIMath string to LaTeX

        Args:
            exp (str): String to translate. If from_file is True, then s
                must represent the file's path
            displaystyle (bool, optional): Add displaystyle attribute.
                Defaults to False.
            from_file (bool, optional): If True, load the string to translate
                from the file specified by s. Defaults to False.
            pprint (bool, optional): Abstract Syntax Tree pretty print.
                Defaults to False.
            to_file (str, optional): If specified, save the translation to
                `to_file`. Defaults to None.

        Returns:
            str: LaTeX translated expression
        """
        return super(ASCIIMath2Tex, self).translate(
            exp,
            displaystyle=displaystyle,
            from_file=from_file,
            pprint=pprint,
            to_file=to_file,
        )


class ASCIIMath2MathML(LarkTranslator):
    """Class that handle the translation from ASCIIMath to MathML

    Args:
        inplace (bool, optional): If True, parse the input inplace.
            See :class:`~lark.Lark`. Defaults to True.
        lexer (str, optional): Lexer used during parsing. See :class:`~lark.Lark`.
            Defaults to "contextual".
        log (bool, optional): If True log the parsing process.
            Defaults to False.
        parser (str, optional): Parser algorithm. See :class:`~lark.Lark`.
            Defaults to "lalr".
        **kwargs: Additional keyword arguments to the :class:`~lark.Lark` class.
    """

    def __init__(self, log=False, **kwargs):
        super(ASCIIMath2MathML, self).__init__(
            asciimath_grammar, ASCIIMath2MathMLTransformer(log=log), **kwargs
        )
        self.__output = ["string", "etree"]

    def _translate(
        self,
        exp,
        displaystyle=False,
        dtd=None,
        dtd_validation=False,
        output="string",
        network=False,
        pprint=False,
        xml_declaration=False,
        xml_pprint=True,
        **kwargs
    ):
        if output not in self.__output:
            raise NotImplementedError(
                "Possible output are: " + ", ".join(self.__output)
            )
        if displaystyle:
            dstyle = '<mstyle displaystyle="true">{}</mstyle>'
        else:
            dstyle = "{}"
        if network and not check_connection():
            network = False
            logging.warning("No connection available...")
        parsed = (
            (
                '<math xmlns="http://www.w3.org/1998/Math/MathML">'
                if dtd != "mathml1"
                else "<math>"
            )
            + dstyle.format(
                super(ASCIIMath2MathML, self)._translate(exp, pprint=pprint)
            )
            + "</math>"
        )
        if (
            dtd_validation
            or xml_pprint
            or xml_declaration
            or output == "etree"
        ):
            parsed = MathMLParser.parse(
                parsed,
                dtd=dtd,
                dtd_validation=True,
                network=network,
                **kwargs
            )
            if output == "string":
                parsed = parsed.getroottree()
                encoding = parsed.docinfo.encoding
                parsed = lxml.etree.tostring(
                    parsed,
                    pretty_print=xml_pprint,
                    xml_declaration=xml_declaration,
                    encoding=encoding,
                ).decode(encoding)
        return parsed

    def translate(
        self,
        exp,
        displaystyle=False,
        dtd=None,
        dtd_validation=False,
        from_file=False,
        output="string",
        network=False,
        pprint=False,
        to_file=None,
        xml_declaration=False,
        xml_pprint=True,
        **kwargs
    ):
        """Translates an ASCIIMath string to MathML

        Args:
            exp (str): String to translate. If from_file is True, then s
                must represent the file's path
            displaystyle (bool, optional): Add displaystyle attribute.
                Defaults to False.
            dtd (str, optional): MathML DTD version to validate the output
                against. It can be: `mathml1`, `mathml2` or `mathml3`.
                Defaults to None.
            dtd_validation (bool, optional): If True validate output against
                the DTD version specified by `dtd`. By default, if one of
                `dtd`, `dtd_validation`, `xml_declaration` or `xml_pprint` is
                True or is not None, then `dtd_validation` will be set to True.
                This is because if either one of them set to True, then
                py_asciimath must parse the input XML, but in order to do that
                it needs to know how to intepret the entities from the DTD.
                Defaults to False.
            from_file (bool, optional): If True, load the string to translate
                from the file specified by s. Defaults to False.
            network (bool, optional): If True validate the output against
                a remote DTD.
                Defaults to False.
            output (str, optional): Output mode: `string` to return the string
                representation of the MathML-converted exxpression, `etree` to
                return a `lxml.etree.ElementTree` object.
                Defaults to False.
            pprint (bool, optional): Abstract Syntax Tree pretty print.
                Defaults to False.
            to_file (str, optional): If specified, save the translation to
                `to_file`.
                Defaults to None.
            xml_declaration (bool, optional): If True, include the XML
                declaration at the beginning of the file.
                Defaults to False.
            xml_pprint (bool, optional): XML pretty print. Defaults to True.
            **kwargs: Additional ~lxml.extree.XMLParser options

        Returns:
            str: MathML translated expression
        """
        return super(ASCIIMath2MathML, self).translate(
            exp,
            displaystyle=displaystyle,
            dtd=dtd,
            dtd_validation=dtd_validation,
            from_file=from_file,
            output=output,
            network=network,
            pprint=pprint,
            to_file=to_file,
            xml_declaration=xml_declaration,
            xml_pprint=xml_pprint,
            **kwargs
        )


class Tex2ASCIIMath(LarkTranslator):
    """Class that handle the translation from LaTeX to ASCIIMath

    Args:
        inplace (bool, optional): If True, parse the input inplace.
            See :class:`~lark.Lark`. Defaults to True.
        lexer (str, optional): Lexer used during parsing. See :class:`~lark.Lark`.
            Defaults to "contextual".
        log (bool, optional): If True log the parsing process.
            Defaults to False.
        parser (str, optional): Parser algorithm. See :class:`~lark.Lark`.
            Defaults to "lalr".
        **kwargs: Additional keyword arguments to the :class:`~lark.Lark` class.
    """

    def __init__(self, log=False, **kwargs):
        super(Tex2ASCIIMath, self).__init__(
            latex_grammar, Tex2ASCIIMathTransformer(log=log), **kwargs
        )

    def _translate(self, exp, pprint=False):
        return super(Tex2ASCIIMath, self)._translate(exp, pprint=pprint)

    def translate(
        self, exp, from_file=False, pprint=False, to_file=None,
    ):
        """Translates an ASCIIMath string to LaTeX

        Args:
            exp (str): String to translate. If from_file is True, then s
                must represent the file's path
            from_file (bool, optional): If True, load the string to translate
                from the file specified by s. Defaults to False.
            pprint (bool, optional): Abstract Syntax Tree pretty print.
                Defaults to False.
            to_file (str, optional): If specified, save the translation to
                `to_file`. Defaults to None.

        Returns:
            str: LaTeX translated expression
        """
        return super(Tex2ASCIIMath, self).translate(
            exp, from_file=from_file, pprint=pprint, to_file=to_file,
        )


class MathML2Tex(Translator):  # pragma: no cover
    """Class that handle the translation from MathML to LaTeX

    The translation from MathML to LaTeX is done via the XSLT provided by
    https://sourceforge.net/projects/xsltml/
    """

    def __init__(self):
        super(MathML2Tex, self).__init__()
        transformer = lxml.etree.parse(
            open(PROJECT_ROOT + "/translation/mathml2tex/mmltex.xsl", "rb")
        )
        self.transformer = lxml.etree.XSLT(transformer)

    def _translate(self, exp, network=False, **kwargs):
        if network and not check_connection():
            network = False
            logging.warning("No connection available...")
        mml_version = MathMLParser.get_doctype_version(exp)
        if mml_version == "1":
            raise NotImplementedError(
                "Translation from MathML1 is not supported"
            )
        parsed = MathMLParser.parse(
            exp,
            dtd_validation=True,
            network=network,
            resolve_entities=True,
            **kwargs
        )
        return str(self.transformer(parsed))

    def translate(
        self, exp, from_file=False, network=False, to_file=None, **kwargs
    ):
        """Translates a MathML string to LaTeX

        Args:
            exp (str): String to translate. If from_file is True, then s
                must represent the file's path
            from_file (bool, optional): If True, load the string to translate
                from the file specified by s. Defaults to False.
            network (bool, optional): If True validate the output against
                a remote DTD.
                Defaults to False.
            to_file (str, optional): If specified, save the translation to
                `to_file`. Defaults to None.
            **kwargs: ~lxml.extree.XMLParser options

        Returns:
            str: LaTeX translated expression
        """
        return super(MathML2Tex, self).translate(
            exp,
            from_file=from_file,
            network=network,
            to_file=to_file,
            **kwargs
        )
