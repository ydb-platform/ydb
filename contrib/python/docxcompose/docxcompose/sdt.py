from lxml.etree import Element
from lxml.etree import QName

from docxcompose.utils import xpath


class StructuredDocumentTags(object):
    """Structured Document Tags (aka Content Controls)"""

    def __init__(self, doc):
        self.doc = doc

    def tags_by_alias(self, alias):
        """Get Structured Document Tags by alias."""
        return xpath(
            self.doc.element.body,
            './/w:sdt/w:sdtPr/w:alias[@w:val="%s"]/ancestor::w:sdt' % alias,
        )

    def set_text(self, alias, text):
        """Set the text content of all Structured Document Tags identified by
        an alias. Only plain text SDTs are supported.

        If the SDT has the 'multiLine' property, newlines in `text` will be
        respected, and the SDTs content will be updated with lines separated
        by line breaks.
        """
        text = text.strip()
        tags = self.tags_by_alias(alias)
        for tag in tags:
            # Ignore if it's not a plain text SDT
            plain_text = xpath(tag, "./w:sdtPr/w:text")
            if not plain_text:
                continue

            nsmap = tag.nsmap
            is_multiline = bool(plain_text[0].xpath("./@w:multiLine", namespaces=nsmap))

            properties = xpath(tag, "./w:sdtPr")
            content = xpath(tag, "./w:sdtContent")
            if not content:
                continue

            run_elements = xpath(content[0], ".//w:r")
            if not run_elements:
                continue

            # First, prepare the SDT for easy updating of its value.
            #
            # We do this by cleaning out the SDT content to only preserve
            # the first of possibly many runs, and remove the contents of
            # that run (except w:rPr formatting properties).
            #
            # That run can then be filled with new text nodes and line breaks
            # as needed. This should allow us to preserve formatting, but
            # otherwise start from a clean slate where we create new nodes
            # instead of having to carefully update an existing structure.

            first_run = run_elements[0]
            self._remove_placeholder(properties, content, first_run)
            self._remove_all_runs_except_first(run_elements)
            self._clean_first_run(first_run)

            # Now update contents by appending new text nodes.
            #
            # If the SDT has the multiLine property, we respect newlines
            # in the input value string and create text nodes delimited by
            # line breaks.
            if not is_multiline:
                text = text.replace("\n", " ")

            lines = text.splitlines()
            for i, line in enumerate(lines, start=1):
                txt_node = Element(QName(nsmap["w"], "t"))
                txt_node.text = line
                first_run.append(txt_node)

                if i != len(lines):
                    br = Element(QName(nsmap["w"], "br"))
                    first_run.append(br)

    def _remove_placeholder(self, properties, content, first_run):
        """Remove placeholder marker and style."""
        showing_placeholder = xpath(properties[0], "./w:showingPlcHdr")
        if showing_placeholder:
            properties[0].remove(showing_placeholder[0])
            run_props = xpath(first_run, "./w:rPr")
            if run_props:
                first_run.remove(run_props[0])

    def _remove_all_runs_except_first(self, run_elements):
        """Remove all runs except the first one."""
        for run in run_elements[1:]:
            run.getparent().remove(run)

    def _clean_first_run(self, first_run):
        """Remove all elements from the first run except run formatting."""
        for child in first_run.getchildren():
            # Preserve formatting
            if QName(child).localname == "rPr":
                continue
            first_run.remove(child)

    def get_text(self, alias):
        """Get the text content of the first Structured Document Tag identified
        by the given alias.
        """
        tags = self.tags_by_alias(alias)
        for tag in tags:
            # Ignore if it's not a plain text SDT
            if not xpath(tag, "./w:sdtPr/w:text"):
                continue

            tokens = []
            text_and_brs = xpath(tag, "./w:sdtContent//w:r/*[self::w:t or self::w:br]")
            for el in text_and_brs:
                if QName(el).localname == "t":
                    tokens.append(el.text)
                elif QName(el).localname == "br":
                    tokens.append("\n")

            return "".join(tokens)
