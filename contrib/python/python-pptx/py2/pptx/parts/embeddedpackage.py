# encoding: utf-8

"""Embedded Package part objects.

"Package" in this context means another OPC package, i.e. a DOCX, PPTX, or XLSX "file".
"""

from pptx.enum.shapes import PROG_ID
from pptx.opc.constants import CONTENT_TYPE as CT
from pptx.opc.package import Part


class EmbeddedPackagePart(Part):
    """A distinct OPC package, e.g. an Excel file, embedded in this PPTX package.

    Has a partname like: `ppt/embeddings/Microsoft_Excel_Sheet1.xlsx`.
    """

    @classmethod
    def factory(cls, prog_id, object_blob, package):
        """Return a new |EmbeddedPackagePart| subclass instance added to *package*.

        The subclass is determined by `prog_id` which corresponds to the "application"
        used to open the "file-type" of `object_blob`. The returned part contains the
        bytes of `object_blob` and has the content-type also determined by `prog_id`.
        """
        # --- a generic OLE object has no subclass ---
        if prog_id not in PROG_ID:
            return cls(
                package.next_partname("/ppt/embeddings/oleObject%d.bin"),
                CT.OFC_OLE_OBJECT,
                package,
                object_blob,
            )

        # --- A Microsoft Office file-type is a distinguished package object ---
        EmbeddedPartCls = {
            PROG_ID.DOCX: EmbeddedDocxPart,
            PROG_ID.PPTX: EmbeddedPptxPart,
            PROG_ID.XLSX: EmbeddedXlsxPart,
        }[prog_id]

        return EmbeddedPartCls.new(object_blob, package)

    @classmethod
    def new(cls, blob, package):
        """Return new |EmbeddedPackagePart| subclass object.

        The returned part object contains `blob` and is added to `package`.
        """
        return cls(
            package.next_partname(cls.partname_template),
            cls.content_type,
            package,
            blob,
        )


class EmbeddedDocxPart(EmbeddedPackagePart):
    """A Word .docx file stored in a part.

    This part-type arises when a Word document appears as an embedded OLE-object shape.
    """

    partname_template = "/ppt/embeddings/Microsoft_Word_Document%d.docx"
    content_type = CT.WML_DOCUMENT


class EmbeddedPptxPart(EmbeddedPackagePart):
    """A PowerPoint file stored in a part.

    This part-type arises when a PowerPoint presentation (.pptx file) appears as an
    embedded OLE-object shape.
    """

    partname_template = "/ppt/embeddings/Microsoft_PowerPoint_Presentation%d.pptx"
    content_type = CT.PML_PRESENTATION


class EmbeddedXlsxPart(EmbeddedPackagePart):
    """An Excel file stored in a part.

    This part-type arises as the data source for a chart, but may also be the OLE-object
    for an embedded object shape.
    """

    partname_template = "/ppt/embeddings/Microsoft_Excel_Sheet%d.xlsx"
    content_type = CT.SML_SHEET
