# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("PdfDocument", "PdfFormEnv", "PdfXObject", "PdfBookmark", "PdfDest")

import ctypes
import logging
from pathlib import Path

import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i
from pypdfium2.version import PDFIUM_INFO
from pypdfium2._helpers.misc import PdfiumError
from pypdfium2._helpers.page import PdfPage
from pypdfium2._helpers.pageobjects import PdfObject
from pypdfium2._helpers.attachment import PdfAttachment

logger = logging.getLogger(__name__)


class PdfDocument (pdfium_i.AutoCloseable):
    """
    Document helper class.
    
    Parameters:
        input_data (str | pathlib.Path | bytes | ctypes.Array | typing.BinaryIO | FPDF_DOCUMENT):
            The input PDF given as file path, bytes, ctypes array, byte stream, or raw PDFium document handle.
            A byte stream is defined as an object that implements ``seek() tell() read() readinto()``.
        password (str | None):
            A password to unlock the PDF, if encrypted. Otherwise, None or an empty string may be passed.
            If a password is given but the PDF is not encrypted, it will be ignored (as of PDFium 5418).
        autoclose (bool):
            Whether byte stream input should be automatically closed on finalization.
    
    Raises:
        PdfiumError: Raised if the document failed to load. The exception is annotated with the reason reported by PDFium (via message and :attr:`~.PdfiumError.err_code`).
        FileNotFoundError: Raised if an invalid or non-existent file path was given.
    
    Hint:
        * Documents may be used in a ``with``-block, closing the document on context manager exit.
          This is recommended when *input_data* is a file path, to safely and immediately release the bound file handle.
        * :func:`len` may be called to get a document's number of pages.
        * Pages may be loaded using list index access.
        * Looping over a document will yield its pages from beginning to end.
        * The ``del`` keyword and list index access may be used to delete pages.
    
    Attributes:
        raw (FPDF_DOCUMENT):
            The underlying PDFium document handle.
        formenv (PdfFormEnv | None):
            Form env, if the document has forms and :meth:`.init_forms` was called.
    """
    
    def __init__(self, input, password=None, autoclose=False):
        
        if isinstance(input, str):
            input = Path(input)
        if isinstance(input, Path):
            input = input.expanduser().resolve()
            if not input.is_file():
                raise FileNotFoundError(input)
        
        self._input = input
        self._password = password
        self._autoclose = autoclose
        self._data_holder = []
        self._data_closer = []
        self.formenv = None
        
        if isinstance(self._input, pdfium_c.FPDF_DOCUMENT):
            self.raw = self._input
        else:
            self.raw, to_hold, to_close = _open_pdf(self._input, self._password, self._autoclose)
            self._data_holder += to_hold
            self._data_closer += to_close
        
        super().__init__(PdfDocument._close_impl, self._data_holder, self._data_closer)
    
    
    # Support using PdfDocument in a with-block
    # Note that pdfium objects should be closed in hierarchial order, but this is managed by our parents/kids system, so callers don't need to mind that.
    
    def __enter__(self):
        return self
    
    def __exit__(self, *_):
        self.close()
    
    
    def __repr__(self):
        if isinstance(self._input, Path):
            input_r = repr( str(self._input) )
        elif isinstance(self._input, bytes):
            input_r = f"<bytes object at {hex(id(self._input))}>"
        elif isinstance(self._input, pdfium_c.FPDF_DOCUMENT):
            input_r = f"<FPDF_DOCUMENT at {hex(id(self._input))}>"
        else:
            input_r = repr(self._input)
        return f"{super().__repr__()[:-1]} from {input_r}>"
    
    
    @property
    def parent(self):  # AutoCloseable hook
        return None
    
    
    @staticmethod
    def _close_impl(raw, data_holder, data_closer):
        pdfium_c.FPDF_CloseDocument(raw)
        for data in data_holder:
            id(data)
        for data in data_closer:
            data.close()
        data_holder.clear()
        data_closer.clear()
    
    
    def __len__(self):
        return pdfium_c.FPDF_GetPageCount(self)
    
    def __iter__(self):
        for i in range( len(self) ):
            yield self[i]
    
    def __getitem__(self, i):
        return self.get_page(i)
    
    def __delitem__(self, i):
        self.del_page(i)
    
    
    @classmethod
    def new(cls):
        """
        Returns:
            PdfDocument: A new, empty document.
        """
        new_pdf = pdfium_c.FPDF_CreateNewDocument()
        return cls(new_pdf)
    
    
    def init_forms(self, config=None):
        """
        Initialize a form env, if the document has forms. If already initialized, nothing will be done.
        See the :attr:`formenv` attribute.
    
        Attention:
            If form rendering is desired, this method shall be called right after document construction, before getting document length or page handles.
        
        Parameters:
            config (FPDF_FORMFILLINFO | None):
                Custom form config interface to use (optional).
        """
        
        formtype = self.get_formtype()
        if formtype == pdfium_c.FORMTYPE_NONE or self.formenv:
            return
        
        if not config:
            if "XFA" in PDFIUM_INFO.flags:  # pragma: no cover
                js_platform = pdfium_c.IPDF_JSPLATFORM(version=3)
                config = pdfium_c.FPDF_FORMFILLINFO(version=2, xfa_disabled=False, m_pJsPlatform=ctypes.pointer(js_platform))
            else:
                config = pdfium_c.FPDF_FORMFILLINFO(version=2)
        
        raw = pdfium_c.FPDFDOC_InitFormFillEnvironment(self, config)
        if not raw:
            raise PdfiumError(f"Initializing form env failed for document {self}.")
        self.formenv = PdfFormEnv(raw, self, config)
        self._add_kid(self.formenv)
        
        if formtype in (pdfium_c.FORMTYPE_XFA_FULL, pdfium_c.FORMTYPE_XFA_FOREGROUND):
            if "XFA" in PDFIUM_INFO.flags:  # pragma: no cover
                ok = pdfium_c.FPDF_LoadXFA(self)
                if not ok:
                    # FIXME ability to propagate an optional exception with error code info?
                    err = pdfium_c.FPDF_GetLastError()
                    logger.warning(f"FPDF_LoadXFA() failed with {pdfium_i.XFAErrorToStr.get(err)}")
            else:
                logger.warning(
                    "init_forms() called on XFA pdf, but this pdfium binary was compiled without XFA support.\n"
                    "Run `PDFIUM_PLATFORM=auto-v8 pip install -v pypdfium2 --no-binary pypdfium2` to get a build with XFA support."
                )
    
    
    def get_formtype(self):
        """
        Returns:
            int: PDFium form type that applies to the document (:attr:`FORMTYPE_*`).
            :attr:`FORMTYPE_NONE` if the document has no forms.
        """
        return pdfium_c.FPDF_GetFormType(self)
    
    
    def get_pagemode(self):
        """
        Returns:
            int: Page displaying mode (:attr:`PAGEMODE_*`).
        """
        return pdfium_c.FPDFDoc_GetPageMode(self)
    
    
    def is_tagged(self):
        """
        Returns:
            bool: Whether the document is tagged (cf. PDF 1.7, 10.7 "Tagged PDF").
        """
        return bool( pdfium_c.FPDFCatalog_IsTagged(self) )
    
    
    def save(self, dest, version=None, flags=0):
        """
        Save the document at its current state.
        
        Parameters:
            dest (str | pathlib.Path | io.BytesIO):
                File path or byte stream the document shall be written to.
            version (int | None):
                The PDF version to use, given as an integer (14 for 1.4, 15 for 1.5, ...).
                If None (the default), PDFium will set a version automatically.
            flags (int):
                PDFium saving flags (defaults to 0).
        """
        
        if isinstance(dest, (str, Path)):
            buffer, need_close = open(dest, "wb"), True
        elif pdfium_i.is_stream(dest, "w"):
            buffer, need_close = dest, False
        else:
            raise ValueError(f"Cannot save to '{dest}'")
        
        try:
            saveargs = (self, pdfium_i.get_bufwriter(buffer), flags)
            ok = pdfium_c.FPDF_SaveAsCopy(*saveargs) if version is None else pdfium_c.FPDF_SaveWithVersion(*saveargs, version)
            if not ok:
                raise PdfiumError("Failed to save document.")
        finally:
            if need_close:
                buffer.close()
    
    
    def get_identifier(self, type=pdfium_c.FILEIDTYPE_PERMANENT):
        """
        Parameters:
            type (int):
                The identifier type to retrieve (:attr:`FILEIDTYPE_*`), either permanent or changing.
                If the file was updated incrementally, the permanent identifier stays the same,
                while the changing identifier is re-calculated.
        Returns:
            bytes: Unique file identifier from the PDF's trailer dictionary.
            See PDF 1.7, Section 14.4 "File Identifiers".
        """
        n_bytes = pdfium_c.FPDF_GetFileIdentifier(self, type, None, 0)
        buffer = ctypes.create_string_buffer(n_bytes)
        pdfium_c.FPDF_GetFileIdentifier(self, type, buffer, n_bytes)
        return buffer.raw[:n_bytes-2]
    
    
    def get_version(self):
        """
        Returns:
            int | None: The PDF version of the document (14 for 1.4, 15 for 1.5, ...),
            or None if the document is new or its version could not be determined.
        """
        version = ctypes.c_int()
        ok = pdfium_c.FPDF_GetFileVersion(self, version)
        if not ok:
            return None
        return version.value
    
    
    def get_metadata_value(self, key):
        """
        Returns:
            str: Value of the given key in the PDF's metadata dictionary.
            If the key is not contained, an empty string will be returned.
        """
        enc_key = (key + "\x00").encode("utf-8")
        n_bytes = pdfium_c.FPDF_GetMetaText(self, enc_key, None, 0)
        buffer = ctypes.create_string_buffer(n_bytes)
        pdfium_c.FPDF_GetMetaText(self, enc_key, buffer, n_bytes)
        return buffer.raw[:n_bytes-2].decode("utf-16-le")
    
    
    METADATA_KEYS = ("Title", "Author", "Subject", "Keywords", "Creator", "Producer", "CreationDate", "ModDate")
    
    
    def get_metadata_dict(self, skip_empty=False):
        """
        Get the document's metadata as dictionary.
        
        Parameters:
            skip_empty (bool):
                If True, skip items whose value is an empty string.
        Returns:
            dict: PDF metadata.
        """
        metadata = {k: self.get_metadata_value(k) for k in self.METADATA_KEYS}
        if skip_empty:
            metadata = {k: v for k, v in metadata.items() if v}
        return metadata
    
    
    def count_attachments(self):
        """
        Returns:
            int: The number of embedded files in the document.
        """
        return pdfium_c.FPDFDoc_GetAttachmentCount(self)
    
    
    def get_attachment(self, index):
        """
        Returns:
            PdfAttachment: The attachment at given index (zero-based).
        """
        raw_attachment = pdfium_c.FPDFDoc_GetAttachment(self, index)
        if not raw_attachment:
            raise PdfiumError(f"Failed to get attachment at index {index}.")
        return PdfAttachment(raw_attachment, self)
    
    
    def new_attachment(self, name):
        """
        Add a new attachment to the document. It may appear at an arbitrary index (as of PDFium 5418).
        
        Parameters:
            name (str):
                The name the attachment shall have. Usually a file name with extension.
        Returns:
            PdfAttachment: Handle to the new, empty attachment.
        """
        enc_name = (name + "\x00").encode("utf-16-le")
        enc_name_ptr = ctypes.cast(enc_name, pdfium_c.FPDF_WIDESTRING)
        raw_attachment = pdfium_c.FPDFDoc_AddAttachment(self, enc_name_ptr)
        if not raw_attachment:
            raise PdfiumError(f"Failed to create new attachment '{name}'.")
        return PdfAttachment(raw_attachment, self)
    
    
    def del_attachment(self, index):
        """
        Unlink the attachment at given index (zero-based).
        It will be hidden from the viewer, but is still present in the file (as of PDFium 5418).
        Following attachments shift one slot to the left in the array representation used by PDFium's API.
        
        Handles to the attachment in question received from :meth:`.get_attachment`
        must not be accessed anymore after this method has been called.
        """
        ok = pdfium_c.FPDFDoc_DeleteAttachment(self, index)
        if not ok:
            raise PdfiumError(f"Failed to delete attachment at index {index}.")
    
    
    def get_page(self, index):
        """
        Returns:
            PdfPage: The page at given index (zero-based).
        Note:
            This calls ``FORM_OnAfterLoadPage()`` if the document has an active form env.
            In that case, note that closing the formenv would implicitly close the page.
        """
        
        raw_page = pdfium_c.FPDF_LoadPage(self, index)
        if not raw_page:
            raise PdfiumError("Failed to load page.")
        page = PdfPage(raw_page, self, self.formenv)
        
        if self.formenv:
            pdfium_c.FORM_OnAfterLoadPage(page, self.formenv)
            self.formenv._add_kid(page)
        else:
            self._add_kid(page)
        
        return page
    
    
    def new_page(self, width, height, index=None):
        """
        Insert a new, empty page into the document.
        
        Parameters:
            width (float):
                Target page width (horizontal size).
            height (float):
                Target page height (vertical size).
            index (int | None):
                Suggested zero-based index at which the page shall be inserted.
                If None or larger that the document's current last index, the page will be appended to the end.
        Returns:
            PdfPage: The newly created page.
        """
        if index is None:
            index = len(self)
        raw_page = pdfium_c.FPDFPage_New(self, index, width, height)
        page = PdfPage(raw_page, self, None)
        # not doing formenv calls for new pages
        self._add_kid(page)
        return page
    
    
    def del_page(self, index):
        """
        Remove the page at given index (zero-based).
        It is recommended to close any open handles to the page before calling this method.
        """
        # FIXME not sure how pdfium would behave if the caller tries to access a handle to a deleted page...
        pdfium_c.FPDFPage_Delete(self, index)
    
    
    def import_pages(self, pdf, pages=None, index=None):
        """
        Import pages from a foreign document.
        
        Parameters:
            pdf (PdfDocument):
                The document from which to import pages.
            pages (list[int] | str | None):
                The pages to include. It may either be a list of zero-based page indices, or a string of one-based page numbers and ranges.
                If None, all pages will be included.
            index (int):
                Zero-based index at which to insert the given pages. If None, they are appended to the end of the document.
        """
        
        if index is None:
            index = len(self)
        
        if isinstance(pages, str):
            ok = pdfium_c.FPDF_ImportPages(self, pdf, pages.encode("ascii"), index)
        else:
            page_count = 0
            c_pages = None
            if pages:
                page_count = len(pages)
                c_pages = (ctypes.c_int * page_count)(*pages)
            ok = pdfium_c.FPDF_ImportPagesByIndex(self, pdf, c_pages, page_count, index)
        
        if not ok:
            raise PdfiumError("Failed to import pages.")
    
    
    def get_page_size(self, index):
        """
        Returns:
            (float, float): Width and height of the page at given index (zero-based), in PDF canvas units.
        """
        size = pdfium_c.FS_SIZEF()
        ok = pdfium_c.FPDF_GetPageSizeByIndexF(self, index, size)
        if not ok:
            raise PdfiumError("Failed to get page size by index.")
        return (size.width, size.height)
    
    
    def get_page_label(self, index):
        """
        Returns:
            str: Label of the page at given index (zero-based).
            (A page label is essentially an alias that may be displayed instead of the page number.)
        """
        n_bytes = pdfium_c.FPDF_GetPageLabel(self, index, None, 0)
        buffer = ctypes.create_string_buffer(n_bytes)
        pdfium_c.FPDF_GetPageLabel(self, index, buffer, n_bytes)
        return buffer.raw[:n_bytes-2].decode("utf-16-le")
    
    
    def page_as_xobject(self, index, dest_pdf):
        """
        Capture a page as XObject and attach it to a document's resources.
        
        Parameters:
            index (int):
                Zero-based index of the page.
            dest_pdf (PdfDocument):
                Target document to which the XObject shall be added.
        Returns:
            PdfXObject: The page as XObject.
        """
        raw_xobject = pdfium_c.FPDF_NewXObjectFromPage(dest_pdf, self, index)
        if not raw_xobject:
            raise PdfiumError(f"Failed to capture page at index {index} as FPDF_XOBJECT.")
        xobject = PdfXObject(raw=raw_xobject, pdf=dest_pdf)
        self._add_kid(xobject)
        return xobject
    
    
    def get_toc(
            self,
            max_depth = 15,
            parent = None,
            level = 0,
            seen = None,
        ):
        """
        Iterate through the bookmarks in the document's table of contents (TOC).
        
        Parameters:
            max_depth (int):
                Maximum recursion depth to consider.
        Yields:
            :class:`.PdfBookmark`
        """
        
        if seen is None:
            seen = set()
        
        bm_ptr = pdfium_c.FPDFBookmark_GetFirstChild(self, parent)
        
        # NOTE We need bool(ptr) here to handle null pointers (where accessing .contents would raise an exception). Don't use ptr != None, it's always true.
        while bm_ptr:
            
            address = ctypes.addressof(bm_ptr.contents)
            if address in seen:
                logger.warning("A circular bookmark reference was detected while traversing the table of contents.")
                break
            else:
                seen.add(address)
            
            yield PdfBookmark(bm_ptr, self, level)
            if level < max_depth-1:
                yield from self.get_toc(max_depth=max_depth, parent=bm_ptr, level=level+1, seen=seen)
            elif pdfium_c.FPDFBookmark_GetFirstChild(self, bm_ptr):
                # Warn only if there actually is a subtree. If level == max_depth but the tree ends there, it's fine as no info is skipped.
                logger.warning(f"Maximum recursion depth {max_depth} reached (subtree skipped).")
            
            bm_ptr = pdfium_c.FPDFBookmark_GetNextSibling(self, bm_ptr)


def _open_pdf(input_data, password, autoclose):
    
    to_hold, to_close = (), ()
    if password is not None:
        password = (password+"\x00").encode("utf-8")
    
    if isinstance(input_data, Path):
        pdf = pdfium_c.FPDF_LoadDocument((str(input_data)+"\x00").encode("utf-8"), password)
    elif isinstance(input_data, (bytes, ctypes.Array)):
        pdf = pdfium_c.FPDF_LoadMemDocument64(input_data, len(input_data), password)
        to_hold = (input_data, )
    elif pdfium_i.is_stream(input_data, "r"):
        bufaccess, to_hold = pdfium_i.get_bufreader(input_data)
        if autoclose:
            to_close = (input_data, )
        pdf = pdfium_c.FPDF_LoadCustomDocument(bufaccess, password)
    else:
        raise TypeError(f"Invalid input type '{type(input_data).__name__}'")
    
    if pdfium_c.FPDF_GetPageCount(pdf) < 1:
        err_code = pdfium_c.FPDF_GetLastError()
        raise PdfiumError(f"Failed to load document (PDFium: {pdfium_i.ErrorToStr.get(err_code)}).", err_code=err_code)
    
    return pdf, to_hold, to_close


class PdfFormEnv (pdfium_i.AutoCloseable):
    """
    Form environment helper class.
    
    Attributes:
        raw (FPDF_FORMHANDLE):
            The underlying PDFium form env handle.
        config (FPDF_FORMFILLINFO):
            Accompanying form configuration interface, to be kept alive.
        pdf (PdfDocument):
            Parent document this form env belongs to.
    """
    
    def __init__(self, raw, pdf, config):
        self.raw = raw
        self.pdf = pdf
        self.config = config
        super().__init__(PdfFormEnv._close_impl, self.config, self.pdf)
    
    @property
    def parent(self):  # AutoCloseable hook
        return self.pdf
    
    @staticmethod
    def _close_impl(raw, config, pdf):
        pdfium_c.FPDFDOC_ExitFormFillEnvironment(raw)
        id(config)
        pdf.formenv = None


class PdfXObject (pdfium_i.AutoCloseable):
    """
    XObject helper class.
    
    Attributes:
        raw (FPDF_XOBJECT): The underlying PDFium XObject handle.
        pdf (PdfDocument): Reference to the document this XObject belongs to.
    """
    
    def __init__(self, raw, pdf):
        self.raw = raw
        self.pdf = pdf
        super().__init__(pdfium_c.FPDF_CloseXObject)
    
    @property
    def parent(self):  # AutoCloseable hook
        return self.pdf
    
    def as_pageobject(self):
        """
        Returns:
            PdfObject: An independent pageobject representation of the XObject.
            If multiple pageobjects are created from an XObject, they share resources.
            Returned pageobjects remain valid after the XObject is closed.
        """
        raw_pageobj = pdfium_c.FPDF_NewFormObjectFromXObject(self)
        # not a child object (see above)
        return PdfObject(raw=raw_pageobj, pdf=self.pdf)


class PdfBookmark (pdfium_i.AutoCastable):
    """
    Bookmark helper class.
    
    Attributes:
        raw (FPDF_BOOKMARK):
            The underlying PDFium bookmark handle.
        pdf (PdfDocument):
            Reference to the document this bookmark belongs to.
        level (int):
            The bookmark's nesting level in the TOC tree (zero-based). Corresponds to the number of parent bookmarks.
    """
    
    def __init__(self, raw, pdf, level):
        self.raw = raw
        self.pdf = pdf
        self.level = level
    
    def get_title(self):
        """
        Returns:
            str: The bookmark's title string.
        """
        n_bytes = pdfium_c.FPDFBookmark_GetTitle(self, None, 0)
        buffer = ctypes.create_string_buffer(n_bytes)
        pdfium_c.FPDFBookmark_GetTitle(self, buffer, n_bytes)
        return buffer.raw[:n_bytes-2].decode("utf-16-le")
    
    def get_count(self):
        """
        Returns:
            int: Signed number of child bookmarks that would be visible if the bookmark were open (i.e. recursively counting children of open children).
            The bookmark's initial state is open (expanded) if the number is positive, closed (collapsed) if negative.
            Zero if the bookmark has no descendants.
        """
        return pdfium_c.FPDFBookmark_GetCount(self)
    
    def get_dest(self):
        """
        Returns:
            PdfDest | None: The bookmark's destination (an object providing page index and viewport), or None on failure.
        """
        raw_dest = pdfium_c.FPDFBookmark_GetDest(self.pdf, self)
        if not raw_dest:
            return None
        return PdfDest(raw_dest, pdf=self.pdf)


class PdfDest (pdfium_i.AutoCastable):
    """
    Destination helper class.
    
    Attributes:
        raw (FPDF_DEST): The underlying PDFium destination handle.
        pdf (PdfDocument): Reference to the document this dest belongs to.
    """
    
    def __init__(self, raw, pdf):
        self.raw = raw
        self.pdf = pdf
    
    def get_index(self):
        """
        Returns:
            int | None: Zero-based index of the page the dest points to, or None on failure.
        """
        val = pdfium_c.FPDFDest_GetDestPageIndex(self.pdf, self)
        return val if val >= 0 else None
    
    def get_view(self):
        """
        Returns:
            (int, list[float]): A tuple of (view_mode, view_pos).
            *view_mode* is a constant (one of :data:`PDFDEST_VIEW_*`) defining how *view_pos* shall be interpreted.
            *view_pos* is the target position on the page the dest points to.
            It may contain between 0 to 4 float coordinates, depending on the view mode.
        """
        n_params = ctypes.c_ulong()
        pos = (pdfium_c.FS_FLOAT * 4)()
        mode = pdfium_c.FPDFDest_GetView(self, n_params, pos)
        pos = list(pos)[:n_params.value]
        return mode, pos
