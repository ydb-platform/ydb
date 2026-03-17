# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("PdfAttachment", )

import ctypes
import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i
from pypdfium2._helpers.misc import PdfiumError


def _encode_key(key):
    if isinstance(key, str):
        return (key + "\x00").encode("utf-8")
    else:
        raise TypeError(f"Key must be str, but {type(key).__name__} was given.")


class PdfAttachment (pdfium_i.AutoCastable):
    """
    Attachment helper class.
    See PDF 1.7, Section 7.11 "File Specifications".
    
    Attributes:
        raw (FPDF_ATTACHMENT):
            The underlying PDFium attachment handle.
        pdf (PdfDocument):
            Reference to the document this attachment belongs to. Must remain valid as long as the attachment is used.
    """
    
    # TODO consider using AutoCloseable machienery to guarantee `pdf` remains alive as long as the attachment object exists
    
    # Problems with PDFium's attachment API:
    # - https://crbug.com/pdfium/1939
    # - https://crbug.com/pdfium/893
    
    
    def __init__(self, raw, pdf):
        self.raw = raw
        self.pdf = pdf
    
    
    def get_name(self):
        """
        Returns:
            str: Name of the attachment.
        """
        n_bytes = pdfium_c.FPDFAttachment_GetName(self, None, 0)
        buffer = ctypes.create_string_buffer(n_bytes)
        buffer_ptr = ctypes.cast(buffer, ctypes.POINTER(pdfium_c.FPDF_WCHAR))
        pdfium_c.FPDFAttachment_GetName(self, buffer_ptr, n_bytes)
        return buffer.raw[:n_bytes-2].decode("utf-16-le")
    
    
    def get_data(self):
        """
        Returns:
            ctypes.Array: The attachment's file data (as :class:`~ctypes.c_char` array).
        """
                
        n_bytes = ctypes.c_ulong()
        pdfium_c.FPDFAttachment_GetFile(self, None, 0, n_bytes)
        n_bytes = n_bytes.value
        if n_bytes == 0:
            raise PdfiumError(f"Failed to extract attachment (buffer length {n_bytes}).")
        
        buffer = ctypes.create_string_buffer(n_bytes)
        out_buflen = ctypes.c_ulong()
        ok = pdfium_c.FPDFAttachment_GetFile(self, buffer, n_bytes, out_buflen)
        out_buflen = out_buflen.value
        if not ok:
            raise PdfiumError("Failed to extract attachment (error status).")
        if n_bytes < out_buflen:
            raise PdfiumError(f"Failed to extract attachment (expected {n_bytes} bytes, but got {out_buflen}).")
        
        return buffer
    
    
    def set_data(self, data):
        """
        Set the attachment's file data.
        If this function is called on an existing attachment, it will be changed to point at the new data,
        but the previous data will not be removed from the file (as of PDFium 5418).
        
        Parameters:
            data (bytes | ctypes.Array):
                New file data for the attachment. May be any data type that can be implicitly converted to :class:`~ctypes.c_void_p`.
        """
        ok = pdfium_c.FPDFAttachment_SetFile(self, self.pdf, data, len(data))
        if not ok:
            raise PdfiumError("Failed to set attachment data.")
    
    
    def has_key(self, key):
        """
        Parameters:
            key (str):
                A key to look for in the attachment's params dictionary.
        Returns:
            bool: True if *key* is contained in the params dictionary, False otherwise.
        """
        return pdfium_c.FPDFAttachment_HasKey(self, _encode_key(key))
    
    
    def get_value_type(self, key):
        """
        Returns:
            int: Type of the value of *key* in the params dictionary (:attr:`FPDF_OBJECT_*`).
        """
        return pdfium_c.FPDFAttachment_GetValueType(self, _encode_key(key))
    
    
    def get_str_value(self, key):
        """
        Returns:
            str: The value of *key* in the params dictionary, if it is a string or name.
            Otherwise, an empty string will be returned. On other failures, an exception will be raised.
        """
        
        enc_key = _encode_key(key)
        n_bytes = pdfium_c.FPDFAttachment_GetStringValue(self, enc_key, None, 0)
        if n_bytes <= 0:
            raise PdfiumError(f"Failed to get value of key '{key}'.")
        
        buffer = ctypes.create_string_buffer(n_bytes)
        buffer_ptr = ctypes.cast(buffer, ctypes.POINTER(pdfium_c.FPDF_WCHAR))
        pdfium_c.FPDFAttachment_GetStringValue(self, enc_key, buffer_ptr, n_bytes)
        
        return buffer.raw[:n_bytes-2].decode("utf-16-le")
    
    
    def set_str_value(self, key, value):
        """
        Set the attribute specified by *key* to the string *value*.
        
        Parameters:
            value (str): New string value for the attribute.
        """
        enc_value = (value + "\x00").encode("utf-16-le")
        enc_value_ptr = ctypes.cast(enc_value, pdfium_c.FPDF_WIDESTRING)
        ok = pdfium_c.FPDFAttachment_SetStringValue(self, _encode_key(key), enc_value_ptr)
        if not ok:
            raise PdfiumError(f"Failed to set attachment param '{key}' to '{value}'.")
