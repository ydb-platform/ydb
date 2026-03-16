# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("PdfiumError", )


class PdfiumError (RuntimeError):
    """
    An exception from the PDFium library, detected by function return code.
    
    Attributes:
        err_code (int | None): PDFium error code, for programmatic handling of error subtypes, if provided by the API in question (e.g. document loading). None otherwise.
    """
    
    def __init__(self, msg, err_code=None):
        super().__init__(msg)
        self.err_code = err_code
