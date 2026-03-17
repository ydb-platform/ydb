# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("PdfUnspHandler", )

import atexit
import logging
import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i

lib_logger = logging.getLogger("pypdfium2")


class PdfUnspHandler:
    """
    Unsupported feature handler helper class.
    
    Attributes:
        handlers (dict[str, typing.Callable]):
            A dictionary of named handler functions to be called with an unsupported code (:attr:`FPDF_UNSP_*`) when PDFium detects an unsupported feature.
    """
    
    def __init__(self):
        self.handlers = {}
        self._config = None
    
    
    def __call__(self, _, type):
        for handler in self.handlers.values():
            handler(type)
    
    
    def setup(self, add_default=True):
        """
        Attach the handler to PDFium, and register an exit function to keep the object alive for the rest of the session.
        
        Parameters:
            add_default (bool):
                If True, add a default callback that will log unsupported features as warning.
        """
        
        self._config = pdfium_c.UNSUPPORT_INFO(version=1)
        pdfium_i.set_callback(self._config, "FSDK_UnSupport_Handler", self)
        pdfium_c.FSDK_SetUnSpObjProcessHandler(self._config)
        
        atexit.register(self._keep)
        
        if add_default:
            self.handlers["default"] = PdfUnspHandler._default
    
    
    def _keep(self):
        id(self.handlers)
        id(self._config)
    
    
    @staticmethod
    def _default(type):
        lib_logger.warning(f"Unsupported PDF feature: {pdfium_i.UnsupportedInfoToStr.get(type)}")
