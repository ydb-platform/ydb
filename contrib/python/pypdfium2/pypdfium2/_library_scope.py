# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import sys
import atexit
import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i


def init_lib():
    assert not pdfium_i.LIBRARY_AVAILABLE
    if pdfium_i.DEBUG_AUTOCLOSE:  # pragma: no cover
        # FIXME not shown on auto-init, because DEBUG_AUTOCLOSE can only be set on the caller side after pypdfium2 has been imported...
        print("Initialize PDFium", file=sys.stderr)
    
    # PDFium init API may change in the future: https://crbug.com/pdfium/1446
    # NOTE Technically, FPDF_InitLibrary() would be sufficient for our purposes, but pdfium docs say "This will be deprecated in the future", so don't use it to be on the safe side. Also, avoid experimental config versions that might not be promoted to stable.
    config = pdfium_c.FPDF_LIBRARY_CONFIG(
        version = 2,
        m_pUserFontPaths = None,
        m_pIsolate = None,
        m_v8EmbedderSlot = 0,
        # m_pPlatform = None,  # v3
        # m_RendererType = pdfium_c.FPDF_RENDERERTYPE_AGG,  # v4
    )
    pdfium_c.FPDF_InitLibraryWithConfig(config)
    pdfium_i.LIBRARY_AVAILABLE.value = True


def destroy_lib():  # pragma: no cover
    assert pdfium_i.LIBRARY_AVAILABLE
    if pdfium_i.DEBUG_AUTOCLOSE:
        pdfium_i._safe_debug("Destroy PDFium")
    pdfium_c.FPDF_DestroyLibrary()
    pdfium_i.LIBRARY_AVAILABLE.value = False


# Load pdfium
init_lib()

# Register an exit handler that will free pdfium
# Trust in Python to call exit handlers only after all objects have been finalized
atexit.register(destroy_lib)
