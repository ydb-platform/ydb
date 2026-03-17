/******************************************************************************
 *
 * Project:  GDAL
 * Purpose:  Includes PDFium headers
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2015, Even Rouault <even dot rouault at spatialys dot com>
 *
 * SPDX-License-Identifier: MIT
 *****************************************************************************/

#ifndef PDFSDK_HEADERS_PDFIUM_H
#define PDFSDK_HEADERS_PDFIUM_H

#if defined(__GNUC__) && !defined(_MSC_VER)
#pragma GCC system_header
#endif

#ifdef HAVE_PDFIUM
#include "cpl_multiproc.h"

#if (!defined(CPL_MULTIPROC_WIN32) && !defined(CPL_MULTIPROC_PTHREAD)) ||      \
    defined(CPL_MULTIPROC_STUB) || defined(CPL_MULTIPROC_NONE)
#error PDF driver compiled with PDFium library requires working threads with mutex locking!
#endif

// Linux ignores timeout, Windows returns if not INFINITE
#ifdef _WIN32
#define PDFIUM_MUTEX_TIMEOUT INFINITE
#else
#define PDFIUM_MUTEX_TIMEOUT 0.0f
#endif

#ifdef _MSC_VER
#pragma warning(push)
// include/pdfium\core/fxcrt/fx_memcpy_wrappers.h(48,30): warning C4244: 'argument': conversion from 'int' to 'wchar_t', possible loss of data
#pragma warning(disable : 4244)

// Nasty hack to avoid build issues with asm volatile in include/pdfium\core/fxcrt/immediate_crash.h(138,3)
#define CORE_FXCRT_IMMEDIATE_CRASH_H_
#include "cpl_error.h"

namespace pdfium
{

[[noreturn]] inline void ImmediateCrash()
{
    // Private joke: GDAL crashing !!!! Are you satisfied Martin ;-)
    CPLError(CE_Fatal, CPLE_AppDefined, "ImmediateCrash()");
}

}  // namespace pdfium

#endif

#include <cstring>
#error #include "public/fpdfview.h"
#error #include "core/fpdfapi/page/cpdf_page.h"
#error #include "core/fpdfapi/page/cpdf_occontext.h"
#error #include "core/fpdfapi/parser/cpdf_array.h"
#error #include "core/fpdfapi/parser/cpdf_dictionary.h"
#error #include "core/fpdfapi/parser/cpdf_document.h"
#error #include "core/fpdfapi/parser/cpdf_number.h"
#error #include "core/fpdfapi/parser/cpdf_object.h"
#error #include "core/fpdfapi/parser/cpdf_stream.h"
#error #include "core/fpdfapi/parser/cpdf_stream_acc.h"
#error #include "core/fpdfapi/render/cpdf_pagerendercontext.h"
#error #include "core/fpdfapi/render/cpdf_progressiverenderer.h"
#error #include "core/fpdfapi/render/cpdf_rendercontext.h"
#error #include "core/fpdfapi/render/cpdf_renderoptions.h"
#error #include "core/fpdfdoc/cpdf_annotlist.h"
#error #include "core/fxcrt/bytestring.h"
#error #include "core/fxge/cfx_defaultrenderdevice.h"
#error #include "core/fxge/dib/cfx_dibitmap.h"
#error #include "core/fxge/cfx_renderdevice.h"
#error #include "core/fxge/agg/cfx_agg_devicedriver.h"
#error #include "core/fxge/agg/cfx_agg_imagerenderer.h"
#error #include "core/fxge/renderdevicedriver_iface.h"
#error #include "fpdfsdk/cpdfsdk_helpers.h"
#error #include "fpdfsdk/cpdfsdk_pauseadapter.h"

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif  // HAVE_PDFIUM

#endif  // PDFSDK_HEADERS_PDFIUM_H
