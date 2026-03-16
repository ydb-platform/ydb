// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_HELPERS_H_
#define FPDFSDK_CPDFSDK_HELPERS_H_

#include <vector>

#include "build/build_config.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_parser.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/dib/fx_dib.h"
#include "public/fpdf_doc.h"
#include "public/fpdf_ext.h"
#include "public/fpdfview.h"

#ifdef PDF_ENABLE_XFA
#include "core/fxcrt/fx_stream.h"
#endif  // PDF_ENABLE_XFA

class CFX_DIBitmap;
class CPDF_Annot;
class CPDF_AnnotContext;
class CPDF_ClipPath;
class CPDF_ContentMarkItem;
class CPDF_Object;
class CPDF_Font;
class CPDF_LinkExtract;
class CPDF_PageObject;
class CPDF_RenderOptions;
class CPDF_Stream;
class CPDF_StructElement;
class CPDF_StructTree;
class CPDF_TextPage;
class CPDF_TextPageFind;
class CPDFSDK_FormFillEnvironment;
class CPDFSDK_InteractiveForm;
struct CPDF_JavaScript;
struct XObjectContext;

#if defined(PDF_USE_SKIA)
class SkCanvas;
#endif

// Conversions to/from underlying types.
IPDF_Page* IPDFPageFromFPDFPage(FPDF_PAGE page);
FPDF_PAGE FPDFPageFromIPDFPage(IPDF_Page* page);
CPDF_Page* CPDFPageFromFPDFPage(FPDF_PAGE page);
FPDF_DOCUMENT FPDFDocumentFromCPDFDocument(CPDF_Document* doc);
CPDF_Document* CPDFDocumentFromFPDFDocument(FPDF_DOCUMENT doc);

// Conversions to/from incomplete FPDF_ API types.
inline FPDF_ACTION FPDFActionFromCPDFDictionary(const CPDF_Dictionary* action) {
  return reinterpret_cast<FPDF_ACTION>(const_cast<CPDF_Dictionary*>(action));
}
inline CPDF_Dictionary* CPDFDictionaryFromFPDFAction(FPDF_ACTION action) {
  return reinterpret_cast<CPDF_Dictionary*>(action);
}

inline FPDF_ANNOTATION FPDFAnnotationFromCPDFAnnotContext(
    CPDF_AnnotContext* annot) {
  return reinterpret_cast<FPDF_ANNOTATION>(annot);
}
inline CPDF_AnnotContext* CPDFAnnotContextFromFPDFAnnotation(
    FPDF_ANNOTATION annot) {
  return reinterpret_cast<CPDF_AnnotContext*>(annot);
}

inline FPDF_ATTACHMENT FPDFAttachmentFromCPDFObject(CPDF_Object* attachment) {
  return reinterpret_cast<FPDF_ATTACHMENT>(attachment);
}
inline CPDF_Object* CPDFObjectFromFPDFAttachment(FPDF_ATTACHMENT attachment) {
  return reinterpret_cast<CPDF_Object*>(attachment);
}

inline FPDF_BITMAP FPDFBitmapFromCFXDIBitmap(CFX_DIBitmap* bitmap) {
  return reinterpret_cast<FPDF_BITMAP>(bitmap);
}
inline CFX_DIBitmap* CFXDIBitmapFromFPDFBitmap(FPDF_BITMAP bitmap) {
  return reinterpret_cast<CFX_DIBitmap*>(bitmap);
}

#if defined(PDF_USE_SKIA)
inline FPDF_SKIA_CANVAS FPDFSkiaCanvasFromSkCanvas(SkCanvas* canvas) {
  return reinterpret_cast<FPDF_SKIA_CANVAS>(canvas);
}
inline SkCanvas* SkCanvasFromFPDFSkiaCanvas(FPDF_SKIA_CANVAS canvas) {
  return reinterpret_cast<SkCanvas*>(canvas);
}
#endif

inline FPDF_BOOKMARK FPDFBookmarkFromCPDFDictionary(
    const CPDF_Dictionary* bookmark) {
  return reinterpret_cast<FPDF_BOOKMARK>(
      const_cast<CPDF_Dictionary*>(bookmark));
}
inline CPDF_Dictionary* CPDFDictionaryFromFPDFBookmark(FPDF_BOOKMARK bookmark) {
  return reinterpret_cast<CPDF_Dictionary*>(bookmark);
}

inline FPDF_CLIPPATH FPDFClipPathFromCPDFClipPath(CPDF_ClipPath* path) {
  return reinterpret_cast<FPDF_CLIPPATH>(path);
}
inline CPDF_ClipPath* CPDFClipPathFromFPDFClipPath(FPDF_CLIPPATH path) {
  return reinterpret_cast<CPDF_ClipPath*>(path);
}

inline FPDF_DEST FPDFDestFromCPDFArray(const CPDF_Array* dest) {
  return reinterpret_cast<FPDF_DEST>(const_cast<CPDF_Array*>(dest));
}
inline CPDF_Array* CPDFArrayFromFPDFDest(FPDF_DEST dest) {
  return reinterpret_cast<CPDF_Array*>(dest);
}

inline FPDF_FONT FPDFFontFromCPDFFont(CPDF_Font* font) {
  return reinterpret_cast<FPDF_FONT>(font);
}
inline CPDF_Font* CPDFFontFromFPDFFont(FPDF_FONT font) {
  return reinterpret_cast<CPDF_Font*>(font);
}

inline FPDF_JAVASCRIPT_ACTION FPDFJavaScriptActionFromCPDFJavaScriptAction(
    CPDF_JavaScript* javascript) {
  return reinterpret_cast<FPDF_JAVASCRIPT_ACTION>(javascript);
}
inline CPDF_JavaScript* CPDFJavaScriptActionFromFPDFJavaScriptAction(
    FPDF_JAVASCRIPT_ACTION javascript) {
  return reinterpret_cast<CPDF_JavaScript*>(javascript);
}

inline FPDF_LINK FPDFLinkFromCPDFDictionary(CPDF_Dictionary* link) {
  return reinterpret_cast<FPDF_LINK>(link);
}
inline CPDF_Dictionary* CPDFDictionaryFromFPDFLink(FPDF_LINK link) {
  return reinterpret_cast<CPDF_Dictionary*>(link);
}

inline FPDF_PAGELINK FPDFPageLinkFromCPDFLinkExtract(CPDF_LinkExtract* link) {
  return reinterpret_cast<FPDF_PAGELINK>(link);
}
inline CPDF_LinkExtract* CPDFLinkExtractFromFPDFPageLink(FPDF_PAGELINK link) {
  return reinterpret_cast<CPDF_LinkExtract*>(link);
}

inline FPDF_PAGEOBJECT FPDFPageObjectFromCPDFPageObject(
    CPDF_PageObject* page_object) {
  return reinterpret_cast<FPDF_PAGEOBJECT>(page_object);
}
inline CPDF_PageObject* CPDFPageObjectFromFPDFPageObject(
    FPDF_PAGEOBJECT page_object) {
  return reinterpret_cast<CPDF_PageObject*>(page_object);
}

inline FPDF_PAGEOBJECTMARK FPDFPageObjectMarkFromCPDFContentMarkItem(
    CPDF_ContentMarkItem* mark) {
  return reinterpret_cast<FPDF_PAGEOBJECTMARK>(mark);
}
inline CPDF_ContentMarkItem* CPDFContentMarkItemFromFPDFPageObjectMark(
    FPDF_PAGEOBJECTMARK mark) {
  return reinterpret_cast<CPDF_ContentMarkItem*>(mark);
}

inline FPDF_PAGERANGE FPDFPageRangeFromCPDFArray(const CPDF_Array* range) {
  return reinterpret_cast<FPDF_PAGERANGE>(range);
}
inline const CPDF_Array* CPDFArrayFromFPDFPageRange(FPDF_PAGERANGE range) {
  return reinterpret_cast<const CPDF_Array*>(range);
}

inline FPDF_PATHSEGMENT FPDFPathSegmentFromFXPathPoint(
    const CFX_Path::Point* segment) {
  return reinterpret_cast<FPDF_PATHSEGMENT>(segment);
}
inline const CFX_Path::Point* FXPathPointFromFPDFPathSegment(
    FPDF_PATHSEGMENT segment) {
  return reinterpret_cast<const CFX_Path::Point*>(segment);
}

inline FPDF_STRUCTTREE FPDFStructTreeFromCPDFStructTree(
    CPDF_StructTree* struct_tree) {
  return reinterpret_cast<FPDF_STRUCTTREE>(struct_tree);
}
inline CPDF_StructTree* CPDFStructTreeFromFPDFStructTree(
    FPDF_STRUCTTREE struct_tree) {
  return reinterpret_cast<CPDF_StructTree*>(struct_tree);
}

inline FPDF_STRUCTELEMENT FPDFStructElementFromCPDFStructElement(
    CPDF_StructElement* struct_element) {
  return reinterpret_cast<FPDF_STRUCTELEMENT>(struct_element);
}
inline CPDF_StructElement* CPDFStructElementFromFPDFStructElement(
    FPDF_STRUCTELEMENT struct_element) {
  return reinterpret_cast<CPDF_StructElement*>(struct_element);
}

inline FPDF_STRUCTELEMENT_ATTR FPDFStructElementAttrFromCPDFDictionary(
    const CPDF_Dictionary* dictionary) {
  return reinterpret_cast<FPDF_STRUCTELEMENT_ATTR>(dictionary);
}
inline const CPDF_Dictionary* CPDFDictionaryFromFPDFStructElementAttr(
    FPDF_STRUCTELEMENT_ATTR struct_element_attr) {
  return reinterpret_cast<const CPDF_Dictionary*>(struct_element_attr);
}

inline FPDF_STRUCTELEMENT_ATTR_VALUE FPDFStructElementAttrValueFromCPDFObject(
    const CPDF_Object* object) {
  return reinterpret_cast<FPDF_STRUCTELEMENT_ATTR_VALUE>(object);
}
inline const CPDF_Object* CPDFObjectFromFPDFStructElementAttrValue(
    FPDF_STRUCTELEMENT_ATTR_VALUE struct_element_attr_value) {
  return reinterpret_cast<const CPDF_Object*>(struct_element_attr_value);
}

inline FPDF_TEXTPAGE FPDFTextPageFromCPDFTextPage(CPDF_TextPage* page) {
  return reinterpret_cast<FPDF_TEXTPAGE>(page);
}
inline CPDF_TextPage* CPDFTextPageFromFPDFTextPage(FPDF_TEXTPAGE page) {
  return reinterpret_cast<CPDF_TextPage*>(page);
}

inline FPDF_SCHHANDLE FPDFSchHandleFromCPDFTextPageFind(
    CPDF_TextPageFind* handle) {
  return reinterpret_cast<FPDF_SCHHANDLE>(handle);
}
inline CPDF_TextPageFind* CPDFTextPageFindFromFPDFSchHandle(
    FPDF_SCHHANDLE handle) {
  return reinterpret_cast<CPDF_TextPageFind*>(handle);
}

inline FPDF_FORMHANDLE FPDFFormHandleFromCPDFSDKFormFillEnvironment(
    CPDFSDK_FormFillEnvironment* handle) {
  return reinterpret_cast<FPDF_FORMHANDLE>(handle);
}
inline CPDFSDK_FormFillEnvironment*
CPDFSDKFormFillEnvironmentFromFPDFFormHandle(FPDF_FORMHANDLE handle) {
  return reinterpret_cast<CPDFSDK_FormFillEnvironment*>(handle);
}

inline FPDF_SIGNATURE FPDFSignatureFromCPDFDictionary(
    const CPDF_Dictionary* dictionary) {
  return reinterpret_cast<FPDF_SIGNATURE>(dictionary);
}
inline const CPDF_Dictionary* CPDFDictionaryFromFPDFSignature(
    FPDF_SIGNATURE signature) {
  return reinterpret_cast<const CPDF_Dictionary*>(signature);
}

inline FPDF_XOBJECT FPDFXObjectFromXObjectContext(XObjectContext* xobject) {
  return reinterpret_cast<FPDF_XOBJECT>(xobject);
}

inline XObjectContext* XObjectContextFromFPDFXObject(FPDF_XOBJECT xobject) {
  return reinterpret_cast<XObjectContext*>(xobject);
}

FXDIB_Format FXDIBFormatFromFPDFFormat(int format);

CPDFSDK_InteractiveForm* FormHandleToInteractiveForm(FPDF_FORMHANDLE hHandle);

UNSAFE_BUFFER_USAGE ByteString
ByteStringFromFPDFWideString(FPDF_WIDESTRING wide_string);

UNSAFE_BUFFER_USAGE WideString
WideStringFromFPDFWideString(FPDF_WIDESTRING wide_string);

// Public APIs are not consistent w.r.t. the type used to represent buffer
// length, while internal code generally expects size_t. Use StrictNumeric here
// to make sure the length types fit in a size_t.
UNSAFE_BUFFER_USAGE pdfium::span<char> SpanFromFPDFApiArgs(
    void* buffer,
    pdfium::StrictNumeric<size_t> buflen);

#ifdef PDF_ENABLE_XFA
// Layering prevents fxcrt from knowing about FPDF_FILEHANDLER, so this can't
// be a static method of IFX_SeekableStream.
RetainPtr<IFX_SeekableStream> MakeSeekableStream(
    FPDF_FILEHANDLER* pFileHandler);
#endif  // PDF_ENABLE_XFA

RetainPtr<const CPDF_Array> GetQuadPointsArrayFromDictionary(
    const CPDF_Dictionary* dict);
RetainPtr<CPDF_Array> GetMutableQuadPointsArrayFromDictionary(
    CPDF_Dictionary* dict);
RetainPtr<CPDF_Array> AddQuadPointsArrayToDictionary(CPDF_Dictionary* dict);
bool IsValidQuadPointsIndex(const CPDF_Array* array, size_t index);
bool GetQuadPointsAtIndex(RetainPtr<const CPDF_Array> array,
                          size_t quad_index,
                          FS_QUADPOINTSF* quad_points);

CFX_PointF CFXPointFFromFSPointF(const FS_POINTF& point);

CFX_FloatRect CFXFloatRectFromFSRectF(const FS_RECTF& rect);
FS_RECTF FSRectFFromCFXFloatRect(const CFX_FloatRect& rect);

CFX_Matrix CFXMatrixFromFSMatrix(const FS_MATRIX& matrix);
FS_MATRIX FSMatrixFromCFXMatrix(const CFX_Matrix& matrix);

unsigned long NulTerminateMaybeCopyAndReturnLength(
    const ByteString& text,
    pdfium::span<char> result_span);

unsigned long Utf16EncodeMaybeCopyAndReturnLength(
    const WideString& text,
    pdfium::span<char> result_span);

// Returns the length of the raw stream data from |stream|. The raw data is the
// stream's data as stored in the PDF without applying any filters. If |buffer|
// is non-empty and its length is large enough to contain the raw data, then
// the raw data is copied into |buffer|.
unsigned long GetRawStreamMaybeCopyAndReturnLength(
    RetainPtr<const CPDF_Stream> stream,
    pdfium::span<uint8_t> buffer);

// Return the length of the decoded stream data of |stream|. The decoded data is
// the uncompressed stream data, i.e. the raw stream data after having all
// filters applied. If |buffer| is non-empty and its length is large enough to
// contain the decoded data, then the decoded data is copied into |buffer|.
unsigned long DecodeStreamMaybeCopyAndReturnLength(
    RetainPtr<const CPDF_Stream> stream,
    pdfium::span<uint8_t> buffer);

void SetPDFSandboxPolicy(FPDF_DWORD policy, FPDF_BOOL enable);
FPDF_BOOL IsPDFSandboxPolicyEnabled(FPDF_DWORD policy);

void SetPDFUnsupportInfo(UNSUPPORT_INFO* unsp_info);
void ReportUnsupportedFeatures(const CPDF_Document* pDoc);
void ReportUnsupportedXFA(const CPDF_Document* pDoc);
void CheckForUnsupportedAnnot(const CPDF_Annot* pAnnot);
void ProcessParseError(CPDF_Parser::Error err);
void SetColorFromScheme(const FPDF_COLORSCHEME* pColorScheme,
                        CPDF_RenderOptions* pRenderOptions);

// Returns a vector of page indices given a page range string.
std::vector<uint32_t> ParsePageRangeString(const ByteString& bsPageRange,
                                           uint32_t nCount);

#endif  // FPDFSDK_CPDFSDK_HELPERS_H_
