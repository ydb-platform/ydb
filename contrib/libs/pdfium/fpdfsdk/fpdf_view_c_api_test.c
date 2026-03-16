// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This "C" (not "C++") file ensures that the public headers compile
// and link for "C" (and not just "C++").

#include <stdio.h>

#include "fpdfsdk/fpdf_view_c_api_test.h"

#include "public/fpdf_annot.h"
#include "public/fpdf_attachment.h"
#include "public/fpdf_catalog.h"
#include "public/fpdf_dataavail.h"
#include "public/fpdf_doc.h"
#include "public/fpdf_edit.h"
#include "public/fpdf_ext.h"
#include "public/fpdf_flatten.h"
#include "public/fpdf_formfill.h"
#include "public/fpdf_fwlevent.h"
#include "public/fpdf_javascript.h"
#include "public/fpdf_ppo.h"
#include "public/fpdf_progressive.h"
#include "public/fpdf_save.h"
#include "public/fpdf_searchex.h"
#include "public/fpdf_signature.h"
#include "public/fpdf_structtree.h"
#include "public/fpdf_sysfontinfo.h"
#include "public/fpdf_text.h"
#include "public/fpdf_thumbnail.h"
#include "public/fpdf_transformpage.h"
#include "public/fpdfview.h"

// Scheme for avoiding LTO out of existence, warnings, etc.
typedef void (*fnptr)(void);  // Legal generic function type for casts.
fnptr g_c_api_test_fnptr = NULL;  // Extern, so can't know it doesn't change.
#define CHK(x) if ((fnptr)(x) == g_c_api_test_fnptr) return 0

// Function to call from gtest harness to ensure linker resolution.
int CheckPDFiumCApi() {
    // fpdf_annot.h
    CHK(FPDFAnnot_AddFileAttachment);
    CHK(FPDFAnnot_AddInkStroke);
    CHK(FPDFAnnot_AppendAttachmentPoints);
    CHK(FPDFAnnot_AppendObject);
    CHK(FPDFAnnot_CountAttachmentPoints);
    CHK(FPDFAnnot_GetAP);
    CHK(FPDFAnnot_GetAttachmentPoints);
    CHK(FPDFAnnot_GetBorder);
    CHK(FPDFAnnot_GetColor);
    CHK(FPDFAnnot_GetFileAttachment);
    CHK(FPDFAnnot_GetFlags);
    CHK(FPDFAnnot_GetFocusableSubtypes);
    CHK(FPDFAnnot_GetFocusableSubtypesCount);
    CHK(FPDFAnnot_GetFontColor);
    CHK(FPDFAnnot_GetFontSize);
    CHK(FPDFAnnot_GetFormAdditionalActionJavaScript);
    CHK(FPDFAnnot_GetFormControlCount);
    CHK(FPDFAnnot_GetFormControlIndex);
    CHK(FPDFAnnot_GetFormFieldAlternateName);
    CHK(FPDFAnnot_GetFormFieldAtPoint);
    CHK(FPDFAnnot_GetFormFieldExportValue);
    CHK(FPDFAnnot_GetFormFieldFlags);
    CHK(FPDFAnnot_GetFormFieldName);
    CHK(FPDFAnnot_GetFormFieldType);
    CHK(FPDFAnnot_GetFormFieldValue);
    CHK(FPDFAnnot_GetInkListCount);
    CHK(FPDFAnnot_GetInkListPath);
    CHK(FPDFAnnot_GetLine);
    CHK(FPDFAnnot_GetLink);
    CHK(FPDFAnnot_GetLinkedAnnot);
    CHK(FPDFAnnot_GetNumberValue);
    CHK(FPDFAnnot_GetObject);
    CHK(FPDFAnnot_GetObjectCount);
    CHK(FPDFAnnot_GetOptionCount);
    CHK(FPDFAnnot_GetOptionLabel);
    CHK(FPDFAnnot_GetRect);
    CHK(FPDFAnnot_GetStringValue);
    CHK(FPDFAnnot_GetSubtype);
    CHK(FPDFAnnot_GetValueType);
    CHK(FPDFAnnot_GetVertices);
    CHK(FPDFAnnot_HasAttachmentPoints);
    CHK(FPDFAnnot_HasKey);
    CHK(FPDFAnnot_IsChecked);
    CHK(FPDFAnnot_IsObjectSupportedSubtype);
    CHK(FPDFAnnot_IsOptionSelected);
    CHK(FPDFAnnot_IsSupportedSubtype);
    CHK(FPDFAnnot_RemoveInkList);
    CHK(FPDFAnnot_RemoveObject);
    CHK(FPDFAnnot_SetAP);
    CHK(FPDFAnnot_SetAttachmentPoints);
    CHK(FPDFAnnot_SetBorder);
    CHK(FPDFAnnot_SetColor);
    CHK(FPDFAnnot_SetFlags);
    CHK(FPDFAnnot_SetFocusableSubtypes);
    CHK(FPDFAnnot_SetRect);
    CHK(FPDFAnnot_SetStringValue);
    CHK(FPDFAnnot_SetURI);
    CHK(FPDFAnnot_UpdateObject);
    CHK(FPDFPage_CloseAnnot);
    CHK(FPDFPage_CreateAnnot);
    CHK(FPDFPage_GetAnnot);
    CHK(FPDFPage_GetAnnotCount);
    CHK(FPDFPage_GetAnnotIndex);
    CHK(FPDFPage_RemoveAnnot);

    // fpdf_attachment.h
    CHK(FPDFAttachment_GetFile);
    CHK(FPDFAttachment_GetName);
    CHK(FPDFAttachment_GetStringValue);
    CHK(FPDFAttachment_GetValueType);
    CHK(FPDFAttachment_HasKey);
    CHK(FPDFAttachment_SetFile);
    CHK(FPDFAttachment_SetStringValue);
    CHK(FPDFDoc_AddAttachment);
    CHK(FPDFDoc_DeleteAttachment);
    CHK(FPDFDoc_GetAttachment);
    CHK(FPDFDoc_GetAttachmentCount);

    // fpdf_catalog.h
    CHK(FPDFCatalog_IsTagged);
    CHK(FPDFCatalog_SetLanguage);

    // fpdf_dataavail.h
    CHK(FPDFAvail_Create);
    CHK(FPDFAvail_Destroy);
    CHK(FPDFAvail_GetDocument);
    CHK(FPDFAvail_GetFirstPageNum);
    CHK(FPDFAvail_IsDocAvail);
    CHK(FPDFAvail_IsFormAvail);
    CHK(FPDFAvail_IsLinearized);
    CHK(FPDFAvail_IsPageAvail);

    // fpdf_doc.h
    CHK(FPDFAction_GetDest);
    CHK(FPDFAction_GetFilePath);
    CHK(FPDFAction_GetType);
    CHK(FPDFAction_GetURIPath);
    CHK(FPDFBookmark_Find);
    CHK(FPDFBookmark_GetAction);
    CHK(FPDFBookmark_GetCount);
    CHK(FPDFBookmark_GetDest);
    CHK(FPDFBookmark_GetFirstChild);
    CHK(FPDFBookmark_GetNextSibling);
    CHK(FPDFBookmark_GetTitle);
    CHK(FPDFDest_GetDestPageIndex);
    CHK(FPDFDest_GetLocationInPage);
    CHK(FPDFDest_GetView);
    CHK(FPDFLink_CountQuadPoints);
    CHK(FPDFLink_Enumerate);
    CHK(FPDFLink_GetAction);
    CHK(FPDFLink_GetAnnot);
    CHK(FPDFLink_GetAnnotRect);
    CHK(FPDFLink_GetDest);
    CHK(FPDFLink_GetLinkAtPoint);
    CHK(FPDFLink_GetLinkZOrderAtPoint);
    CHK(FPDFLink_GetQuadPoints);
    CHK(FPDF_GetFileIdentifier);
    CHK(FPDF_GetMetaText);
    CHK(FPDF_GetPageAAction);
    CHK(FPDF_GetPageLabel);

    // fpdf_edit.h
    CHK(FPDFFont_Close);
    CHK(FPDFFont_GetAscent);
    CHK(FPDFFont_GetBaseFontName);
    CHK(FPDFFont_GetDescent);
    CHK(FPDFFont_GetFamilyName);
    CHK(FPDFFont_GetFlags);
    CHK(FPDFFont_GetFontData);
    CHK(FPDFFont_GetGlyphPath);
    CHK(FPDFFont_GetGlyphWidth);
    CHK(FPDFFont_GetIsEmbedded);
    CHK(FPDFFont_GetItalicAngle);
    CHK(FPDFFont_GetWeight);
    CHK(FPDFFormObj_CountObjects);
    CHK(FPDFFormObj_GetObject);
    CHK(FPDFGlyphPath_CountGlyphSegments);
    CHK(FPDFGlyphPath_GetGlyphPathSegment);
    CHK(FPDFImageObj_GetBitmap);
    CHK(FPDFImageObj_GetImageDataDecoded);
    CHK(FPDFImageObj_GetImageDataRaw);
    CHK(FPDFImageObj_GetImageFilter);
    CHK(FPDFImageObj_GetImageFilterCount);
    CHK(FPDFImageObj_GetImageMetadata);
    CHK(FPDFImageObj_GetImagePixelSize);
    CHK(FPDFImageObj_GetRenderedBitmap);
    CHK(FPDFImageObj_LoadJpegFile);
    CHK(FPDFImageObj_LoadJpegFileInline);
    CHK(FPDFImageObj_SetBitmap);
    CHK(FPDFImageObj_SetMatrix);
    CHK(FPDFPageObjMark_CountParams);
    CHK(FPDFPageObjMark_GetName);
    CHK(FPDFPageObjMark_GetParamBlobValue);
    CHK(FPDFPageObjMark_GetParamIntValue);
    CHK(FPDFPageObjMark_GetParamKey);
    CHK(FPDFPageObjMark_GetParamStringValue);
    CHK(FPDFPageObjMark_GetParamValueType);
    CHK(FPDFPageObjMark_RemoveParam);
    CHK(FPDFPageObjMark_SetBlobParam);
    CHK(FPDFPageObjMark_SetIntParam);
    CHK(FPDFPageObjMark_SetStringParam);
    CHK(FPDFPageObj_AddMark);
    CHK(FPDFPageObj_CountMarks);
    CHK(FPDFPageObj_CreateNewPath);
    CHK(FPDFPageObj_CreateNewRect);
    CHK(FPDFPageObj_CreateTextObj);
    CHK(FPDFPageObj_Destroy);
    CHK(FPDFPageObj_GetBounds);
    CHK(FPDFPageObj_GetDashArray);
    CHK(FPDFPageObj_GetDashCount);
    CHK(FPDFPageObj_GetDashPhase);
    CHK(FPDFPageObj_GetFillColor);
    CHK(FPDFPageObj_GetLineCap);
    CHK(FPDFPageObj_GetLineJoin);
    CHK(FPDFPageObj_GetMark);
    CHK(FPDFPageObj_GetMarkedContentID);
    CHK(FPDFPageObj_GetMatrix);
    CHK(FPDFPageObj_GetRotatedBounds);
    CHK(FPDFPageObj_GetStrokeColor);
    CHK(FPDFPageObj_GetStrokeWidth);
    CHK(FPDFPageObj_GetType);
    CHK(FPDFPageObj_HasTransparency);
    CHK(FPDFPageObj_NewImageObj);
    CHK(FPDFPageObj_NewTextObj);
    CHK(FPDFPageObj_RemoveMark);
    CHK(FPDFPageObj_SetBlendMode);
    CHK(FPDFPageObj_SetDashArray);
    CHK(FPDFPageObj_SetDashPhase);
    CHK(FPDFPageObj_SetFillColor);
    CHK(FPDFPageObj_SetLineCap);
    CHK(FPDFPageObj_SetLineJoin);
    CHK(FPDFPageObj_SetMatrix);
    CHK(FPDFPageObj_SetStrokeColor);
    CHK(FPDFPageObj_SetStrokeWidth);
    CHK(FPDFPageObj_Transform);
    CHK(FPDFPageObj_TransformF);
    CHK(FPDFPage_CountObjects);
    CHK(FPDFPage_Delete);
    CHK(FPDFPage_GenerateContent);
    CHK(FPDFPage_GetObject);
    CHK(FPDFPage_GetRotation);
    CHK(FPDFPage_HasTransparency);
    CHK(FPDFPage_InsertObject);
    CHK(FPDFPage_New);
    CHK(FPDFPage_RemoveObject);
    CHK(FPDFPage_SetRotation);
    CHK(FPDFPage_TransformAnnots);
    CHK(FPDFPathSegment_GetClose);
    CHK(FPDFPathSegment_GetPoint);
    CHK(FPDFPathSegment_GetType);
    CHK(FPDFPath_BezierTo);
    CHK(FPDFPath_Close);
    CHK(FPDFPath_CountSegments);
    CHK(FPDFPath_GetDrawMode);
    CHK(FPDFPath_GetPathSegment);
    CHK(FPDFPath_LineTo);
    CHK(FPDFPath_MoveTo);
    CHK(FPDFPath_SetDrawMode);
    CHK(FPDFTextObj_GetFont);
    CHK(FPDFTextObj_GetFontSize);
    CHK(FPDFTextObj_GetRenderedBitmap);
    CHK(FPDFTextObj_GetText);
    CHK(FPDFTextObj_GetTextRenderMode);
    CHK(FPDFTextObj_SetTextRenderMode);
    CHK(FPDFText_LoadCidType2Font);
    CHK(FPDFText_LoadFont);
    CHK(FPDFText_LoadStandardFont);
    CHK(FPDFText_SetCharcodes);
    CHK(FPDFText_SetText);
    CHK(FPDF_CreateNewDocument);
    CHK(FPDF_MovePages);

    // fpdf_ext.h
    CHK(FPDFDoc_GetPageMode);
    CHK(FSDK_SetLocaltimeFunction);
    CHK(FSDK_SetTimeFunction);
    CHK(FSDK_SetUnSpObjProcessHandler);

    // fpdf_flatten.h
    CHK(FPDFPage_Flatten);

    // fpdf_fwlevent.h - no exports.

    // fpdf_formfill.h
    CHK(FORM_CanRedo);
    CHK(FORM_CanUndo);
    CHK(FORM_DoDocumentAAction);
    CHK(FORM_DoDocumentJSAction);
    CHK(FORM_DoDocumentOpenAction);
    CHK(FORM_DoPageAAction);
    CHK(FORM_ForceToKillFocus);
    CHK(FORM_GetFocusedAnnot);
    CHK(FORM_GetFocusedText);
    CHK(FORM_GetSelectedText);
    CHK(FORM_IsIndexSelected);
    CHK(FORM_OnAfterLoadPage);
    CHK(FORM_OnBeforeClosePage);
    CHK(FORM_OnChar);
    CHK(FORM_OnFocus);
    CHK(FORM_OnKeyDown);
    CHK(FORM_OnKeyUp);
    CHK(FORM_OnLButtonDoubleClick);
    CHK(FORM_OnLButtonDown);
    CHK(FORM_OnLButtonUp);
    CHK(FORM_OnMouseMove);
    CHK(FORM_OnMouseWheel);
    CHK(FORM_OnRButtonDown);
    CHK(FORM_OnRButtonUp);
    CHK(FORM_Redo);
    CHK(FORM_ReplaceAndKeepSelection);
    CHK(FORM_ReplaceSelection);
    CHK(FORM_SelectAllText);
    CHK(FORM_SetFocusedAnnot);
    CHK(FORM_SetIndexSelected);
    CHK(FORM_Undo);
    CHK(FPDFDOC_ExitFormFillEnvironment);
    CHK(FPDFDOC_InitFormFillEnvironment);
    CHK(FPDFPage_FormFieldZOrderAtPoint);
    CHK(FPDFPage_HasFormFieldAtPoint);
    CHK(FPDF_FFLDraw);
#if defined(PDF_USE_SKIA)
    CHK(FPDF_FFLDrawSkia);
#endif
    CHK(FPDF_GetFormType);
    CHK(FPDF_LoadXFA);
    CHK(FPDF_RemoveFormFieldHighlight);
    CHK(FPDF_SetFormFieldHighlightAlpha);
    CHK(FPDF_SetFormFieldHighlightColor);

    // fpdf_javascript.h
    CHK(FPDFDoc_CloseJavaScriptAction);
    CHK(FPDFDoc_GetJavaScriptAction);
    CHK(FPDFDoc_GetJavaScriptActionCount);
    CHK(FPDFJavaScriptAction_GetName);
    CHK(FPDFJavaScriptAction_GetScript);

    // fpdf_ppo.h
    CHK(FPDF_CloseXObject);
    CHK(FPDF_CopyViewerPreferences);
    CHK(FPDF_ImportNPagesToOne);
    CHK(FPDF_ImportPages);
    CHK(FPDF_ImportPagesByIndex);
    CHK(FPDF_NewFormObjectFromXObject);
    CHK(FPDF_NewXObjectFromPage);

    // fpdf_progressive.h
    CHK(FPDF_RenderPageBitmapWithColorScheme_Start);
    CHK(FPDF_RenderPageBitmap_Start);
    CHK(FPDF_RenderPage_Close);
    CHK(FPDF_RenderPage_Continue);

    // fpdf_save.h
    CHK(FPDF_SaveAsCopy);
    CHK(FPDF_SaveWithVersion);

    // fpdf_searchex.h
    CHK(FPDFText_GetCharIndexFromTextIndex);
    CHK(FPDFText_GetTextIndexFromCharIndex);

    // fpdf_signature.h
    CHK(FPDFSignatureObj_GetByteRange);
    CHK(FPDFSignatureObj_GetContents);
    CHK(FPDFSignatureObj_GetDocMDPPermission);
    CHK(FPDFSignatureObj_GetReason);
    CHK(FPDFSignatureObj_GetSubFilter);
    CHK(FPDFSignatureObj_GetTime);
    CHK(FPDF_GetSignatureCount);
    CHK(FPDF_GetSignatureObject);

    // fpdf_structtree.h
    CHK(FPDF_StructElement_Attr_CountChildren);
    CHK(FPDF_StructElement_Attr_GetBlobValue);
    CHK(FPDF_StructElement_Attr_GetBooleanValue);
    CHK(FPDF_StructElement_Attr_GetChildAtIndex);
    CHK(FPDF_StructElement_Attr_GetCount);
    CHK(FPDF_StructElement_Attr_GetName);
    CHK(FPDF_StructElement_Attr_GetNumberValue);
    CHK(FPDF_StructElement_Attr_GetStringValue);
    CHK(FPDF_StructElement_Attr_GetType);
    CHK(FPDF_StructElement_Attr_GetValue);
    CHK(FPDF_StructElement_CountChildren);
    CHK(FPDF_StructElement_GetActualText);
    CHK(FPDF_StructElement_GetAltText);
    CHK(FPDF_StructElement_GetAttributeAtIndex);
    CHK(FPDF_StructElement_GetAttributeCount);
    CHK(FPDF_StructElement_GetChildAtIndex);
    CHK(FPDF_StructElement_GetChildMarkedContentID);
    CHK(FPDF_StructElement_GetID);
    CHK(FPDF_StructElement_GetLang);
    CHK(FPDF_StructElement_GetMarkedContentID);
    CHK(FPDF_StructElement_GetMarkedContentIdAtIndex);
    CHK(FPDF_StructElement_GetMarkedContentIdCount);
    CHK(FPDF_StructElement_GetObjType);
    CHK(FPDF_StructElement_GetParent);
    CHK(FPDF_StructElement_GetStringAttribute);
    CHK(FPDF_StructElement_GetTitle);
    CHK(FPDF_StructElement_GetType);
    CHK(FPDF_StructTree_Close);
    CHK(FPDF_StructTree_CountChildren);
    CHK(FPDF_StructTree_GetChildAtIndex);
    CHK(FPDF_StructTree_GetForPage);

    // fpdf_sysfontinfo.h
    CHK(FPDF_AddInstalledFont);
    CHK(FPDF_FreeDefaultSystemFontInfo);
    CHK(FPDF_GetDefaultSystemFontInfo);
    CHK(FPDF_GetDefaultTTFMap);
    CHK(FPDF_GetDefaultTTFMapCount);
    CHK(FPDF_GetDefaultTTFMapEntry);
    CHK(FPDF_SetSystemFontInfo);

    // fpdf_text.h
    CHK(FPDFLink_CloseWebLinks);
    CHK(FPDFLink_CountRects);
    CHK(FPDFLink_CountWebLinks);
    CHK(FPDFLink_GetRect);
    CHK(FPDFLink_GetTextRange);
    CHK(FPDFLink_GetURL);
    CHK(FPDFLink_LoadWebLinks);
    CHK(FPDFText_ClosePage);
    CHK(FPDFText_CountChars);
    CHK(FPDFText_CountRects);
    CHK(FPDFText_FindClose);
    CHK(FPDFText_FindNext);
    CHK(FPDFText_FindPrev);
    CHK(FPDFText_FindStart);
    CHK(FPDFText_GetBoundedText);
    CHK(FPDFText_GetCharAngle);
    CHK(FPDFText_GetCharBox);
    CHK(FPDFText_GetCharIndexAtPos);
    CHK(FPDFText_GetCharOrigin);
    CHK(FPDFText_GetFillColor);
    CHK(FPDFText_GetFontInfo);
    CHK(FPDFText_GetFontSize);
    CHK(FPDFText_GetFontWeight);
    CHK(FPDFText_GetLooseCharBox);
    CHK(FPDFText_GetMatrix);
    CHK(FPDFText_GetRect);
    CHK(FPDFText_GetSchCount);
    CHK(FPDFText_GetSchResultIndex);
    CHK(FPDFText_GetStrokeColor);
    CHK(FPDFText_GetText);
    CHK(FPDFText_GetTextObject);
    CHK(FPDFText_GetUnicode);
    CHK(FPDFText_HasUnicodeMapError);
    CHK(FPDFText_IsGenerated);
    CHK(FPDFText_IsHyphen);
    CHK(FPDFText_LoadPage);

    // fpdf_thumbnail.h
    CHK(FPDFPage_GetDecodedThumbnailData);
    CHK(FPDFPage_GetRawThumbnailData);
    CHK(FPDFPage_GetThumbnailAsBitmap);

    // fpdf_transformpage.h
    CHK(FPDFClipPath_CountPathSegments);
    CHK(FPDFClipPath_CountPaths);
    CHK(FPDFClipPath_GetPathSegment);
    CHK(FPDFPageObj_GetClipPath);
    CHK(FPDFPageObj_TransformClipPath);
    CHK(FPDFPage_GetArtBox);
    CHK(FPDFPage_GetBleedBox);
    CHK(FPDFPage_GetCropBox);
    CHK(FPDFPage_GetMediaBox);
    CHK(FPDFPage_GetTrimBox);
    CHK(FPDFPage_InsertClipPath);
    CHK(FPDFPage_SetArtBox);
    CHK(FPDFPage_SetBleedBox);
    CHK(FPDFPage_SetCropBox);
    CHK(FPDFPage_SetMediaBox);
    CHK(FPDFPage_SetTrimBox);
    CHK(FPDFPage_TransFormWithClip);
    CHK(FPDF_CreateClipPath);
    CHK(FPDF_DestroyClipPath);

    // fpdfview.h
    CHK(FPDFBitmap_Create);
    CHK(FPDFBitmap_CreateEx);
    CHK(FPDFBitmap_Destroy);
    CHK(FPDFBitmap_FillRect);
    CHK(FPDFBitmap_GetBuffer);
    CHK(FPDFBitmap_GetFormat);
    CHK(FPDFBitmap_GetHeight);
    CHK(FPDFBitmap_GetStride);
    CHK(FPDFBitmap_GetWidth);
#ifdef PDF_ENABLE_XFA
    CHK(FPDF_BStr_Clear);
    CHK(FPDF_BStr_Init);
    CHK(FPDF_BStr_Set);
#endif
    CHK(FPDF_CloseDocument);
    CHK(FPDF_ClosePage);
    CHK(FPDF_CountNamedDests);
    CHK(FPDF_DestroyLibrary);
    CHK(FPDF_DeviceToPage);
    CHK(FPDF_DocumentHasValidCrossReferenceTable);
#ifdef PDF_ENABLE_V8
    CHK(FPDF_GetArrayBufferAllocatorSharedInstance);
#endif
    CHK(FPDF_GetDocPermissions);
    CHK(FPDF_GetDocUserPermissions);
    CHK(FPDF_GetFileVersion);
    CHK(FPDF_GetLastError);
    CHK(FPDF_GetNamedDest);
    CHK(FPDF_GetNamedDestByName);
    CHK(FPDF_GetPageBoundingBox);
    CHK(FPDF_GetPageCount);
    CHK(FPDF_GetPageHeight);
    CHK(FPDF_GetPageHeightF);
    CHK(FPDF_GetPageSizeByIndex);
    CHK(FPDF_GetPageSizeByIndexF);
    CHK(FPDF_GetPageWidth);
    CHK(FPDF_GetPageWidthF);
#ifdef PDF_ENABLE_V8
    CHK(FPDF_GetRecommendedV8Flags);
#endif
    CHK(FPDF_GetSecurityHandlerRevision);
    CHK(FPDF_GetTrailerEnds);
    CHK(FPDF_GetXFAPacketContent);
    CHK(FPDF_GetXFAPacketCount);
    CHK(FPDF_GetXFAPacketName);
    CHK(FPDF_InitLibrary);
    CHK(FPDF_InitLibraryWithConfig);
    CHK(FPDF_LoadCustomDocument);
    CHK(FPDF_LoadDocument);
    CHK(FPDF_LoadMemDocument);
    CHK(FPDF_LoadMemDocument64);
    CHK(FPDF_LoadPage);
    CHK(FPDF_PageToDevice);
#ifdef _WIN32
    CHK(FPDF_RenderPage);
#endif
    CHK(FPDF_RenderPageBitmap);
    CHK(FPDF_RenderPageBitmapWithMatrix);
#if defined(PDF_USE_SKIA)
    CHK(FPDF_RenderPageSkia);
#endif
#if defined(_WIN32)
    CHK(FPDF_SetPrintMode);
#endif
    CHK(FPDF_SetSandBoxPolicy);
    CHK(FPDF_VIEWERREF_GetDuplex);
    CHK(FPDF_VIEWERREF_GetName);
    CHK(FPDF_VIEWERREF_GetNumCopies);
    CHK(FPDF_VIEWERREF_GetPrintPageRange);
    CHK(FPDF_VIEWERREF_GetPrintPageRangeCount);
    CHK(FPDF_VIEWERREF_GetPrintPageRangeElement);
    CHK(FPDF_VIEWERREF_GetPrintScaling);

    return 1;
}

#undef CHK
