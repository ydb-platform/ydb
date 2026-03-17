// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef PUBLIC_CPP_FPDF_DELETERS_H_
#define PUBLIC_CPP_FPDF_DELETERS_H_

#include "public/fpdf_annot.h"
#include "public/fpdf_dataavail.h"
#include "public/fpdf_edit.h"
#include "public/fpdf_formfill.h"
#include "public/fpdf_javascript.h"
#include "public/fpdf_structtree.h"
#include "public/fpdf_text.h"
#include "public/fpdf_transformpage.h"
#include "public/fpdfview.h"

// Custom deleters for using FPDF_* types with std::unique_ptr<>.

struct FPDFAnnotationDeleter {
  inline void operator()(FPDF_ANNOTATION annot) { FPDFPage_CloseAnnot(annot); }
};

struct FPDFAvailDeleter {
  inline void operator()(FPDF_AVAIL avail) { FPDFAvail_Destroy(avail); }
};

struct FPDFBitmapDeleter {
  inline void operator()(FPDF_BITMAP bitmap) { FPDFBitmap_Destroy(bitmap); }
};

struct FPDFClipPathDeleter {
  inline void operator()(FPDF_CLIPPATH clip_path) {
    FPDF_DestroyClipPath(clip_path);
  }
};

struct FPDFDocumentDeleter {
  inline void operator()(FPDF_DOCUMENT doc) { FPDF_CloseDocument(doc); }
};

struct FPDFFontDeleter {
  inline void operator()(FPDF_FONT font) { FPDFFont_Close(font); }
};

struct FPDFFormHandleDeleter {
  inline void operator()(FPDF_FORMHANDLE form) {
    FPDFDOC_ExitFormFillEnvironment(form);
  }
};

struct FPDFJavaScriptActionDeleter {
  inline void operator()(FPDF_JAVASCRIPT_ACTION javascript) {
    FPDFDoc_CloseJavaScriptAction(javascript);
  }
};

struct FPDFPageDeleter {
  inline void operator()(FPDF_PAGE page) { FPDF_ClosePage(page); }
};

struct FPDFPageLinkDeleter {
  inline void operator()(FPDF_PAGELINK pagelink) {
    FPDFLink_CloseWebLinks(pagelink);
  }
};

struct FPDFPageObjectDeleter {
  inline void operator()(FPDF_PAGEOBJECT object) {
    FPDFPageObj_Destroy(object);
  }
};

struct FPDFStructTreeDeleter {
  inline void operator()(FPDF_STRUCTTREE tree) { FPDF_StructTree_Close(tree); }
};

struct FPDFTextFindDeleter {
  inline void operator()(FPDF_SCHHANDLE handle) { FPDFText_FindClose(handle); }
};

struct FPDFTextPageDeleter {
  inline void operator()(FPDF_TEXTPAGE text) { FPDFText_ClosePage(text); }
};

#endif  // PUBLIC_CPP_FPDF_DELETERS_H_
