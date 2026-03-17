// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_IPWL_FILLERNOTIFY_H_
#define FPDFSDK_PWL_IPWL_FILLERNOTIFY_H_

#include <memory>
#include <utility>

#include "core/fxcrt/mask.h"
#include "core/fxcrt/widestring.h"
#include "public/fpdf_fwlevent.h"

class CFX_FloatRect;

class IPWL_FillerNotify {
 public:
  // These must match the values in public/fpdf_formfill.h
  enum CursorStyle {
    kArrow = 0,
    kNESW = 1,
    kNWSE = 2,
    kVBeam = 3,
    kHBeam = 4,
    kHand = 5,
  };

  class PerWindowData {
   public:
    virtual ~PerWindowData() = default;
    virtual std::unique_ptr<PerWindowData> Clone() const = 0;
  };

  struct BeforeKeystrokeResult {
    bool rc;
    bool exit;
  };

  virtual ~IPWL_FillerNotify() = default;

  virtual void InvalidateRect(PerWindowData* pWidgetData,
                              const CFX_FloatRect& rect) = 0;
  virtual void OutputSelectedRect(PerWindowData* pWidgetData,
                                  const CFX_FloatRect& rect) = 0;
  virtual bool IsSelectionImplemented() const = 0;
  virtual void SetCursor(CursorStyle nCursorStyle) = 0;

  // Must write to |bBottom| and |fPopupRet|.
  virtual void QueryWherePopup(const PerWindowData* pAttached,
                               float fPopupMin,
                               float fPopupMax,
                               bool* bBottom,
                               float* fPopupRet) = 0;

  virtual BeforeKeystrokeResult OnBeforeKeyStroke(
      const PerWindowData* pAttached,
      WideString& strChange,
      const WideString& strChangeEx,
      int nSelStart,
      int nSelEnd,
      bool bKeyDown,
      Mask<FWL_EVENTFLAG> nFlag) = 0;

  virtual bool OnPopupPreOpen(const PerWindowData* pAttached,
                              Mask<FWL_EVENTFLAG> nFlag) = 0;

  virtual bool OnPopupPostOpen(const PerWindowData* pAttached,
                               Mask<FWL_EVENTFLAG> nFlag) = 0;
};

#endif  // FPDFSDK_PWL_IPWL_FILLERNOTIFY_H_
