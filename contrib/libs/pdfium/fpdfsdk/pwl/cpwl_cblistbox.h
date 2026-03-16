// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_CBLISTBOX_H_
#define FPDFSDK_PWL_CPWL_CBLISTBOX_H_

#include <memory>

#include "fpdfsdk/pwl/cpwl_list_box.h"
#include "fpdfsdk/pwl/ipwl_fillernotify.h"
#include "public/fpdf_fwlevent.h"

class CPWL_CBListBox final : public CPWL_ListBox {
 public:
  CPWL_CBListBox(
      const CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_CBListBox() override;

  // CPWL_ListBox
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;

  bool IsMovementKey(FWL_VKEYCODE nKeyCode) const;
  bool OnMovementKeyDown(FWL_VKEYCODE nKeyCode, Mask<FWL_EVENTFLAG> nFlag);
  bool IsChar(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag) const;
  bool OnCharNotify(uint16_t nChar, Mask<FWL_EVENTFLAG> nFlag);
};

#endif  // FPDFSDK_PWL_CPWL_CBLISTBOX_H_
