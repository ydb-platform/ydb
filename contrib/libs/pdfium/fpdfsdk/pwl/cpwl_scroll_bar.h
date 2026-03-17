// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_PWL_CPWL_SCROLL_BAR_H_
#define FPDFSDK_PWL_CPWL_SCROLL_BAR_H_

#include <memory>

#include "core/fxcrt/cfx_timer.h"
#include "core/fxcrt/unowned_ptr.h"
#include "fpdfsdk/pwl/cpwl_sbbutton.h"
#include "fpdfsdk/pwl/cpwl_wnd.h"

struct PWL_SCROLL_INFO {
 public:
  PWL_SCROLL_INFO()
      : fContentMin(0.0f),
        fContentMax(0.0f),
        fPlateWidth(0.0f),
        fBigStep(0.0f),
        fSmallStep(0.0f) {}

  bool operator==(const PWL_SCROLL_INFO& that) const {
    return fContentMin == that.fContentMin && fContentMax == that.fContentMax &&
           fPlateWidth == that.fPlateWidth && fBigStep == that.fBigStep &&
           fSmallStep == that.fSmallStep;
  }
  bool operator!=(const PWL_SCROLL_INFO& that) const {
    return !(*this == that);
  }

  float fContentMin;
  float fContentMax;
  float fPlateWidth;
  float fBigStep;
  float fSmallStep;
};

struct PWL_FLOATRANGE {
 public:
  PWL_FLOATRANGE() = default;

  bool operator==(const PWL_FLOATRANGE& that) const {
    return fMin == that.fMin && fMax == that.fMax;
  }
  bool operator!=(const PWL_FLOATRANGE& that) const { return !(*this == that); }

  void Reset();
  void Set(float min, float max);
  bool In(float x) const;
  float GetWidth() const;

  float fMin = 0.0f;
  float fMax = 0.0f;
};

struct PWL_SCROLL_PRIVATEDATA {
 public:
  PWL_SCROLL_PRIVATEDATA();

  bool operator==(const PWL_SCROLL_PRIVATEDATA& that) const {
    return ScrollRange == that.ScrollRange &&
           fClientWidth == that.fClientWidth && fScrollPos == that.fScrollPos &&
           fBigStep == that.fBigStep && fSmallStep == that.fSmallStep;
  }
  bool operator!=(const PWL_SCROLL_PRIVATEDATA& that) const {
    return !(*this == that);
  }

  void Default();
  void SetScrollRange(float min, float max);
  void SetClientWidth(float width);
  void SetSmallStep(float step);
  void SetBigStep(float step);
  bool SetPos(float pos);

  void AddSmall();
  void SubSmall();
  void AddBig();
  void SubBig();

  PWL_FLOATRANGE ScrollRange;
  float fClientWidth;
  float fScrollPos;
  float fBigStep;
  float fSmallStep;
};

class CPWL_ScrollBar final : public CPWL_Wnd, public CFX_Timer::CallbackIface {
 public:
  static constexpr float kWidth = 12.0f;
  static constexpr uint8_t kTransparency = 150;

  CPWL_ScrollBar(
      const CreateParams& cp,
      std::unique_ptr<IPWL_FillerNotify::PerWindowData> pAttachedData);
  ~CPWL_ScrollBar() override;

  // CPWL_Wnd:
  void OnDestroy() override;
  bool RepositionChildWnd() override;
  void DrawThisAppearance(CFX_RenderDevice* pDevice,
                          const CFX_Matrix& mtUser2Device) override;
  bool OnLButtonDown(Mask<FWL_EVENTFLAG> nFlag,
                     const CFX_PointF& point) override;
  bool OnLButtonUp(Mask<FWL_EVENTFLAG> nFlag, const CFX_PointF& point) override;
  void SetScrollInfo(const PWL_SCROLL_INFO& info) override;
  void SetScrollPosition(float pos) override;
  void NotifyLButtonDown(CPWL_Wnd* child, const CFX_PointF& pos) override;
  void NotifyLButtonUp(CPWL_Wnd* child, const CFX_PointF& pos) override;
  void NotifyMouseMove(CPWL_Wnd* child, const CFX_PointF& pos) override;
  void CreateChildWnd(const CreateParams& cp) override;

  // CFX_Timer::CallbackIface:
  void OnTimerFired() override;

  float GetScrollBarWidth() const;

 private:
  void SetScrollRange(float fMin, float fMax, float fClientWidth);
  void SetScrollPos(float fPos);

  // Returns |true| iff this instance is still allocated.
  [[nodiscard]] bool MovePosButton(bool bRefresh);
  void SetScrollStep(float fBigStep, float fSmallStep);
  void NotifyScrollWindow();
  CFX_FloatRect GetScrollArea() const;

  void CreateButtons(const CreateParams& cp);

  void OnMinButtonLBDown(const CFX_PointF& point);
  void OnMinButtonLBUp(const CFX_PointF& point);
  void OnMinButtonMouseMove(const CFX_PointF& point);

  void OnMaxButtonLBDown(const CFX_PointF& point);
  void OnMaxButtonLBUp(const CFX_PointF& point);
  void OnMaxButtonMouseMove(const CFX_PointF& point);

  void OnPosButtonLBDown(const CFX_PointF& point);
  void OnPosButtonLBUp(const CFX_PointF& point);
  void OnPosButtonMouseMove(const CFX_PointF& point);

  float TrueToFace(float);
  float FaceToTrue(float);

  PWL_SCROLL_INFO m_OriginInfo;
  UnownedPtr<CPWL_SBButton> m_pMinButton;
  UnownedPtr<CPWL_SBButton> m_pMaxButton;
  UnownedPtr<CPWL_SBButton> m_pPosButton;
  std::unique_ptr<CFX_Timer> m_pTimer;
  PWL_SCROLL_PRIVATEDATA m_sData;
  bool m_bMouseDown = false;
  bool m_bMinOrMax = false;
  float m_nOldPos = 0.0f;
  float m_fOldPosButton = 0.0f;
};

#endif  // FPDFSDK_PWL_CPWL_SCROLL_BAR_H_
