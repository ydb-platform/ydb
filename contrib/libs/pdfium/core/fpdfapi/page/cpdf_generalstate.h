// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_GENERALSTATE_H_
#define CORE_FPDFAPI_PAGE_CPDF_GENERALSTATE_H_

#include <vector>

#include "constants/transparency.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/shared_copy_on_write.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class CPDF_Dictionary;
class CPDF_Object;
class CPDF_TransferFunc;

class CPDF_GeneralState {
 public:
  CPDF_GeneralState();
  CPDF_GeneralState(const CPDF_GeneralState& that);
  ~CPDF_GeneralState();

  void Emplace() { m_Ref.Emplace(); }
  bool HasRef() const { return !!m_Ref; }

  void SetRenderIntent(const ByteString& ri);

  ByteString GetBlendMode() const;
  BlendMode GetBlendType() const;
  void SetBlendType(BlendMode type);

  float GetFillAlpha() const;
  void SetFillAlpha(float alpha);

  float GetStrokeAlpha() const;
  void SetStrokeAlpha(float alpha);

  RetainPtr<const CPDF_Dictionary> GetSoftMask() const;
  RetainPtr<CPDF_Dictionary> GetMutableSoftMask();
  void SetSoftMask(RetainPtr<CPDF_Dictionary> pDict);

  RetainPtr<const CPDF_Object> GetTR() const;
  void SetTR(RetainPtr<const CPDF_Object> pObject);

  RetainPtr<CPDF_TransferFunc> GetTransferFunc() const;
  void SetTransferFunc(RetainPtr<CPDF_TransferFunc> pFunc);

  void SetBlendMode(const ByteString& mode);

  const CFX_Matrix* GetSMaskMatrix() const;
  void SetSMaskMatrix(const CFX_Matrix& matrix);

  bool GetFillOP() const;
  void SetFillOP(bool op);

  bool GetStrokeOP() const;
  void SetStrokeOP(bool op);

  int GetOPMode() const;
  void SetOPMode(int mode);

  void SetBG(RetainPtr<const CPDF_Object> pObject);
  void SetUCR(RetainPtr<const CPDF_Object> pObject);
  void SetHT(RetainPtr<const CPDF_Object> pObject);

  void SetFlatness(float flatness);
  void SetSmoothness(float smoothness);

  bool GetStrokeAdjust() const;
  void SetStrokeAdjust(bool adjust);

  void SetAlphaSource(bool source);
  void SetTextKnockout(bool knockout);

  void SetGraphicsResourceNames(std::vector<ByteString> names);
  void AppendGraphicsResourceName(ByteString name);
  pdfium::span<const ByteString> GetGraphicsResourceNames() const;

 private:
  class StateData final : public Retainable {
   public:
    CONSTRUCT_VIA_MAKE_RETAIN;

    RetainPtr<StateData> Clone() const;

    ByteString m_BlendMode = pdfium::transparency::kNormal;
    BlendMode m_BlendType = BlendMode::kNormal;
    RetainPtr<CPDF_Dictionary> m_pSoftMask;
    CFX_Matrix m_SMaskMatrix;
    float m_StrokeAlpha = 1.0f;
    float m_FillAlpha = 1.0f;
    RetainPtr<const CPDF_Object> m_pTR;
    RetainPtr<CPDF_TransferFunc> m_pTransferFunc;
    int m_RenderIntent = 0;
    bool m_StrokeAdjust = false;
    bool m_AlphaSource = false;
    bool m_TextKnockout = false;
    bool m_StrokeOP = false;
    bool m_FillOP = false;
    int m_OPMode = 0;
    RetainPtr<const CPDF_Object> m_pBG;
    RetainPtr<const CPDF_Object> m_pUCR;
    RetainPtr<const CPDF_Object> m_pHT;
    float m_Flatness = 1.0f;
    float m_Smoothness = 0.0f;
    // The resource names of the graphics states that apply to this object.
    std::vector<ByteString> m_GraphicsResourceNames;

   private:
    StateData();
    StateData(const StateData& that);
    ~StateData() override;
  };

  SharedCopyOnWrite<StateData> m_Ref;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_GENERALSTATE_H_
