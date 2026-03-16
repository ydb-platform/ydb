// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_CONTENTMARKITEM_H_
#define CORE_FPDFAPI_PAGE_CPDF_CONTENTMARKITEM_H_

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Dictionary;

class CPDF_ContentMarkItem final : public Retainable {
 public:
  enum ParamType { kNone, kPropertiesDict, kDirectDict };

  CONSTRUCT_VIA_MAKE_RETAIN;

  const ByteString& GetName() const { return m_MarkName; }
  ParamType GetParamType() const { return m_ParamType; }
  RetainPtr<const CPDF_Dictionary> GetParam() const;
  RetainPtr<CPDF_Dictionary> GetParam();
  const ByteString& GetPropertyName() const { return m_PropertyName; }

  void SetDirectDict(RetainPtr<CPDF_Dictionary> pDict);
  void SetPropertiesHolder(RetainPtr<CPDF_Dictionary> pHolder,
                           const ByteString& property_name);

 private:
  explicit CPDF_ContentMarkItem(ByteString name);
  ~CPDF_ContentMarkItem() override;

  ParamType m_ParamType = kNone;
  ByteString m_MarkName;
  ByteString m_PropertyName;
  RetainPtr<CPDF_Dictionary> m_pPropertiesHolder;
  RetainPtr<CPDF_Dictionary> m_pDirectDict;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_CONTENTMARKITEM_H_
