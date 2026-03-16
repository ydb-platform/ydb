// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_STRUCTELEMENT_H_
#define CORE_FPDFDOC_CPDF_STRUCTELEMENT_H_

#include <optional>
#include <vector>

#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Dictionary;
class CPDF_Object;
class CPDF_StructTree;

class CPDF_StructElement final : public Retainable {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  ByteString GetType() const { return m_Type; }
  ByteString GetObjType() const;
  WideString GetAltText() const;
  WideString GetActualText() const;
  WideString GetTitle() const;
  std::optional<WideString> GetID() const;
  std::optional<WideString> GetLang() const;
  RetainPtr<const CPDF_Object> GetA() const;
  RetainPtr<const CPDF_Object> GetK() const;

  size_t CountKids() const;
  CPDF_StructElement* GetKidIfElement(size_t index) const;
  int GetKidContentId(size_t index) const;
  bool UpdateKidIfElement(const CPDF_Dictionary* pDict,
                          CPDF_StructElement* pElement);

  CPDF_StructElement* GetParent() const { return m_pParentElement; }
  void SetParent(CPDF_StructElement* pParentElement) {
    m_pParentElement = pParentElement;
  }

 private:
  struct Kid {
    enum Type { kInvalid, kElement, kPageContent, kStreamContent, kObject };

    Kid();
    Kid(const Kid& that);
    ~Kid();

    Type m_Type = kInvalid;
    uint32_t m_PageObjNum = 0;  // For {PageContent, StreamContent, Object}.
    uint32_t m_RefObjNum = 0;   // For {StreamContent, Object} types.
    uint32_t m_ContentId = 0;   // For {PageContent, StreamContent} types.
    RetainPtr<CPDF_StructElement> m_pElement;  // For Element type.
    RetainPtr<const CPDF_Dictionary> m_pDict;  // For Element type.
  };

  CPDF_StructElement(const CPDF_StructTree* pTree,
                     RetainPtr<const CPDF_Dictionary> pDict);
  ~CPDF_StructElement() override;

  void LoadKids();
  void LoadKid(uint32_t page_obj_num,
               RetainPtr<const CPDF_Object> pKidObj,
               Kid& kid);

  UnownedPtr<const CPDF_StructTree> const m_pTree;
  RetainPtr<const CPDF_Dictionary> const m_pDict;
  UnownedPtr<CPDF_StructElement> m_pParentElement;
  const ByteString m_Type;
  std::vector<Kid> m_Kids;
};

#endif  // CORE_FPDFDOC_CPDF_STRUCTELEMENT_H_
