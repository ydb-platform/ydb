// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_EDIT_CPDF_PAGEORGANIZER_H_
#define CORE_FPDFAPI_EDIT_CPDF_PAGEORGANIZER_H_

#include <stdint.h>

#include <map>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Dictionary;
class CPDF_Document;
class CPDF_Object;
class CPDF_Reference;

class CPDF_PageOrganizer {
 protected:
  CPDF_PageOrganizer(CPDF_Document* dest_doc, CPDF_Document* src_doc);
  ~CPDF_PageOrganizer();

  // Must be called after construction before doing anything else.
  bool Init();

  bool UpdateReference(RetainPtr<CPDF_Object> obj);

  CPDF_Document* dest() { return dest_doc_; }
  const CPDF_Document* dest() const { return dest_doc_; }

  CPDF_Document* src() { return src_doc_; }
  const CPDF_Document* src() const { return src_doc_; }

  void AddObjectMapping(uint32_t old_page_obj_num, uint32_t new_page_obj_num) {
    object_number_map_[old_page_obj_num] = new_page_obj_num;
  }

  void ClearObjectNumberMap() { object_number_map_.clear(); }

  static bool CopyInheritable(RetainPtr<CPDF_Dictionary> dest_page_dict,
                              RetainPtr<const CPDF_Dictionary> src_page_dict,
                              const ByteString& key);

  static RetainPtr<const CPDF_Object> PageDictGetInheritableTag(
      RetainPtr<const CPDF_Dictionary> dict,
      const ByteString& src_tag);

 private:
  bool InitDestDoc();

  uint32_t GetNewObjId(CPDF_Reference* ref);

  UnownedPtr<CPDF_Document> const dest_doc_;
  UnownedPtr<CPDF_Document> const src_doc_;

  // Mapping of source object number to destination object number.
  std::map<uint32_t, uint32_t> object_number_map_;
};

#endif  // CORE_FPDFAPI_EDIT_CPDF_PAGEORGANIZER_H_
