// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_PARSER_CPDF_OBJECT_AVAIL_H_
#define CORE_FPDFAPI_PARSER_CPDF_OBJECT_AVAIL_H_

#include <set>
#include <stack>

#include "core/fpdfapi/parser/cpdf_data_avail.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_Object;
class CPDF_IndirectObjectHolder;
class CPDF_ReadValidator;

// Helper for check availability of object tree.
class CPDF_ObjectAvail {
 public:
  CPDF_ObjectAvail(RetainPtr<CPDF_ReadValidator> validator,
                   CPDF_IndirectObjectHolder* holder,
                   RetainPtr<const CPDF_Object> root);
  CPDF_ObjectAvail(RetainPtr<CPDF_ReadValidator> validator,
                   CPDF_IndirectObjectHolder* holder,
                   uint32_t obj_num);
  virtual ~CPDF_ObjectAvail();

  CPDF_DataAvail::DocAvailStatus CheckAvail();

 protected:
  virtual bool ExcludeObject(const CPDF_Object* object) const;

 private:
  bool LoadRootObject();
  bool CheckObjects();
  bool AppendObjectSubRefs(RetainPtr<const CPDF_Object> object,
                           std::stack<uint32_t>* refs) const;
  void CleanMemory();
  bool HasObjectParsed(uint32_t obj_num) const;

  RetainPtr<CPDF_ReadValidator> const validator_;
  UnownedPtr<CPDF_IndirectObjectHolder> const holder_;
  RetainPtr<const CPDF_Object> root_;
  std::set<uint32_t> parsed_objnums_;
  std::stack<uint32_t> non_parsed_objects_;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_OBJECT_AVAIL_H_
