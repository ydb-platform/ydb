// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/parser/cpdf_object_avail.h"

#include <utility>

#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_indirect_object_holder.h"
#include "core/fpdfapi/parser/cpdf_object_walker.h"
#include "core/fpdfapi/parser/cpdf_read_validator.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"

CPDF_ObjectAvail::CPDF_ObjectAvail(RetainPtr<CPDF_ReadValidator> validator,
                                   CPDF_IndirectObjectHolder* holder,
                                   RetainPtr<const CPDF_Object> root)
    : validator_(std::move(validator)),
      holder_(holder),
      root_(std::move(root)) {
  DCHECK(validator_);
  DCHECK(holder);
  DCHECK(root_);
  if (!root_->IsInline())
    parsed_objnums_.insert(root_->GetObjNum());
}

CPDF_ObjectAvail::CPDF_ObjectAvail(RetainPtr<CPDF_ReadValidator> validator,
                                   CPDF_IndirectObjectHolder* holder,
                                   uint32_t obj_num)
    : validator_(std::move(validator)),
      holder_(holder),
      root_(pdfium::MakeRetain<CPDF_Reference>(holder, obj_num)) {
  DCHECK(validator_);
  DCHECK(holder);
}

CPDF_ObjectAvail::~CPDF_ObjectAvail() = default;

CPDF_DataAvail::DocAvailStatus CPDF_ObjectAvail::CheckAvail() {
  if (!LoadRootObject())
    return CPDF_DataAvail::kDataNotAvailable;

  if (CheckObjects()) {
    CleanMemory();
    return CPDF_DataAvail::kDataAvailable;
  }
  return CPDF_DataAvail::kDataNotAvailable;
}

bool CPDF_ObjectAvail::LoadRootObject() {
  if (!non_parsed_objects_.empty())
    return true;

  while (root_ && root_->IsReference()) {
    const uint32_t ref_obj_num = root_->AsReference()->GetRefObjNum();
    if (HasObjectParsed(ref_obj_num)) {
      root_ = nullptr;
      return true;
    }

    CPDF_ReadValidator::ScopedSession parse_session(validator_);
    RetainPtr<CPDF_Object> direct =
        holder_->GetOrParseIndirectObject(ref_obj_num);
    if (validator_->has_read_problems())
      return false;

    parsed_objnums_.insert(ref_obj_num);
    root_ = std::move(direct);
  }
  std::stack<uint32_t> non_parsed_objects_in_root;
  if (AppendObjectSubRefs(root_, &non_parsed_objects_in_root)) {
    non_parsed_objects_ = std::move(non_parsed_objects_in_root);
    return true;
  }
  return false;
}

bool CPDF_ObjectAvail::CheckObjects() {
  std::set<uint32_t> checked_objects;
  std::stack<uint32_t> objects_to_check = std::move(non_parsed_objects_);
  non_parsed_objects_ = std::stack<uint32_t>();
  while (!objects_to_check.empty()) {
    const uint32_t obj_num = objects_to_check.top();
    objects_to_check.pop();

    if (HasObjectParsed(obj_num))
      continue;

    if (!checked_objects.insert(obj_num).second)
      continue;

    CPDF_ReadValidator::ScopedSession parse_session(validator_);
    RetainPtr<const CPDF_Object> direct =
        holder_->GetOrParseIndirectObject(obj_num);
    if (direct == root_)
      continue;

    if (validator_->has_read_problems() ||
        !AppendObjectSubRefs(std::move(direct), &objects_to_check)) {
      non_parsed_objects_.push(obj_num);
      continue;
    }
    parsed_objnums_.insert(obj_num);
  }
  return non_parsed_objects_.empty();
}

bool CPDF_ObjectAvail::AppendObjectSubRefs(RetainPtr<const CPDF_Object> object,
                                           std::stack<uint32_t>* refs) const {
  DCHECK(refs);
  if (!object)
    return true;

  CPDF_ObjectWalker walker(std::move(object));
  while (RetainPtr<const CPDF_Object> obj = walker.GetNext()) {
    CPDF_ReadValidator::ScopedSession parse_session(validator_);

    // Skip if this object if it's an inlined root, the parent object or
    // explicitily excluded.
    const bool skip = (walker.GetParent() && obj == root_) ||
                      walker.dictionary_key() == "Parent" ||
                      (obj != root_ && ExcludeObject(obj.Get()));

    // We need to parse the object before we can do the exclusion check.
    // This is because the exclusion check may check against a referenced
    // field of the object which we need to make sure is loaded.
    if (validator_->has_read_problems())
      return false;

    if (skip) {
      walker.SkipWalkIntoCurrentObject();
      continue;
    }

    if (obj->IsReference())
      refs->push(obj->AsReference()->GetRefObjNum());
  }
  return true;
}

void CPDF_ObjectAvail::CleanMemory() {
  root_.Reset();
  parsed_objnums_.clear();
}

bool CPDF_ObjectAvail::ExcludeObject(const CPDF_Object* object) const {
  return false;
}

bool CPDF_ObjectAvail::HasObjectParsed(uint32_t obj_num) const {
  return pdfium::Contains(parsed_objnums_, obj_num);
}
