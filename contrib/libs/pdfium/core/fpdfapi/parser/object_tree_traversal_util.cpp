// Copyright 2023 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/parser/object_tree_traversal_util.h"

#include <stdint.h>

#include <map>
#include <queue>
#include <set>
#include <utility>
#include <vector>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/unowned_ptr.h"

namespace {

class ObjectTreeTraverser {
 public:
  explicit ObjectTreeTraverser(const CPDF_Document* document)
      : document_(document) {
    const CPDF_Parser* parser = document_->GetParser();
    const CPDF_Dictionary* trailer = parser ? parser->GetTrailer() : nullptr;
    const CPDF_Dictionary* root = trailer ? trailer : document_->GetRoot();
    const uint32_t root_object_number =
        trailer ? parser->GetTrailerObjectNumber() : root->GetObjNum();
    // If `root` is a trailer, then it may not have an object number, as many
    // trailers are inlined.
    if (root_object_number) {
      referenced_objects_[root_object_number] = 1;
      object_number_map_[root] = root_object_number;
    }

    object_queue_.push(pdfium::WrapRetain(root));
    seen_objects_.insert(root);
  }
  ~ObjectTreeTraverser() = default;

  void Traverse() { CalculateReferenceCounts(GetReferenceEntries()); }

  const std::map<uint32_t, int>& referenced_objects() {
    return referenced_objects_;
  }

 private:
  struct ReferenceEntry {
    uint32_t ref_object_number;
    uint32_t referenced_object_number;
  };

  std::vector<ReferenceEntry> GetReferenceEntries() {
    std::vector<ReferenceEntry> reference_entries;
    while (!object_queue_.empty()) {
      RetainPtr<const CPDF_Object> current_object = object_queue_.front();
      object_queue_.pop();

      switch (current_object->GetType()) {
        case CPDF_Object::kArray: {
          CPDF_ArrayLocker locker(current_object->AsArray());
          for (const auto& it : locker) {
            PushNewObject(current_object, it);
          }
          break;
        }
        case CPDF_Object::kDictionary: {
          CPDF_DictionaryLocker locker(current_object->AsDictionary());
          for (const auto& it : locker) {
            PushNewObject(current_object, it.second);
          }
          break;
        }
        case CPDF_Object::kReference: {
          const CPDF_Reference* ref_object = current_object->AsReference();
          const uint32_t ref_object_number = GetObjectNumber(ref_object);
          const uint32_t referenced_object_number = ref_object->GetRefObjNum();

          RetainPtr<const CPDF_Object> referenced_object;
          if (ref_object->HasIndirectObjectHolder()) {
            // Calling GetIndirectObject() does not work for normal references.
            referenced_object = ref_object->GetDirect();
          } else {
            // Calling GetDirect() does not work for references from trailers.
            referenced_object =
                document_->GetIndirectObject(referenced_object_number);
          }
          // Unlike the other object types, CPDF_Reference can point at nullptr.
          if (referenced_object) {
            CHECK(referenced_object_number);
            reference_entries.push_back(
                {ref_object_number, referenced_object_number});
            PushNewObject(ref_object, referenced_object);
          }
          break;
        }
        case CPDF_Object::kStream: {
          RetainPtr<const CPDF_Dictionary> dict =
              current_object->AsStream()->GetDict();
          CHECK(dict->IsInline());  // i.e. No object number.
          CPDF_DictionaryLocker locker(dict);
          for (const auto& it : locker) {
            PushNewObject(current_object, it.second);
          }
          break;
        }
        default: {
          break;
        }
      }
    }
    return reference_entries;
  }

  void CalculateReferenceCounts(
      const std::vector<ReferenceEntry>& reference_entries) {
    // Tracks PDF objects that referenced other PDF objects, identified by their
    // object numbers. Never 0.
    std::set<uint32_t> seen_ref_objects;

    for (const ReferenceEntry& entry : reference_entries) {
      // Make sure this is not a self-reference.
      if (entry.referenced_object_number == entry.ref_object_number) {
        continue;
      }

      // Make sure this is not a circular reference.
      if (pdfium::Contains(seen_ref_objects, entry.ref_object_number) &&
          pdfium::Contains(seen_ref_objects, entry.referenced_object_number)) {
        continue;
      }

      ++referenced_objects_[entry.referenced_object_number];
      if (entry.ref_object_number) {
        seen_ref_objects.insert(entry.ref_object_number);
      }
    }
  }

  void PushNewObject(const CPDF_Object* parent_object,
                     RetainPtr<const CPDF_Object> child_object) {
    CHECK(parent_object);
    CHECK(child_object);
    const bool inserted = seen_objects_.insert(child_object).second;
    if (!inserted) {
      return;
    }
    const uint32_t child_object_number = child_object->GetObjNum();
    if (child_object_number) {
      object_number_map_[child_object] = child_object_number;
    } else {
      // This search can fail for inlined trailers.
      auto it = object_number_map_.find(parent_object);
      if (it != object_number_map_.end()) {
        object_number_map_[child_object] = it->second;
      }
    }
    object_queue_.push(std::move(child_object));
  }

  // Returns 0 if not found.
  uint32_t GetObjectNumber(const CPDF_Object* object) const {
    auto it = object_number_map_.find(object);
    return it != object_number_map_.end() ? it->second : 0;
  }

  UnownedPtr<const CPDF_Document> const document_;

  // Queue of objects to traverse.
  // - Pointers in the queue are non-null.
  // - The same pointer never enters the queue twice.
  std::queue<RetainPtr<const CPDF_Object>> object_queue_;

  // Map of objects to "top-level" object numbers. For inline objects, this is
  // the ancestor object with an object number. The keys are non-null and the
  // values are never 0.
  // This is used to prevent self-references, as a single PDF object, with
  // inlined objects, is represented by multiple CPDF_Objects.
  std::map<const CPDF_Object*, uint32_t> object_number_map_;

  // Tracks traversed objects to prevent duplicates from getting into
  // `object_queue_` and `object_number_map_`.
  std::set<const CPDF_Object*> seen_objects_;

  // Tracks which PDF objects are referenced.
  // Key: object number
  // Value: number of times referenced
  std::map<uint32_t, int> referenced_objects_;
};

}  // namespace

std::set<uint32_t> GetObjectsWithReferences(const CPDF_Document* document) {
  ObjectTreeTraverser traverser(document);
  traverser.Traverse();

  std::set<uint32_t> results;
  for (const auto& it : traverser.referenced_objects()) {
    results.insert(it.first);
  }
  return results;
}

std::set<uint32_t> GetObjectsWithMultipleReferences(
    const CPDF_Document* document) {
  ObjectTreeTraverser traverser(document);
  traverser.Traverse();

  std::set<uint32_t> results;
  for (const auto& it : traverser.referenced_objects()) {
    if (it.second > 1) {
      results.insert(it.first);
    }
  }
  return results;
}
