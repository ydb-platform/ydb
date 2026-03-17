// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/edit/cpdf_pagecontentmanager.h"

#include <stdint.h>

#include <map>
#include <numeric>
#include <sstream>
#include <utility>
#include <vector>

#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/page/cpdf_pageobjectholder.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/object_tree_traversal_util.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/adapters.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include <absl/types/variant.h>

CPDF_PageContentManager::CPDF_PageContentManager(
    CPDF_PageObjectHolder* page_obj_holder,
    CPDF_Document* document)
    : page_obj_holder_(page_obj_holder),
      document_(document),
      objects_with_multi_refs_(GetObjectsWithMultipleReferences(document_)) {
  RetainPtr<CPDF_Dictionary> page_dict = page_obj_holder_->GetMutableDict();
  RetainPtr<CPDF_Object> contents_obj =
      page_dict->GetMutableObjectFor("Contents");
  RetainPtr<CPDF_Array> contents_array = ToArray(contents_obj);
  if (contents_array) {
    CHECK(contents_array->IsInline());
    contents_ = std::move(contents_array);
    return;
  }

  RetainPtr<CPDF_Reference> contents_reference = ToReference(contents_obj);
  if (contents_reference) {
    RetainPtr<CPDF_Object> indirect_obj =
        contents_reference->GetMutableDirect();
    if (!indirect_obj)
      return;

    contents_array.Reset(indirect_obj->AsMutableArray());
    if (contents_array) {
      if (pdfium::Contains(objects_with_multi_refs_,
                           contents_array->GetObjNum())) {
        RetainPtr<CPDF_Array> cloned_contents_array =
            pdfium::WrapRetain(contents_array->Clone()->AsMutableArray());
        page_dict->SetFor("Contents", cloned_contents_array);
        contents_ = std::move(cloned_contents_array);
      } else {
        contents_ = std::move(contents_array);
      }
    } else if (indirect_obj->IsStream()) {
      contents_ = pdfium::WrapRetain(indirect_obj->AsMutableStream());
    }
  }
}

CPDF_PageContentManager::~CPDF_PageContentManager() {
  ExecuteScheduledRemovals();
}

RetainPtr<CPDF_Stream> CPDF_PageContentManager::GetStreamByIndex(
    size_t stream_index) {
  RetainPtr<CPDF_Stream> contents_stream = GetContentsStream();
  if (contents_stream) {
    return stream_index == 0 ? contents_stream : nullptr;
  }

  RetainPtr<CPDF_Array> contents_array = GetContentsArray();
  if (!contents_array) {
    return nullptr;
  }

  RetainPtr<CPDF_Reference> stream_reference =
      ToReference(contents_array->GetMutableObjectAt(stream_index));
  if (!stream_reference)
    return nullptr;

  return ToStream(stream_reference->GetMutableDirect());
}

size_t CPDF_PageContentManager::AddStream(fxcrt::ostringstream* buf) {
  auto new_stream = document_->NewIndirect<CPDF_Stream>(buf);

  // If there is one Content stream (not in an array), now there will be two, so
  // create an array with the old and the new one. The new one's index is 1.
  RetainPtr<CPDF_Stream> contents_stream = GetContentsStream();
  if (contents_stream) {
    auto new_contents_array = document_->NewIndirect<CPDF_Array>();
    new_contents_array->AppendNew<CPDF_Reference>(document_,
                                                  contents_stream->GetObjNum());
    new_contents_array->AppendNew<CPDF_Reference>(document_,
                                                  new_stream->GetObjNum());

    RetainPtr<CPDF_Dictionary> page_dict = page_obj_holder_->GetMutableDict();
    page_dict->SetNewFor<CPDF_Reference>("Contents", document_,
                                         new_contents_array->GetObjNum());
    contents_ = std::move(new_contents_array);
    return 1;
  }

  // If there is an array, just add the new stream to it, at the last position.
  RetainPtr<CPDF_Array> contents_array = GetContentsArray();
  if (contents_array) {
    contents_array->AppendNew<CPDF_Reference>(document_,
                                              new_stream->GetObjNum());
    return contents_array->size() - 1;
  }

  // There were no Contents, so add the new stream as the single Content stream.
  // Its index is 0.
  RetainPtr<CPDF_Dictionary> page_dict = page_obj_holder_->GetMutableDict();
  page_dict->SetNewFor<CPDF_Reference>("Contents", document_,
                                       new_stream->GetObjNum());
  contents_ = std::move(new_stream);
  return 0;
}

void CPDF_PageContentManager::UpdateStream(size_t stream_index,
                                           fxcrt::ostringstream* buf) {
  // If `buf` is now empty, remove the stream instead of setting the data.
  if (buf->tellp() <= 0) {
    ScheduleRemoveStreamByIndex(stream_index);
    return;
  }

  RetainPtr<CPDF_Stream> existing_stream = GetStreamByIndex(stream_index);
  CHECK(existing_stream);
  if (!pdfium::Contains(objects_with_multi_refs_,
                        existing_stream->GetObjNum())) {
    existing_stream->SetDataFromStringstreamAndRemoveFilter(buf);
    return;
  }

  if (GetContentsStream()) {
    auto new_stream = document_->NewIndirect<CPDF_Stream>(buf);
    RetainPtr<CPDF_Dictionary> page_dict = page_obj_holder_->GetMutableDict();
    page_dict->SetNewFor<CPDF_Reference>("Contents", document_,
                                         new_stream->GetObjNum());
  }

  RetainPtr<CPDF_Array> contents_array = GetContentsArray();
  if (!contents_array) {
    return;
  }

  RetainPtr<CPDF_Reference> stream_reference =
      ToReference(contents_array->GetMutableObjectAt(stream_index));
  if (!stream_reference) {
    return;
  }

  auto new_stream = document_->NewIndirect<CPDF_Stream>(buf);
  stream_reference->SetRef(document_, new_stream->GetObjNum());
}

void CPDF_PageContentManager::ScheduleRemoveStreamByIndex(size_t stream_index) {
  streams_to_remove_.insert(stream_index);
}

void CPDF_PageContentManager::ExecuteScheduledRemovals() {
  // This method assumes there are no dirty streams in the
  // CPDF_PageObjectHolder. If there were any, their indexes would need to be
  // updated.
  // Since CPDF_PageContentManager is only instantiated in
  // CPDF_PageContentGenerator::GenerateContent(), which cleans up the dirty
  // streams first, this should always be true.
  DCHECK(!page_obj_holder_->HasDirtyStreams());

  if (streams_to_remove_.empty()) {
    return;
  }

  RetainPtr<CPDF_Stream> contents_stream = GetContentsStream();
  if (contents_stream) {
    // Only stream that can be removed is 0.
    if (streams_to_remove_.find(0) != streams_to_remove_.end()) {
      RetainPtr<CPDF_Dictionary> page_dict = page_obj_holder_->GetMutableDict();
      page_dict->RemoveFor("Contents");
    }
    return;
  }

  RetainPtr<CPDF_Array> contents_array = GetContentsArray();
  if (!contents_array) {
    return;
  }

  // Initialize a vector with the old stream indexes. This will be used to build
  // a map from the old to the new indexes.
  std::vector<size_t> streams_left(contents_array->size());
  std::iota(streams_left.begin(), streams_left.end(), 0);

  // In reverse order so as to not change the indexes in the middle of the loop,
  // remove the streams.
  for (size_t stream_index : pdfium::Reversed(streams_to_remove_)) {
    contents_array->RemoveAt(stream_index);
    streams_left.erase(streams_left.begin() + stream_index);
  }

  // Create a mapping from the old to the new stream indexes, shifted due to the
  // deletion of the |streams_to_remove_|.
  std::map<size_t, size_t> stream_index_mapping;
  for (size_t i = 0; i < streams_left.size(); ++i) {
    stream_index_mapping[streams_left[i]] = i;
  }

  // Update the page objects' content stream indexes.
  for (const auto& obj : *page_obj_holder_) {
    int32_t old_stream_index = obj->GetContentStream();
    int32_t new_stream_index =
        pdfium::checked_cast<int32_t>(stream_index_mapping[old_stream_index]);
    obj->SetContentStream(new_stream_index);
  }

  // Even if there is a single content stream now, keep the array with a single
  // element. It's valid, a second stream might be added in the near future, and
  // the complexity of removing it is not worth it.
}

RetainPtr<CPDF_Stream> CPDF_PageContentManager::GetContentsStream() {
  if (absl::holds_alternative<RetainPtr<CPDF_Stream>>(contents_)) {
    return absl::get<RetainPtr<CPDF_Stream>>(contents_);
  }
  return nullptr;
}

RetainPtr<CPDF_Array> CPDF_PageContentManager::GetContentsArray() {
  if (absl::holds_alternative<RetainPtr<CPDF_Array>>(contents_)) {
    return absl::get<RetainPtr<CPDF_Array>>(contents_);
  }
  return nullptr;
}
