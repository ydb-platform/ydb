// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_EDIT_CPDF_PAGECONTENTMANAGER_H_
#define CORE_FPDFAPI_EDIT_CPDF_PAGECONTENTMANAGER_H_

#include <stdint.h>

#include <set>

#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include <absl/types/variant.h>

class CPDF_Array;
class CPDF_Document;
class CPDF_PageObjectHolder;
class CPDF_Stream;

class CPDF_PageContentManager {
 public:
  CPDF_PageContentManager(CPDF_PageObjectHolder* page_obj_holder,
                          CPDF_Document* document);
  ~CPDF_PageContentManager();

  // Adds a new Content stream. Its index in the array will be returned, or 0
  // if Contents is not an array, but only a single stream.
  size_t AddStream(fxcrt::ostringstream* buf);

  // Changes the stream at `stream_index` to contain the data in `buf`. If `buf`
  // is empty, then schedule the removal of the stream instead.
  void UpdateStream(size_t stream_index, fxcrt::ostringstream* buf);

 private:
  // Gets the Content stream at a given index. If Contents is a single stream
  // rather than an array, it is retrievable at index 0.
  RetainPtr<CPDF_Stream> GetStreamByIndex(size_t stream_index);

  // Schedules the removal of the Content stream at a given index. It will be
  // removed upon CPDF_PageContentManager destruction.
  void ScheduleRemoveStreamByIndex(size_t stream_index);

  // Removes all Content streams for which ScheduleRemoveStreamByIndex() was
  // called. Update the content stream of all page objects with the shifted
  // indexes.
  void ExecuteScheduledRemovals();

  RetainPtr<CPDF_Stream> GetContentsStream();
  RetainPtr<CPDF_Array> GetContentsArray();

  UnownedPtr<CPDF_PageObjectHolder> const page_obj_holder_;
  UnownedPtr<CPDF_Document> const document_;
  const std::set<uint32_t> objects_with_multi_refs_;
  // When holding a CPDF_Stream, the pointer may be null.
  absl::variant<RetainPtr<CPDF_Stream>, RetainPtr<CPDF_Array>> contents_;
  std::set<size_t> streams_to_remove_;
};

#endif  // CORE_FPDFAPI_EDIT_CPDF_PAGECONTENTMANAGER_H_
