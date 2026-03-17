// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_SCOPED_SET_INSERTION_H_
#define CORE_FXCRT_SCOPED_SET_INSERTION_H_

#include <set>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/unowned_ptr.h"

namespace fxcrt {

// Track the addition of an object to a set, removing it automatically when
// the ScopedSetInsertion goes out of scope.
template <typename T>
class ScopedSetInsertion {
 public:
  FX_STACK_ALLOCATED();

  ScopedSetInsertion(std::set<T>* org_set, const T& elem)
      : set_(org_set), insert_results_(set_->insert(elem)) {
    CHECK(insert_results_.second);
  }
  ScopedSetInsertion(const ScopedSetInsertion&) = delete;
  ScopedSetInsertion& operator=(const ScopedSetInsertion&) = delete;
  ~ScopedSetInsertion() { set_->erase(insert_results_.first); }

 private:
  UnownedPtr<std::set<T>> const set_;
  const std::pair<typename std::set<T>::iterator, bool> insert_results_;
};

}  // namespace fxcrt

using fxcrt::ScopedSetInsertion;

#endif  // CORE_FXCRT_SCOPED_SET_INSERTION_H_
