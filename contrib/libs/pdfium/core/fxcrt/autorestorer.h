// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_AUTORESTORER_H_
#define CORE_FXCRT_AUTORESTORER_H_

#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/unowned_ptr.h"

namespace fxcrt {

template <typename T>
class AutoRestorer {
 public:
  FX_STACK_ALLOCATED();

  explicit AutoRestorer(T* location)
      : m_Location(location), m_OldValue(*location) {}
  ~AutoRestorer() {
    if (m_Location)
      *m_Location = m_OldValue;
  }
  void AbandonRestoration() { m_Location = nullptr; }

 private:
  UnownedPtr<T> m_Location;
  const T m_OldValue;
};

}  // namespace fxcrt

using fxcrt::AutoRestorer;

#endif  // CORE_FXCRT_AUTORESTORER_H_
