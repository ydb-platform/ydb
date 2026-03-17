// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_CPDFSDK_ANNOTITERATION_H_
#define FPDFSDK_CPDFSDK_ANNOTITERATION_H_

#include <vector>

#include "fpdfsdk/cpdfsdk_annot.h"

class CPDFSDK_PageView;

class CPDFSDK_AnnotIteration {
 public:
  using const_iterator =
      std::vector<ObservedPtr<CPDFSDK_Annot>>::const_iterator;

  static CPDFSDK_AnnotIteration CreateForDrawing(CPDFSDK_PageView* page_view);

  explicit CPDFSDK_AnnotIteration(CPDFSDK_PageView* page_view);
  CPDFSDK_AnnotIteration(const CPDFSDK_AnnotIteration&) = delete;
  CPDFSDK_AnnotIteration& operator=(const CPDFSDK_AnnotIteration&) = delete;
  ~CPDFSDK_AnnotIteration();

  const_iterator begin() const { return list_.begin(); }
  const_iterator end() const { return list_.end(); }

 private:
  CPDFSDK_AnnotIteration(CPDFSDK_PageView* page_view,
                         bool put_focused_annot_at_end);

  std::vector<ObservedPtr<CPDFSDK_Annot>> list_;
};

#endif  // FPDFSDK_CPDFSDK_ANNOTITERATION_H_
