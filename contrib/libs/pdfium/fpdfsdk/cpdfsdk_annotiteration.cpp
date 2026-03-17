// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/cpdfsdk_annotiteration.h"

#include <algorithm>

#include "fpdfsdk/cpdfsdk_annot.h"
#include "fpdfsdk/cpdfsdk_pageview.h"

// static
CPDFSDK_AnnotIteration CPDFSDK_AnnotIteration::CreateForDrawing(
    CPDFSDK_PageView* page_view) {
  CPDFSDK_AnnotIteration result(page_view);
  return CPDFSDK_AnnotIteration(page_view, /*put_focused_annot_at_end=*/true);
}

CPDFSDK_AnnotIteration::CPDFSDK_AnnotIteration(CPDFSDK_PageView* page_view)
    : CPDFSDK_AnnotIteration(page_view, /*put_focused_annot_at_end=*/false) {}

CPDFSDK_AnnotIteration::CPDFSDK_AnnotIteration(CPDFSDK_PageView* page_view,
                                               bool put_focused_annot_at_end) {
  // Copying ObservedPtrs is expensive, so do it once at the end.
  std::vector<CPDFSDK_Annot*> copied_list = page_view->GetAnnotList();
  std::stable_sort(copied_list.begin(), copied_list.end(),
                   [](const CPDFSDK_Annot* p1, const CPDFSDK_Annot* p2) {
                     return p1->GetLayoutOrder() < p2->GetLayoutOrder();
                   });

  CPDFSDK_Annot* pTopMostAnnot = page_view->GetFocusAnnot();
  if (pTopMostAnnot) {
    auto it = std::find(copied_list.begin(), copied_list.end(), pTopMostAnnot);
    if (it != copied_list.end()) {
      copied_list.erase(it);
      auto insert_it =
          put_focused_annot_at_end ? copied_list.end() : copied_list.begin();
      copied_list.insert(insert_it, pTopMostAnnot);
    }
  }

  list_.reserve(copied_list.size());
  for (auto* pAnnot : copied_list)
    list_.emplace_back(pAnnot);
}

CPDFSDK_AnnotIteration::~CPDFSDK_AnnotIteration() = default;
