// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef FPDFSDK_FORMFILLER_CFFL_FIELDACTION_H_
#define FPDFSDK_FORMFILLER_CFFL_FIELDACTION_H_

#include "core/fxcrt/widestring.h"

struct CFFL_FieldAction {
  CFFL_FieldAction();
  CFFL_FieldAction(const CFFL_FieldAction& other) = delete;
  ~CFFL_FieldAction();

  bool bModifier = false;
  bool bShift = false;
  bool bKeyDown = false;
  bool bWillCommit = false;
  bool bFieldFull = false;
  bool bRC = true;
  int nSelEnd = 0;
  int nSelStart = 0;
  WideString sChange;
  WideString sChangeEx;
  WideString sValue;
};

#endif  // FPDFSDK_FORMFILLER_CFFL_FIELDACTION_H_
