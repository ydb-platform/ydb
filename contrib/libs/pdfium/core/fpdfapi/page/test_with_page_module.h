// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FPDFAPI_PAGE_TEST_WITH_PAGE_MODULE_H_
#define CORE_FPDFAPI_PAGE_TEST_WITH_PAGE_MODULE_H_

#include "testing/gtest/include/gtest/gtest.h"

class TestWithPageModule : public testing::Test {
 public:
  void SetUp() override;
  void TearDown() override;
};

#endif  // CORE_FPDFAPI_PAGE_TEST_WITH_PAGE_MODULE_H_
