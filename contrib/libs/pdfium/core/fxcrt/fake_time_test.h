// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_FAKE_TIME_TEST_H_
#define CORE_FXCRT_FAKE_TIME_TEST_H_

#include "testing/gtest/include/gtest/gtest.h"

class FakeTimeTest : public ::testing::Test {
 public:
  void SetUp() override;
  void TearDown() override;
};

#endif  // CORE_FXCRT_FAKE_TIME_TEST_H_
