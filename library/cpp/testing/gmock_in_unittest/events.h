#pragma once

#include <gtest/gtest.h>

class TGMockTestEventListener: public testing::EmptyTestEventListener {
public:
    void OnTestPartResult(const testing::TestPartResult& result) override;
};
