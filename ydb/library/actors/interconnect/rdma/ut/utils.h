#pragma once

#include <library/cpp/testing/gtest/gtest.h>

namespace NRdmaTest {

extern const char* RdmaTestEnvSwitchName;
bool IsRdmaTestDisabled();

inline void GTestSkip() {
    GTEST_SKIP() << "Skipping all rdma tests for suit, set \""
                 << RdmaTestEnvSwitchName << "\" env if it is RDMA compatible";
}

}

class TSkipFixture : public ::testing::Test {
protected:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

template <typename T>
class TSkipFixtureWithParams : public ::testing::TestWithParam<T> {
protected:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

