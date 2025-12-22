#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

namespace NMonitoring {
    struct TDynamicCounters;
}

static void GTestSkip() {
    GTEST_SKIP() << "Skipping all rdma tests for suit, set \""
                 << NRdmaTest::RdmaTestEnvSwitchName << "\" env if it is RDMA compatible";
}

class TAllocatorSuite : public ::testing::Test {
protected:
    void SetUp() override {
        using namespace NRdmaTest;
        if (IsRdmaTestDisabled()) {
            GTestSkip();
        }
    }
};

TEST_F(TAllocatorSuite, SlotPoolLimit) {
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 32
    };
    static auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    const size_t sz = 4 << 20;
    std::vector<NInterconnect::NRdma::TMemRegionPtr> regions;
    regions.reserve(8);
    size_t i = 0;
    for (;;i++) {
        auto reg = pool->Alloc(sz, 0);
        if (!reg) {
            UNIT_ASSERT(i == 8); // 32 / 4
            break;
        }
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        ASSERT_TRUE(reg->GetSize() == sz) << "invalid size of allocated chunk";
        regions.push_back(reg);
    }

    regions.erase(regions.begin()); // free one region

    {
        auto reg = pool->Alloc(sz, 0); // allocate one
        ASSERT_TRUE(reg->GetAddr()) << "invalid address";
        ASSERT_TRUE(reg->GetSize() == sz) << "invalid size of allocated chunk";
        UNIT_ASSERT(!pool->Alloc(sz, 0)); // pool is full
    }

    regions.clear();
}
