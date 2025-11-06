#pragma once

#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/library/actors/interconnect/rdma/cq_actor/cq_actor.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>

#include <ydb/library/actors/testlib/test_runtime.h>

namespace NInterconnect::NRdma {
    class TRdmaCtx;
}

namespace NRdmaTest {

inline void GTestSkip() {
    GTEST_SKIP() << "Skipping all rdma tests for suit, set \""
                 << RdmaTestEnvSwitchName << "\" env if it is RDMA compatible";
}

struct TLocalRdmaStuff {
    std::shared_ptr<NInterconnect::NRdma::IMemPool> MemPool;
    THolder<NActors::TTestActorRuntimeBase> ActorSystem;
    NActors::TActorId CqActorId;
    NInterconnect::NRdma::ICq::TPtr CqPtr;
    NInterconnect::NRdma::TRdmaCtx* Ctx;
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> Qp1;
    std::shared_ptr<NInterconnect::NRdma::TQueuePair> Qp2;
};

enum class EReadResult {
    OK,
    WRPOST_ERR,
    READ_ERR
};

std::tuple<THolder<NActors::TTestActorRuntimeBase>, NInterconnect::NRdma::TRdmaCtx*> PrepareTestRuntime(TString defIp);

std::shared_ptr<TLocalRdmaStuff> InitLocalRdmaStuff(TString bindTo="::1");

EReadResult ReadOneMemRegion(std::shared_ptr<TLocalRdmaStuff> rdma, std::shared_ptr<NInterconnect::NRdma::TQueuePair> qp,
    void* dstAddr, ui32 dstRkey, int dstSize, NInterconnect::NRdma::TMemRegionPtr& src, std::function<void()> hook = {});

NInterconnect::NRdma::ICq::TPtr GetCqHandle(NActors::TTestActorRuntimeBase* actorSystem,
    NInterconnect::NRdma::TRdmaCtx* ctx, NActors::TActorId cqActorId);

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

