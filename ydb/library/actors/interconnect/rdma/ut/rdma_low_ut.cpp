#include <string.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/link_manager.h>
#include <ydb/library/actors/interconnect/interconnect_address.h>

using namespace NInterconnect::NRdma;

Y_UNIT_TEST_SUITE(RdmaLow) {
    Y_UNIT_TEST(ReadInOneProcess) {
        const size_t memRegSize = 4096;

        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

        NInterconnect::TAddress address("::1", 7777);
        Cerr << address.ToString() << Endl;

        auto ctx = NInterconnect::NRdma::NLinkMgr::GetCtx(address.GetV6CompatAddr());
        UNIT_ASSERT(ctx);
        Cerr << "Using verbs context: " << *ctx << Endl;

        TQueuePair qp1;
        {
            int err = qp1.Init(ctx);
            UNIT_ASSERT(err == 0);
        }

        auto reg1 = memPool->Alloc(memRegSize);

        memset(reg1->GetAddr(), 0, memRegSize); 
        const char* testRtring = "-_RMDA_YDB_INTERCONNRCT_-";
        strncpy((char*)reg1->GetAddr(), testRtring, memRegSize);

        auto qp1num = qp1.GetQpNum();

        TQueuePair qp2;
        {
            int err = qp2.Init(ctx);
            UNIT_ASSERT(err == 0);
            err = qp2.ToRtsState(ctx, qp1num, ctx->GetGid(), ctx->GetPortAttr().active_mtu);
            UNIT_ASSERT(err == 0);
        }

        {
            int err = qp1.ToRtsState(ctx, qp2.GetQpNum(), ctx->GetGid(), ctx->GetPortAttr().active_mtu);
            UNIT_ASSERT(err == 0);
        }

        auto reg2 = memPool->Alloc(memRegSize);

        qp2.SendRdmaReadWr(123, reg2->GetAddr(), reg2->GetLKey(ctx->GetDeviceIndex()), reg1->GetAddr(), reg1->GetRKey(ctx->GetDeviceIndex()), memRegSize);

        qp2.ProcessCq();

        UNIT_ASSERT(strncmp((char*)reg1->GetAddr(), (char*)reg2->GetAddr(), memRegSize) == 0);
    }
}
 
