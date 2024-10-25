#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/load_test/service_actor.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace {

struct TTetsEnv {
    TTetsEnv()
    : Env({
        .NodeCount = 8,
        .VDiskReplPausedAtStart = false,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
    })
    , Counters(new ::NMonitoring::TDynamicCounters())
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        VDiskActorId = GroupInfo->GetActorId(0);

        Env.Runtime->SetLogPriority(NKikimrServices::BS_LOAD_TEST, NLog::PRI_DEBUG);
    }

    TString RunSingleLoadTest(const TString& command) {
        const auto sender = Env.Runtime->AllocateEdgeActor(VDiskActorId.NodeId(), __FILE__, __LINE__);
        auto stream = TStringInput(command);

        const ui64 tag = 42ULL;
        GroupWriteActorId = Env.Runtime->Register(CreateWriterLoadTest(ParseFromTextFormat<NKikimr::TEvLoadTestRequest::TStorageLoad>(stream), sender, Counters, tag), sender, 0, std::nullopt, VDiskActorId.NodeId());

        const auto res = Env.WaitForEdgeActorEvent<TEvLoad::TEvLoadTestFinished>(sender, true);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Tag, tag);
        return res->Get()->LastHtmlPage;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    TActorId GroupWriteActorId;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
};

}

Y_UNIT_TEST_SUITE(GroupWriteTest) {

    Y_UNIT_TEST(Simple) {
        TTetsEnv env;

        const TString conf(R"(DurationSeconds: 3
            Tablets: {
                Tablets: { TabletId: 1 Channel: 0 GroupId: )" + ToString(env.GroupInfo->GroupID) + R"( Generation: 1 }
                WriteSizes: { Weight: 1.0 Min: 1000000 Max: 4000000 }
                WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }
                MaxInFlightWriteRequests: 10
                FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }
                PutHandleClass: TabletLog
            })"
        );

        const auto html = env.RunSingleLoadTest(conf);
        UNIT_ASSERT(html.Contains("<tr><td>BadPutResults</td><td>0</td></tr>"));
    }
}
