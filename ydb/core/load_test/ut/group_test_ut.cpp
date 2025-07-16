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

    TString RunSingleLoadTest(const TString& command, bool checkRdmaMemory = false) {
        if (checkRdmaMemory) {
            Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPut::EventType) {
                    auto* res = ev->Get<TEvBlobStorage::TEvVPut>();
                    UNIT_ASSERT(res);
                    auto payload = res->GetPayload();
                    for (auto& rope : payload) {
                        for (auto it = rope.Begin(); it != rope.End(); ++it) {
                            const TRcBuf& chunk = it.GetChunk();
                            auto memReg = NInterconnect::NRdma::TryExtractFromRcBuf(chunk);
                            UNIT_ASSERT_C(!memReg.Empty(), "unable to extract mem region from chunk");
                            UNIT_ASSERT_C(memReg.GetLKey(0) != 0, "invalid lkey");
                            UNIT_ASSERT_C(memReg.GetRKey(0) != 0, "invalid rkey");
                        }
                    }
                }
                return true;
            };
        }

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

std::vector<ui64> GetOutputValue(const TStringBuf& html, const TStringBuf& param) {
    TStringBuilder str;
    str << "<tr><td>" << param << "</td><td>";
    std::vector<ui64> result;
    TStringBuf::size_type pos = 0;
    do {
        pos = html.find(str, pos);
        if (TStringBuf::npos != pos) {
            const auto from = pos + str.length();
            const auto to = html.find("</td></tr>", from);
            if (TStringBuf::npos != to) {
                result.emplace_back(FromString<ui64>(html.substr(from, to - from)));
                pos = to + 10U;
            }
        }
    } while (TStringBuf::npos != pos);
    UNIT_ASSERT(!result.empty());
    return result;
}

}

Y_UNIT_TEST_SUITE(GroupWriteTest) {
    Y_UNIT_TEST(Simple) {
        TTetsEnv env;

        const TString conf(R"(DurationSeconds: 30
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
        UNIT_ASSERT(GetOutputValue(html, "OkPutResults").front() >= 300U);
        UNIT_ASSERT(GetOutputValue(html, "BadPutResults").front() == 0U);
        UNIT_ASSERT(GetOutputValue(html, "TotalBytesWritten").front() >= 300000000U);
        UNIT_ASSERT(GetOutputValue(html, "TotalBytesRead").front() == 0U);
    }

    Y_UNIT_TEST(SimpleRdma) {
        TTetsEnv env;

        const TString conf(R"(DurationSeconds: 30
            Tablets: {
                Tablets: { TabletId: 1 Channel: 0 GroupId: )" + ToString(env.GroupInfo->GroupID) + R"( Generation: 1 }
                WriteSizes: { Weight: 1.0 Min: 1000000 Max: 4000000 }
                WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }
                MaxInFlightWriteRequests: 10
                FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }
                PutHandleClass: TabletLog
                RdmaMode: 1
            })"
        );

        const auto html = env.RunSingleLoadTest(conf, true);
        UNIT_ASSERT(GetOutputValue(html, "OkPutResults").front() >= 300U);
        UNIT_ASSERT(GetOutputValue(html, "BadPutResults").front() == 0U);
        UNIT_ASSERT(GetOutputValue(html, "TotalBytesWritten").front() >= 300000000U);
        UNIT_ASSERT(GetOutputValue(html, "TotalBytesRead").front() == 0U);
    }

    Y_UNIT_TEST(ByTableName) {
        TTetsEnv env;

        const TString conf(R"(DurationSeconds: 30
            Tablets: {
                Tablets: { TabletName: "NewTable" Channel: 0 GroupId: )" + ToString(env.GroupInfo->GroupID) + R"( Generation: 1 }
                WriteSizes: { Weight: 1.0 Min: 2000000 Max: 2000000 }
                WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }
                MaxInFlightWriteRequests: 10
                FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }
                PutHandleClass: TabletLog
            })"
        );

        const auto html = env.RunSingleLoadTest(conf);
        UNIT_ASSERT(GetOutputValue(html, "OkPutResults").front() >= 300U);
        UNIT_ASSERT(GetOutputValue(html, "BadPutResults").front() == 0U);
        UNIT_ASSERT(GetOutputValue(html, "TotalBytesWritten").front() >= 600000000U);
        UNIT_ASSERT(GetOutputValue(html, "TotalBytesRead").front() == 0U);
    }

    Y_UNIT_TEST(WithRead) {
        TTetsEnv env;

        const TString conf(R"(DurationSeconds: 10
            Tablets: {
                Tablets: { TabletId: 3 Channel: 0 GroupId: )" + ToString(env.GroupInfo->GroupID) + R"( Generation: 1 }
                WriteSizes: { Weight: 1.0 Min: 1000000 Max: 3000000 }
                WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }
                MaxInFlightWriteRequests: 1
                FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }
                ReadSizes: { Weight: 1.0 Min: 1000000 Max: 2000000 }
                ReadIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }
                MaxInFlightReadRequests: 1
                PutHandleClass: UserData
            })"
        );

        const auto html = env.RunSingleLoadTest(conf);
        UNIT_ASSERT(GetOutputValue(html, "OkPutResults").front() >= 100U);
        UNIT_ASSERT(GetOutputValue(html, "BadPutResults").front() == 0U);
        const auto totalWritten = GetOutputValue(html, "TotalBytesWritten").front();
        const auto totalRead = GetOutputValue(html, "TotalBytesRead").front();
        UNIT_ASSERT(totalWritten >= 100000000U);
        UNIT_ASSERT(totalWritten <= 300000000U);
        UNIT_ASSERT(totalRead >= 100000000U);
        UNIT_ASSERT(totalRead <= 200000000U);
    }

    Y_UNIT_TEST(TwoTables) {
        TTetsEnv env;

        const TString conf(R"(DurationSeconds: 20
            Tablets: {
                Tablets: { TabletName: "TableOne" Channel: 0 GroupId: )" + ToString(env.GroupInfo->GroupID) + R"( Generation: 1 }
                WriteSizes: { Weight: 1.0 Min: 2000000 Max: 3000000 }
                WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }
                MaxInFlightWriteRequests: 10
                FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }
                PutHandleClass: TabletLog
            }
            Tablets: {
                Tablets: { TabletName: "TableTwo" Channel: 0 GroupId: )" + ToString(env.GroupInfo->GroupID) + R"( Generation: 1 }
                WriteSizes: { Weight: 1.0 Min: 100000 Max: 200000 }
                WriteIntervals: { Weight: 1.0 Uniform: { MinUs: 100000 MaxUs: 100000 } }
                MaxInFlightWriteRequests: 10
                FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }
                PutHandleClass: TabletLog
            }
        )");

        const auto html = env.RunSingleLoadTest(conf);
        const auto okResults = GetOutputValue(html, "OkPutResults");
        const auto badResults = GetOutputValue(html, "BadPutResults");
        const auto totalWritten = GetOutputValue(html, "TotalBytesWritten");
        const auto totalRead = GetOutputValue(html, "TotalBytesRead");
        UNIT_ASSERT(okResults.front() >= 200U);
        UNIT_ASSERT(okResults.back() >= 200U);
        UNIT_ASSERT(badResults.front() + badResults.back() == 0U);
        UNIT_ASSERT(totalWritten.front() >= 400000000U);
        UNIT_ASSERT(totalWritten.front() <= 600000000U);
        UNIT_ASSERT(totalWritten.back() >= 20000000U);
        UNIT_ASSERT(totalWritten.back() <= 40000000U);
    }

    Y_UNIT_TEST(WriteHardRateDispatcher) {
        TTetsEnv env;

        const TString conf(R"(DurationSeconds: 10
            Tablets: {
                Tablets: { TabletId: 5 Channel: 0 GroupId: )" + ToString(env.GroupInfo->GroupID) + R"( Generation: 1 }
                WriteSizes: { Weight: 1.0 Min: 1000000 Max: 4000000 }
                WriteHardRateDispatcher: {
                    RequestsPerSecondAtStart: 1
                    RequestsPerSecondOnFinish: 1000
                }
                MaxInFlightWriteRequests: 5
                FlushIntervals: { Weight: 1.0 Uniform: { MinUs: 1000000 MaxUs: 1000000 } }
                PutHandleClass: TabletLog
            })"
        );

        const auto html = env.RunSingleLoadTest(conf);
        UNIT_ASSERT(GetOutputValue(html, "OkPutResults").front() >= 4990U);
        UNIT_ASSERT(GetOutputValue(html, "OkPutResults").front() <= 5010U);
        UNIT_ASSERT(GetOutputValue(html, "BadPutResults").front() == 0U);
    }
}
