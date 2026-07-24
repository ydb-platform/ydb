#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_QUEUE

namespace {

struct TTetsEnvBase {
    TTetsEnvBase(TEnvironmentSetup::TSettings&& settings)
    : Env(std::move(settings))
    {
        Env.CreateBoxAndPool(1, 1);

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        VDiskActorId = GroupInfo->GetActorId(0);

        Sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = Env.Invoke(request);
        const auto& baseConfig = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(GroupInfo->GroupID.GetRawId(), baseConfig.GetGroup(0).GetGroupId());

        Env.Sim(TDuration::Minutes(1));
    }

    template<class TEvent>
    void SendToDsProxy(TEvent* event) {
        Env.Runtime->WrapInActorContext(Sender, [&] {
            SendToBSProxy(Sender, GroupInfo->GroupID, event);
        });
    }

    std::unique_ptr<TEvBlobStorage::TEvPut> GetData(ui32 size) {
        static ui32 step = 0;
        Payload = TRcBuf(TString(size, 'a'));
        auto id = TLogoBlobID(123 /*tablet id*/, 1, ++step, 0 /*channel*/, size, 0 /*cookie=*/);
        return std::make_unique<TEvBlobStorage::TEvPut>(id, Payload, TInstant::Max());
    }

    NKikimrProto::EReplyStatus WriteData(ui32 size) {
        SendToDsProxy(GetData(size).release());
        return Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Sender, false)->Get()->Status;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    ui32 CollectGeneration = 0;
    TActorId Sender;
    TRcBuf Payload;
};

} // anon ns

Y_UNIT_TEST_SUITE(UserChecksumming) {

Y_UNIT_TEST(VDiskAcceptsCorrectData) {
    TTetsEnvBase env({
        .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
                config.BlobHeaderMode = EBlobHeaderMode::XXH3_64BIT_HEADER;
            },
    });

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                [[maybe_unused]]auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                break;
            }
        }
        return true;
    };

    ui32 size = 16_KB;
    UNIT_ASSERT_EQUAL(env.WriteData(size), NKikimrProto::OK);
}

Y_UNIT_TEST(VDiskRejectsCorruptedData) {
    TTetsEnvBase env({
        .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
                config.BlobHeaderMode = EBlobHeaderMode::XXH3_64BIT_HEADER;
            },
    });

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                [[maybe_unused]]auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                ++env.Payload[env.Payload.size() / 2];
                break;
            }
        }
        return true;
    };

    ui32 size = 16_KB;
    auto res = env.WriteData(size);
    Cerr << res << "\n";
    UNIT_ASSERT_EQUAL(res, NKikimrProto::ERROR);
}

}