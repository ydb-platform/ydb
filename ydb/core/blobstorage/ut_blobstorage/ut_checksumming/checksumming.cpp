#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <ydb/core/blobstorage/base/blobstorage_checksum.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/vdisk/common/blob_header_mode.h>

#include <functional>

namespace {

TString GenData(ui32 size, ui64 seed = 0) {
    return TEnvironmentSetup::GenerateRandomString(size, seed);
}

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
        // PdiskLayout = MakePDiskLayout(baseConfig, GroupInfo->GetTopology(), baseConfig.GetGroup(0).GetGroupId());

        Env.Sim(TDuration::Minutes(1));
    }

    template<class TEvent>
    void SendToDsProxy(TEvent* event) {
        Env.Runtime->WrapInActorContext(Sender, [&] {
            SendToBSProxy(Sender, GroupInfo->GroupID, event);
        });
    }

    std::unique_ptr<TEvBlobStorage::TEvPut> CreateTEvPut(const TString& data) {
        static ui32 step = 0;
        Payload = TRcBuf(data);
        LastBlobId = TLogoBlobID(123 /*tablet id*/, 1, ++step, 0 /*channel*/, data.size(), 0 /*cookie=*/);
        return std::make_unique<TEvBlobStorage::TEvPut>(LastBlobId, Payload, TInstant::Max());
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> WriteData(const TString& data) {
        SendToDsProxy(CreateTEvPut(data).release());
        return Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Sender, false);
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> ReadDataFromDsProxy(ui32 shift, ui32 size) {
        SendToDsProxy(new TEvBlobStorage::TEvGet(LastBlobId, shift, size, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead));
        return Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(Sender, false);
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> ReadDataFromDsProxy() {
        return ReadDataFromDsProxy(0, LastBlobId.BlobSize());
    }

    void EnableChecksumCalcAndValidationOnDsProxy() {
        Env.SetIcbControl(0, "DSProxyControls.EnableChecksumCalcAndValidationOnDsProxy", 1);
    }

    void EnableChecksumReadValidationOnVDisk() {
        Env.SetIcbControl(0, "VDiskControls.EnableChecksumReadValidationOnVDisk", 1);
    }

    void EnableChecksumWriteValidationOnVDisk() {
        Env.SetIcbControl(0, "VDiskControls.EnableChecksumWriteValidationOnVDisk", 1);
    }

    void RestartVDiskNode() {
        Env.RestartNode(VDiskActorId.NodeId());
        Env.Sim(TDuration::Seconds(5));
        Sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    }

    TString DecryptLastPart(const TString& partData) {
        UNIT_ASSERT_VALUES_EQUAL(GroupInfo->Type.TotalPartCount(), 1);
        TString decryptedData = partData;
        char *buffer = decryptedData.Detach();
        Decrypt(buffer, buffer, 0, decryptedData.size(), LastBlobId, *GroupInfo);
        return decryptedData;
    }

    NKikimrProto::EReplyStatus ReadLastPartFromVDisk(ui32 shift, ui32 size, TString* partData = nullptr,
            const std::function<void(TEvBlobStorage::TEvVGetResult&)>& inspect = {}) {
        NKikimrProto::EReplyStatus status = NKikimrProto::ERROR;
        TBlobStorageGroupInfo::TVDiskIds vdiskIds;
        GroupInfo->PickSubgroup(LastBlobId.Hash(), &vdiskIds, nullptr);
        UNIT_ASSERT(!vdiskIds.empty());
        const TVDiskID& vdiskId = vdiskIds[0];

        Env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
            const TActorId& edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            Env.Runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId,
                TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                Nothing(), {TEvBlobStorage::TEvVGet::TExtremeQuery(LastBlobId, shift, size)}).release()), queueId.NodeId());

            auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
            const auto& record = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), NKikimrProto::OK, response->Get()->ToString());
            UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);

            if (inspect) {
                inspect(*response->Get());
            }

            const auto& result = record.GetResult(0);
            status = result.GetStatus();
            if (partData && status == NKikimrProto::OK) {
                *partData = response->Get()->GetBlobData(result).ConvertToString();
            }
        });

        return status;
    }

    NKikimrProto::EReplyStatus ReadLastPartFromVDisk(TString* partData = nullptr,
            const std::function<void(TEvBlobStorage::TEvVGetResult&)>& inspect = {}) {
        return ReadLastPartFromVDisk(0, 0, partData, inspect);
    }

    NKikimrBlobStorage::TEvVMultiPutResult SendVMultiPutToVDisk(std::unique_ptr<TEvBlobStorage::TEvVMultiPut> event) {
        NKikimrBlobStorage::TEvVMultiPutResult result;
        Env.WithQueueId(GroupInfo->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            const TActorId& edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            Env.Runtime->Send(new IEventHandle(queueId, edge, event.release()), queueId.NodeId());
            auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVMultiPutResult>(edge, false);
            result = response->Get()->Record;
        });
        return result;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    ui32 CollectGeneration = 0;
    TActorId Sender;
    TRcBuf Payload;
    TLogoBlobID LastBlobId;
};

TEnvironmentSetup::TSettings Xxh3HeaderSettings() {
    return {
        .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
            config.BlobHeaderMode = EBlobHeaderMode::XXH3_64BIT_HEADER;
        },
    };
}

void MutateFirstOkVGetResult(TEvBlobStorage::TEvVGetResult& vgetResult,
        const std::function<void(NKikimrBlobStorage::TQueryResult&)>& mutate) {
    UNIT_ASSERT_VALUES_EQUAL(vgetResult.Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT(vgetResult.Record.ResultSize() > 0);
    NKikimrBlobStorage::TQueryResult& result = *vgetResult.Record.MutableResult(0);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
    mutate(result);
}

void CorruptVPutPayload(TEvBlobStorage::TEvVPut& vput) {
    TString data = vput.GetBuffer().ConvertToString();
    UNIT_ASSERT(!data.empty());
    const size_t pos = data.size() / 2;
    data[pos] = data[pos] ^ 1;
    vput.StripPayload();
    vput.AddPayload(TRope(data));
}

void CorruptVGetResultPayload(TEvBlobStorage::TEvVGetResult& vgetResult, NKikimrBlobStorage::TQueryResult& result) {
    UNIT_ASSERT(vgetResult.HasBlob(result));
    TString data = vgetResult.GetBlobData(result).ConvertToString();
    UNIT_ASSERT(!data.empty());
    const size_t pos = data.size() / 2;
    data[pos] = data[pos] ^ 1;
    result.ClearBufferData();
    result.ClearPayloadId();
    vgetResult.SetBlobData(result, TRope(data));
}

ui64 CalculateChecksum(const TString& data) {
    const TRope rope(data);
    return CalculateXxh3Hash(rope.Begin(), rope.GetSize()).second;
}

} // anon ns

Y_UNIT_TEST_SUITE(UserChecksumming) {

Y_UNIT_TEST(PutEightBytesWithXxh3BlobHeader) {
    TTetsEnvBase env(Xxh3HeaderSettings());

    const TString data = "abcdefgh";
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);
    TString partData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(&partData), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(env.DecryptLastPart(partData), data);
}

Y_UNIT_TEST(PutEightBytesWithoutXxh3BlobHeader) {
    TTetsEnvBase env({
        .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
            config.BlobHeaderMode = EBlobHeaderMode::NO_HEADER;
        },
    });

    const TString data = "abcdefgh";
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);
    TString partData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(&partData), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(env.DecryptLastPart(partData), data);
}

Y_UNIT_TEST(ReadsXxh3HeaderBlobAfterHeaderModeRollback) {
    EBlobHeaderMode blobHeaderMode = EBlobHeaderMode::XXH3_64BIT_HEADER;
    ui32 vDiskConfigPreprocessorCalled = 0;
    TTetsEnvBase env({
        .VDiskConfigPreprocessor = [&](TVDiskConfig& config) {
            config.BlobHeaderMode = blobHeaderMode;
            ++vDiskConfigPreprocessorCalled;
        },
    });
    env.EnableChecksumReadValidationOnVDisk();

    const TString data = GenData(16_KB, 11);
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    bool sawStoredChecksum = false;
    TString partData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(&partData, [&](TEvBlobStorage::TEvVGetResult& vgetResult) {
        MutateFirstOkVGetResult(vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
            sawStoredChecksum = true;
            UNIT_ASSERT(result.HasChecksum());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result.GetChecksumType()),
                static_cast<ui32>(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob));
        });
    }), NKikimrProto::OK);
    UNIT_ASSERT(sawStoredChecksum);
    UNIT_ASSERT_VALUES_EQUAL(env.DecryptLastPart(partData), data);

    blobHeaderMode = EBlobHeaderMode::NO_HEADER;
    vDiskConfigPreprocessorCalled = 0;
    env.RestartVDiskNode();
    UNIT_ASSERT_VALUES_EQUAL(vDiskConfigPreprocessorCalled, 1);

    auto readResult = env.ReadDataFromDsProxy();
    UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Status, NKikimrProto::OK, readResult->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->ResponseSz, 1);
    const auto& response = readResult->Get()->Responses[0];
    UNIT_ASSERT_VALUES_EQUAL(response.Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(response.Buffer.ConvertToString(), data);
}

Y_UNIT_TEST(DsProxySendsXxh3ChecksumToVDisk) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();

    bool seenVPut = false;
    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                seenVPut = true;
                UNIT_ASSERT(vput->Record.HasChecksum());
                UNIT_ASSERT(vput->Record.HasChecksumType());
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(vput->Record.GetChecksumType()),
                    static_cast<ui32>(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob));
                const TRope buffer = vput->GetBuffer();
                UNIT_ASSERT_VALUES_EQUAL(vput->Record.GetChecksum(),
                    CalculateXxh3Hash(buffer.Begin(), buffer.GetSize()).second);
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 1));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(seenVPut);
}

Y_UNIT_TEST(DsProxyDoesNotSendXxh3ChecksumByDefault) {
    TTetsEnvBase env(Xxh3HeaderSettings());

    bool seenVPut = false;
    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                seenVPut = true;
                UNIT_ASSERT(!vput->Record.HasChecksum());
                UNIT_ASSERT(!vput->Record.HasChecksumType());
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 12));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(seenVPut);
}

Y_UNIT_TEST(VDiskAcceptsCorrectData) {
    TTetsEnvBase env(Xxh3HeaderSettings());

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                [[maybe_unused]]auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 2));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);
}

Y_UNIT_TEST(VDiskRejectsCorruptedData) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumWriteValidationOnVDisk();

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                CorruptVPutPayload(*vput);
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 3));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::ERROR);
    UNIT_ASSERT_STRING_CONTAINS(writeResult->Get()->ErrorReason, "buffer checksum mismatch");
}

Y_UNIT_TEST(VDiskAcceptsInvalidVPutChecksumWhenWriteValidationDisabled) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                UNIT_ASSERT(vput->Record.HasChecksum());
                vput->Record.SetChecksum(vput->Record.GetChecksum() ^ ui64(1));
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 13));
    UNIT_ASSERT_VALUES_EQUAL_C(writeResult->Get()->Status, NKikimrProto::OK, writeResult->Get()->ErrorReason);
}

Y_UNIT_TEST(VDiskRejectsInvalidVPutChecksum) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumWriteValidationOnVDisk();

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                UNIT_ASSERT(vput->Record.HasChecksum());
                vput->Record.SetChecksum(vput->Record.GetChecksum() ^ ui64(1));
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 4));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::ERROR);
    UNIT_ASSERT_STRING_CONTAINS(writeResult->Get()->ErrorReason, "buffer checksum mismatch");
}

Y_UNIT_TEST(VDiskAcceptsMissingVPutChecksumFields) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumWriteValidationOnVDisk();

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                vput->Record.ClearChecksum();
                vput->Record.ClearChecksumType();
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 5));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);
}

Y_UNIT_TEST(VDiskAcceptsLegacyVPutChecksumWithoutType) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumWriteValidationOnVDisk();

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                UNIT_ASSERT(vput->Record.HasChecksum());
                vput->Record.ClearChecksumType();
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 6));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);
}

Y_UNIT_TEST(VDiskRejectsVPutChecksumTypeWithoutChecksum) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumWriteValidationOnVDisk();

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                vput->Record.ClearChecksum();
                vput->Record.SetChecksumType(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob);
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 7));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::ERROR);
    UNIT_ASSERT_STRING_CONTAINS(writeResult->Get()->ErrorReason, "buffer checksum mismatch");
}

Y_UNIT_TEST(VDiskRejectsVPutChecksumWithNoChecksumType) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumWriteValidationOnVDisk();

    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::EvVPut: {
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                UNIT_ASSERT(vput->Record.HasChecksum());
                vput->Record.SetChecksumType(NKikimrBlobStorage::TChecksumType::NoChecksum);
                break;
            }
        }
        return true;
    };

    auto writeResult = env.WriteData(GenData(16_KB, 8));
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::ERROR);
    UNIT_ASSERT_STRING_CONTAINS(writeResult->Get()->ErrorReason, "buffer checksum mismatch");
}

Y_UNIT_TEST(VDiskAcceptsAndRejectsVMultiPutChecksums) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumWriteValidationOnVDisk();

    struct TChecksumCase {
        const char *Name;
        bool HasChecksum;
        ui64 ChecksumXor;
        bool HasChecksumType;
        NKikimrBlobStorage::TChecksumType ChecksumType;
        NKikimrProto::EReplyStatus ExpectedStatus;
    };

    const TVector<TChecksumCase> cases = {
        {"NoChecksumFields", false, 0, false, NKikimrBlobStorage::TChecksumType::NoChecksum,
            NKikimrProto::OK},
        {"NoChecksumTypeOnly", false, 0, true, NKikimrBlobStorage::TChecksumType::NoChecksum,
            NKikimrProto::OK},
        {"ValidXxh3Checksum", true, 0, true, NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob,
            NKikimrProto::OK},
        {"LegacyValidXxh3ChecksumWithoutType", true, 0, false, NKikimrBlobStorage::TChecksumType::NoChecksum,
            NKikimrProto::OK},
        {"InvalidXxh3Checksum", true, 1, true, NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob,
            NKikimrProto::ERROR},
        {"ChecksumTypeWithoutChecksum", false, 0, true, NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob,
            NKikimrProto::ERROR},
        {"ChecksumWithNoChecksumType", true, 0, true, NKikimrBlobStorage::TChecksumType::NoChecksum,
            NKikimrProto::ERROR},
    };

    ui32 step = 1;
    for (const auto& testCase : cases) {
        const TString data = GenData(64, step);
        const TLogoBlobID id(123 /*tablet id*/, 1, step, 0 /*channel*/, data.size(), step, 1 /*part id*/);
        auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(env.GroupInfo->GetVDiskId(0),
            TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
        multiPut->AddVPut(id, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);

        auto& item = *multiPut->Record.MutableItems(0);
        if (testCase.HasChecksum) {
            item.SetChecksum(CalculateChecksum(data) ^ testCase.ChecksumXor);
        }
        if (testCase.HasChecksumType) {
            item.SetChecksumType(testCase.ChecksumType);
        }

        auto result = env.SendVMultiPutToVDisk(std::move(multiPut));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NKikimrProto::OK, testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(result.ItemsSize(), 1, testCase.Name);
        const auto& resultItem = result.GetItems(0);
        UNIT_ASSERT_VALUES_EQUAL_C(resultItem.GetStatus(), testCase.ExpectedStatus, testCase.Name);

        ++step;
    }
}

Y_UNIT_TEST(VDiskReturnsXxh3ChecksumInVGetResult) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumReadValidationOnVDisk();

    const TString data = "abcdefgh";
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    bool seenVGetResult = false;
    TString partData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(&partData, [&](TEvBlobStorage::TEvVGetResult& vgetResult) {
        MutateFirstOkVGetResult(vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
            seenVGetResult = true;
            UNIT_ASSERT(result.HasChecksum());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result.GetChecksumType()),
                static_cast<ui32>(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob));
            const TRope buffer = vgetResult.GetBlobData(result);
            UNIT_ASSERT_VALUES_EQUAL(result.GetChecksum(),
                CalculateXxh3Hash(buffer.Begin(), buffer.GetSize()).second);
        });
    }), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(env.DecryptLastPart(partData), data);
    UNIT_ASSERT(seenVGetResult);
}

Y_UNIT_TEST(VDiskDoesNotReturnXxh3ChecksumByDefault) {
    TTetsEnvBase env(Xxh3HeaderSettings());

    const TString data = "abcdefgh";
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    bool seenVGetResult = false;
    TString partData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(&partData, [&](TEvBlobStorage::TEvVGetResult& vgetResult) {
        MutateFirstOkVGetResult(vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
            seenVGetResult = true;
            UNIT_ASSERT(!result.HasChecksum());
            UNIT_ASSERT(!result.HasChecksumType());
        });
    }), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(env.DecryptLastPart(partData), data);
    UNIT_ASSERT(seenVGetResult);
}

// ------------------ Substring Reads ------------------

Y_UNIT_TEST(VDiskReturnsXxh3ChecksumForSubstringWhenReadValidationEnabled) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumReadValidationOnVDisk();

    const TString data = GenData(256, 14);
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    constexpr ui32 shift = 17;
    constexpr ui32 size = 53;

    TString fullPartData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(&fullPartData), NKikimrProto::OK);

    bool seenVGetResult = false;
    TString partData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(shift, size, &partData, [&](TEvBlobStorage::TEvVGetResult& vgetResult) {
        MutateFirstOkVGetResult(vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
            seenVGetResult = true;
            UNIT_ASSERT_VALUES_EQUAL(result.GetShift(), shift);
            UNIT_ASSERT(result.HasChecksum());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result.GetChecksumType()),
                static_cast<ui32>(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob));
            const TRope buffer = vgetResult.GetBlobData(result);
            UNIT_ASSERT_VALUES_EQUAL(buffer.GetSize(), size);
            UNIT_ASSERT_VALUES_EQUAL(result.GetChecksum(),
                CalculateXxh3Hash(buffer.Begin(), buffer.GetSize()).second);
        });
    }), NKikimrProto::OK);
    UNIT_ASSERT(seenVGetResult);
    UNIT_ASSERT_VALUES_EQUAL(partData, fullPartData.substr(shift, size));
}

Y_UNIT_TEST(VDiskDoesNotReturnXxh3ChecksumForSubstringByDefault) {
    TTetsEnvBase env(Xxh3HeaderSettings());

    const TString data = GenData(256, 15);
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    constexpr ui32 shift = 11;
    constexpr ui32 size = 37;

    TString fullPartData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(&fullPartData), NKikimrProto::OK);

    bool seenVGetResult = false;
    TString partData;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadLastPartFromVDisk(shift, size, &partData, [&](TEvBlobStorage::TEvVGetResult& vgetResult) {
        MutateFirstOkVGetResult(vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
            seenVGetResult = true;
            UNIT_ASSERT_VALUES_EQUAL(result.GetShift(), shift);
            UNIT_ASSERT(!result.HasChecksum());
            UNIT_ASSERT(!result.HasChecksumType());
            UNIT_ASSERT_VALUES_EQUAL(vgetResult.GetBlobData(result).GetSize(), size);
        });
    }), NKikimrProto::OK);
    UNIT_ASSERT(seenVGetResult);
    UNIT_ASSERT_VALUES_EQUAL(partData, fullPartData.substr(shift, size));
}

Y_UNIT_TEST(DsProxyReturnsSubstringWithChecksumsEnabled) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumReadValidationOnVDisk();

    const TString data = GenData(256, 16);
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    constexpr ui32 shift = 23;
    constexpr ui32 size = 41;

    auto readResult = env.ReadDataFromDsProxy(shift, size);
    UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Status, NKikimrProto::OK, readResult->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->ResponseSz, 1);
    const auto& response = readResult->Get()->Responses[0];
    UNIT_ASSERT_VALUES_EQUAL(response.Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(response.Shift, shift);
    UNIT_ASSERT_VALUES_EQUAL(response.RequestedSize, size);
    UNIT_ASSERT_VALUES_EQUAL(response.Buffer.ConvertToString(), data.substr(shift, size));
}

Y_UNIT_TEST(DsProxyRejectsCorruptedSubstringVGetChecksum) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumReadValidationOnVDisk();

    const TString data = GenData(256, 17);
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    constexpr ui32 shift = 29;
    constexpr ui32 size = 43;

    bool corruptedVGetResult = false;
    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult && !corruptedVGetResult) {
            auto* vgetResult = ev->Get<TEvBlobStorage::TEvVGetResult>();
            corruptedVGetResult = true;
            MutateFirstOkVGetResult(*vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetShift(), shift);
                UNIT_ASSERT(result.HasChecksum());
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result.GetChecksumType()),
                    static_cast<ui32>(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob));
                UNIT_ASSERT_VALUES_EQUAL(vgetResult->GetBlobData(result).GetSize(), size);
                CorruptVGetResultPayload(*vgetResult, result);
            });
        }
        return true;
    };

    auto readResult = env.ReadDataFromDsProxy(shift, size);
    UNIT_ASSERT(corruptedVGetResult);
    UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Status, NKikimrProto::ERROR, readResult->Get()->ToString());
    UNIT_ASSERT_STRING_CONTAINS(readResult->Get()->ErrorReason, "buffer checksum mismatch");
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->ResponseSz, 1);
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Responses[0].Status, NKikimrProto::ERROR);
}

// ------------------ DsProxy ------------------

Y_UNIT_TEST(DsProxyRejectsInvalidVGetChecksum) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumReadValidationOnVDisk();

    const TString data = GenData(64, 9);
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    bool corruptedVGetResult = false;
    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult && !corruptedVGetResult) {
            auto* vgetResult = ev->Get<TEvBlobStorage::TEvVGetResult>();
            corruptedVGetResult = true;
            MutateFirstOkVGetResult(*vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
                UNIT_ASSERT(result.HasChecksum());
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result.GetChecksumType()),
                    static_cast<ui32>(NKikimrBlobStorage::TChecksumType::XXH3_64BitBlob));
                CorruptVGetResultPayload(*vgetResult, result);
            });
        }
        return true;
    };

    auto readResult = env.ReadDataFromDsProxy();
    UNIT_ASSERT(corruptedVGetResult);
    UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Status, NKikimrProto::ERROR, readResult->Get()->ToString());
    UNIT_ASSERT_STRING_CONTAINS(readResult->Get()->ErrorReason, "buffer checksum mismatch");
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->ResponseSz, 1);
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->Responses[0].Status, NKikimrProto::ERROR);
}

Y_UNIT_TEST(DsProxyAcceptsCorruptedVGetDataWithoutChecksum) {
    TTetsEnvBase env(Xxh3HeaderSettings());
    env.EnableChecksumCalcAndValidationOnDsProxy();
    env.EnableChecksumReadValidationOnVDisk();

    const TString data = GenData(64, 10);
    auto writeResult = env.WriteData(data);
    UNIT_ASSERT_EQUAL(writeResult->Get()->Status, NKikimrProto::OK);

    bool corruptedVGetResult = false;
    env.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult && !corruptedVGetResult) {
            auto* vgetResult = ev->Get<TEvBlobStorage::TEvVGetResult>();
            corruptedVGetResult = true;
            MutateFirstOkVGetResult(*vgetResult, [&](NKikimrBlobStorage::TQueryResult& result) {
                CorruptVGetResultPayload(*vgetResult, result);
                result.ClearChecksum();
                result.SetChecksumType(NKikimrBlobStorage::TChecksumType::NoChecksum);
            });
        }
        return true;
    };

    auto readResult = env.ReadDataFromDsProxy();
    UNIT_ASSERT(corruptedVGetResult);
    UNIT_ASSERT_VALUES_EQUAL_C(readResult->Get()->Status, NKikimrProto::OK, readResult->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(readResult->Get()->ResponseSz, 1);
    const auto& response = readResult->Get()->Responses[0];
    UNIT_ASSERT_VALUES_EQUAL(response.Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(response.Buffer.size(), data.size());
    UNIT_ASSERT_VALUES_UNEQUAL(response.Buffer.ConvertToString(), data);
}

}
