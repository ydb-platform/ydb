#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/erasure/erasure.h>

namespace {

TString GenData(ui32 size, ui64 seed = 0) {
    return TEnvironmentSetup::GenerateRandomString(size, seed);
}

struct TTestEnv {
    TTestEnv(TErasureType::ECrcMode crcMode)
        : Env({})
        , CrcMode(crcMode)
    {
        Env.CreateBoxAndPool(1, 1);
        const auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());
        VDiskActorId = GroupInfo->GetActorId(0);
        Sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        Env.Sim(TDuration::Minutes(1));
    }

    template<class TEvent>
    void SendToDsProxy(TEvent* event) {
        Env.Runtime->WrapInActorContext(Sender, [&] {
            SendToBSProxy(Sender, GroupInfo->GroupID, event);
        });
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> Write(const TString& data) {
        static ui32 step = 0;
        LastBlobId = TLogoBlobID::Make(123, 1, ++step, 0, data.size(), 0, CrcMode);
        SendToDsProxy(new TEvBlobStorage::TEvPut(LastBlobId, TRcBuf(data), TInstant::Max()));
        return Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Sender, false);
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> ReadFromDsProxy(ui32 shift = 0, ui32 size = 0) {
        SendToDsProxy(new TEvBlobStorage::TEvGet(LastBlobId, shift, size, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead));
        return Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(Sender, false);
    }

    void EnableVDiskChecksumValidation(bool read, bool write) {
        Env.SetIcbControl(0, "VDiskControls.EnableChecksumReadValidationOnVDisk", read);
        Env.SetIcbControl(0, "VDiskControls.EnableChecksumWriteValidationOnVDisk", write);
    }

    NKikimrProto::EReplyStatus ReadPart(ui32 shift = 0, ui32 size = 0, TString* partData = nullptr) {
        NKikimrProto::EReplyStatus status = NKikimrProto::ERROR;
        TBlobStorageGroupInfo::TVDiskIds vdiskIds;
        GroupInfo->PickSubgroup(LastBlobId.Hash(), &vdiskIds, nullptr);
        UNIT_ASSERT(!vdiskIds.empty());
        const TVDiskID& vdiskId = vdiskIds[0];

        Env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            auto request = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                Nothing(), {TEvBlobStorage::TEvVGet::TExtremeQuery(LastBlobId, shift, size)});
            Env.Runtime->Send(new IEventHandle(queueId, edge, request.release()), queueId.NodeId());
            auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrProto::OK, response->Get()->ToString());
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.ResultSize(), 1);
            const auto& result = response->Get()->Record.GetResult(0);
            status = result.GetStatus();
            if (partData && status == NKikimrProto::OK) {
                *partData = response->Get()->GetBlobData(result).ConvertToString();
            }
        });
        return status;
    }

    void SendPartRead() {
        TBlobStorageGroupInfo::TVDiskIds vdiskIds;
        GroupInfo->PickSubgroup(LastBlobId.Hash(), &vdiskIds, nullptr);
        UNIT_ASSERT(!vdiskIds.empty());
        const TVDiskID& vdiskId = vdiskIds[0];

        Env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, [&](TActorId queueId) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            auto request = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
                Nothing(), {TEvBlobStorage::TEvVGet::TExtremeQuery(LastBlobId, 0, 0)});
            Env.Runtime->Send(new IEventHandle(queueId, edge, request.release()), queueId.NodeId());
        });
    }

    TString DecryptPart(const TString& partData) const {
        UNIT_ASSERT_VALUES_EQUAL(GroupInfo->Type.TotalPartCount(), 1);
        TString result = partData;
        char* buffer = result.Detach();
        Decrypt(buffer, buffer, 0, result.size(), LastBlobId, *GroupInfo);
        return result;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId VDiskActorId;
    TActorId Sender;
    TLogoBlobID LastBlobId;
    const TErasureType::ECrcMode CrcMode;
};

bool IsVPutToVDisk(const TTestEnv& env, const std::unique_ptr<IEventHandle>& ev) {
    return ev->GetTypeRewrite() == TEvBlobStorage::EvVPut && ev->Recipient == env.VDiskActorId;
}

void CorruptVPutPayload(TEvBlobStorage::TEvVPut& vput) {
    TString data = vput.GetBuffer().ConvertToString();
    UNIT_ASSERT(!data.empty());
    const size_t pos = data.size() / 2;
    data[pos] = data[pos] ^ 1;
    vput.StripPayload();
    vput.AddPayload(TRope(data));
}

void CorruptVGetResultPayload(TEvBlobStorage::TEvVGetResult& vgetResult,
        NKikimrBlobStorage::TQueryResult& result) {
    UNIT_ASSERT(vgetResult.HasBlob(result));
    TString data = vgetResult.GetBlobData(result).ConvertToString();
    UNIT_ASSERT(!data.empty());
    const size_t pos = data.size() / 2;
    data[pos] = data[pos] ^ 1;
    result.ClearBufferData();
    result.ClearPayloadId();
    vgetResult.SetBlobData(result, TRope(data));
}

void CorruptVGetResultChecksum(TEvBlobStorage::TEvVGetResult& vgetResult,
        NKikimrBlobStorage::TQueryResult& result) {
    UNIT_ASSERT(vgetResult.HasBlob(result));
    TString data = vgetResult.GetBlobData(result).ConvertToString();
    UNIT_ASSERT(data.size() > sizeof(ui64));
    const size_t checksumByte = data.size() - 1;
    data[checksumByte] = data[checksumByte] ^ 1;
    result.ClearBufferData();
    result.ClearPayloadId();
    vgetResult.SetBlobData(result, TRope(data));
}

void AssertWholePart(const TTestEnv& env, const TString& encryptedPart, const TString& data) {
    const TString decryptedPart = env.DecryptPart(encryptedPart);
    UNIT_ASSERT(CheckCrcAtTheEnd(TErasureType::CrcModeWholePart, TRope(decryptedPart)));
    UNIT_ASSERT_VALUES_EQUAL(decryptedPart.substr(0, data.size()), data);
}

void SetCorruptingVPutFilter(TTestEnv& env, bool& corrupted) {
    env.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (IsVPutToVDisk(env, ev) && !corrupted) {
            CorruptVPutPayload(*ev->Get<TEvBlobStorage::TEvVPut>());
            corrupted = true;
        }
        return true;
    };
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(UserChecksumming) {

Y_UNIT_TEST(PutEightBytesWithWholePartChecksum) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    const TString data = "abcdefgh";
    UNIT_ASSERT_VALUES_EQUAL(env.Write(data)->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(env.LastBlobId.CrcMode()),
        static_cast<ui32>(TErasureType::CrcModeWholePart));
    TString part;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadPart(0, 0, &part), NKikimrProto::OK);
    AssertWholePart(env, part, data);
}

Y_UNIT_TEST(PutEightBytesWithoutChecksum) {
    TTestEnv env(TErasureType::CrcModeNone);
    const TString data = "abcdefgh";
    UNIT_ASSERT_VALUES_EQUAL(env.Write(data)->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(env.LastBlobId.CrcMode()),
        static_cast<ui32>(TErasureType::CrcModeNone));
    TString part;
    UNIT_ASSERT_VALUES_EQUAL(env.ReadPart(0, 0, &part), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(env.DecryptPart(part), data);
}

// ------------------ VDisk Writes ------------------

Y_UNIT_TEST(VDiskAcceptsValidWholePartChecksumWhenWriteValidationEnabled) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    env.EnableVDiskChecksumValidation(false, true);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(GenData(16_KB, 1))->Get()->Status, NKikimrProto::OK);
}

Y_UNIT_TEST(VDiskRejectsCorruptedWholePartChecksumWhenWriteValidationEnabled) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    env.EnableVDiskChecksumValidation(false, true);
    bool corrupted = false;
    SetCorruptingVPutFilter(env, corrupted);
    const auto result = env.Write(GenData(16_KB, 2));
    UNIT_ASSERT(corrupted);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
    UNIT_ASSERT_STRING_CONTAINS(result->Get()->ErrorReason, "buffer checksum mismatch");
}

Y_UNIT_TEST(VDiskAcceptsCorruptedWholePartChecksumByDefault) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    bool corrupted = false;
    SetCorruptingVPutFilter(env, corrupted);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(GenData(16_KB, 3))->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(corrupted);
}

Y_UNIT_TEST(VDiskDoesNotValidateCrcModeNoneOnWrite) {
    TTestEnv env(TErasureType::CrcModeNone);
    env.EnableVDiskChecksumValidation(false, true);
    bool corrupted = false;
    SetCorruptingVPutFilter(env, corrupted);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(GenData(16_KB, 4))->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(corrupted);
}

Y_UNIT_TEST(VDiskAcceptsVMultiPutWithCrcModeNoneWhenWriteValidationEnabled) {
    TTestEnv env(TErasureType::CrcModeNone);
    env.EnableVDiskChecksumValidation(false, true);
    const TString data = GenData(64, 9);
    const TLogoBlobID fullId = TLogoBlobID::Make(123, 1, 1, 0, data.size(), 0, TErasureType::CrcModeNone);
    const TLogoBlobID partId(fullId, 1);

    auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(env.GroupInfo->GetVDiskId(0),
        TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
    multiPut->AddVPut(partId, TRcBuf(data), nullptr, nullptr, NWilson::TTraceId());

    env.Env.WithQueueId(env.GroupInfo->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog,
            [&](TActorId queueId) {
        const TActorId edge = env.Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
        env.Env.Runtime->Send(new IEventHandle(queueId, edge, multiPut.release()), queueId.NodeId());
        const auto result = env.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVMultiPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetItems(0).GetStatus(), NKikimrProto::OK);
    });
}

// ------------------ VDisk Reads ------------------

Y_UNIT_TEST(VDiskRejectsCorruptedWholePartChecksumWhenReadValidationEnabled) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    bool corrupted = false;
    SetCorruptingVPutFilter(env, corrupted);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(GenData(16_KB, 5))->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(corrupted);
    env.EnableVDiskChecksumValidation(true, false);
    bool restoreRequested = false;
    env.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvRestoreCorruptedBlob) {
            restoreRequested = true;
        }
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult) {
            return false;
        }
        return true;
    };
    env.SendPartRead();
    env.Env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(restoreRequested);
}

Y_UNIT_TEST(VDiskAcceptsCorruptedWholePartChecksumByDefaultOnRead) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    bool corrupted = false;
    SetCorruptingVPutFilter(env, corrupted);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(GenData(16_KB, 6))->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(corrupted);
    UNIT_ASSERT_VALUES_EQUAL(env.ReadPart(), NKikimrProto::OK);
}

Y_UNIT_TEST(VDiskDoesNotValidateCrcModeNoneOnRead) {
    TTestEnv env(TErasureType::CrcModeNone);
    bool corrupted = false;
    SetCorruptingVPutFilter(env, corrupted);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(GenData(16_KB, 7))->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(corrupted);
    env.EnableVDiskChecksumValidation(true, false);
    UNIT_ASSERT_VALUES_EQUAL(env.ReadPart(), NKikimrProto::OK);
}

// ------------------ Substring Reads ------------------

Y_UNIT_TEST(VDiskDoesNotValidatePartialWholePartRead) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    bool corrupted = false;
    SetCorruptingVPutFilter(env, corrupted);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(GenData(16_KB, 8))->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT(corrupted);
    env.EnableVDiskChecksumValidation(true, false);
    UNIT_ASSERT_VALUES_EQUAL(env.ReadPart(10, 32), NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyReadsSubstringOfWholePartChecksumBlob) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    const TString data = GenData(256, 10);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(data)->Get()->Status, NKikimrProto::OK);
    const auto result = env.ReadFromDsProxy(23, 41);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data.substr(23, 41));
}

// ------------------ DsProxy ------------------

Y_UNIT_TEST(DsProxyReadsWholePartChecksumBlob) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    const TString data = GenData(16_KB, 9);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(data)->Get()->Status, NKikimrProto::OK);
    const auto result = env.ReadFromDsProxy();
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
}

Y_UNIT_TEST(DsProxyRejectsInvalidVGetChecksum) {
    TTestEnv env(TErasureType::CrcModeWholePart);
    const TString data = GenData(16_KB, 10);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(data)->Get()->Status, NKikimrProto::OK);

    bool corrupted = false;
    env.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult && !corrupted) {
            auto* vget = ev->Get<TEvBlobStorage::TEvVGetResult>();
            auto* queryResult = vget->Record.MutableResult(0);
            UNIT_ASSERT_VALUES_EQUAL(queryResult->GetStatus(), NKikimrProto::OK);
            const TLogoBlobID partId = LogoBlobIDFromLogoBlobID(queryResult->GetBlobID());
            UNIT_ASSERT_VALUES_EQUAL(vget->GetBlobData(*queryResult).size(), env.GroupInfo->Type.PartSize(partId));
            CorruptVGetResultChecksum(*vget, *queryResult);
            corrupted = true;
        }
        return true;
    };

    const auto result = env.ReadFromDsProxy();
    UNIT_ASSERT(corrupted);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::ERROR, result->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::ERROR);
}

Y_UNIT_TEST(DsProxyAcceptsCorruptedFullPartVGetWithCrcModeNone) {
    TTestEnv env(TErasureType::CrcModeNone);
    const TString data = GenData(16_KB, 11);
    UNIT_ASSERT_VALUES_EQUAL(env.Write(data)->Get()->Status, NKikimrProto::OK);

    bool corrupted = false;
    env.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult && !corrupted) {
            auto* vget = ev->Get<TEvBlobStorage::TEvVGetResult>();
            auto* queryResult = vget->Record.MutableResult(0);
            UNIT_ASSERT_VALUES_EQUAL(queryResult->GetStatus(), NKikimrProto::OK);
            CorruptVGetResultPayload(*vget, *queryResult);
            corrupted = true;
        }
        return true;
    };

    const auto result = env.ReadFromDsProxy();
    UNIT_ASSERT(corrupted);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
    UNIT_ASSERT_VALUES_UNEQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
}

}
