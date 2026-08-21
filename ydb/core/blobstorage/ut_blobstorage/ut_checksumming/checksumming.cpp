#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/erasure/erasure.h>

#include <cstring>

namespace {

TString GenData(ui32 size, ui64 seed = 0) {
    return TEnvironmentSetup::GenerateRandomString(size, seed);
}

struct TTestEnv {
    TTestEnv(TErasureType::ECrcMode crcMode,
            TBlobStorageGroupType erasure = TBlobStorageGroupType::ErasureNone)
        : Env(TEnvironmentSetup::TSettings{
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
        })
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

    void PrepareBlobId(ui32 size) {
        static ui32 step = 0;
        LastBlobId = TLogoBlobID::Make(123, 1, ++step, 0, size, 0, CrcMode);
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> WritePrepared(const TString& data,
            TInstant waitDeadline = TInstant::Max()) {
        UNIT_ASSERT_VALUES_EQUAL(LastBlobId.BlobSize(), data.size());
        SendToDsProxy(new TEvBlobStorage::TEvPut(LastBlobId, TRcBuf(data), TInstant::Max()));
        return Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Sender, false, waitDeadline);
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvPutResult>> Write(const TString& data,
            TInstant waitDeadline = TInstant::Max()) {
        PrepareBlobId(data.size());
        return WritePrepared(data, waitDeadline);
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvGetResult>> ReadFromDsProxy(ui32 shift = 0, ui32 size = 0,
            TInstant waitDeadline = TInstant::Max(), bool mustRestoreFirst = false) {
        SendToDsProxy(new TEvBlobStorage::TEvGet(LastBlobId, shift, size, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead, mustRestoreFirst));
        return Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(Sender, false, waitDeadline);
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
    char* checksum = data.Detach() + data.size() - sizeof(ui64);
    std::memset(checksum, 0, sizeof(ui64));
    if (CheckCrcAtTheEnd(TErasureType::CrcModeWholePart, TRope(data))) {
        std::memset(checksum, 0xff, sizeof(ui64));
    }
    UNIT_ASSERT(!CheckCrcAtTheEnd(TErasureType::CrcModeWholePart, TRope(data)));
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

struct TPartErrorInjection {
    const TLogoBlobID BlobId;
    const ui32 BrokenPartCount;
    THashMap<TVDiskID, ui32> CandidatePartsByDisk;
    THashSet<ui32> BrokenParts;

    bool IsTargetBlob(const TLogoBlobID& partId) const {
        return partId.FullID() == BlobId;
    }

    bool ShouldBreakPart(ui32 partId) {
        if (BrokenParts.contains(partId)) {
            return true;
        } else if (BrokenParts.size() < BrokenPartCount) {
            BrokenParts.insert(partId);
            return true;
        }
        return false;
    }
};

std::shared_ptr<TPartErrorInjection> MakePartErrorInjection(TTestEnv& env, ui32 brokenPartCount,
        ui32 candidatePartCount) {
    auto injection = std::make_shared<TPartErrorInjection>(TPartErrorInjection{
        .BlobId = env.LastBlobId,
        .BrokenPartCount = brokenPartCount,
    });

    UNIT_ASSERT(brokenPartCount <= candidatePartCount);
    UNIT_ASSERT(candidatePartCount <= env.GroupInfo->Type.TotalPartCount());
    // Subgroup indexes 0..TotalPartCount-1 are the primary locations of parts 1..N.
    for (ui32 partId = 1; partId <= candidatePartCount; ++partId) {
        const TVDiskID vdiskId = env.GroupInfo->GetVDiskInSubgroup(partId - 1, env.LastBlobId.Hash());
        UNIT_ASSERT(injection->CandidatePartsByDisk.emplace(vdiskId, partId).second);
    }
    return injection;
}

std::shared_ptr<TPartErrorInjection> SetVPutErrorFilter(TTestEnv& env, ui32 brokenPartCount) {
    auto injection = MakePartErrorInjection(env, brokenPartCount, brokenPartCount);
    auto* runtime = env.Env.Runtime.get();
    env.Env.Runtime->FilterFunction = [injection, runtime](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
            auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
            const TVDiskID vdiskId = VDiskIDFromVDiskID(vput->Record.GetVDiskID());
            const TLogoBlobID partId = LogoBlobIDFromLogoBlobID(vput->Record.GetBlobID());
            const auto candidatePart = injection->CandidatePartsByDisk.find(vdiskId);
            if (candidatePart != injection->CandidatePartsByDisk.end() && injection->IsTargetBlob(partId) &&
                    partId.PartId() == candidatePart->second && injection->ShouldBreakPart(partId.PartId())) {
                auto result = std::make_unique<TEvBlobStorage::TEvVPutResult>();
                result->MakeError(NKikimrProto::ERROR, "injected VPut error", vput->Record);
                runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie),
                    ev->Sender.NodeId());
                return false;
            }
        }
        return true;
    };
    return injection;
}

std::shared_ptr<TPartErrorInjection> SetVGetErrorFilter(TTestEnv& env, ui32 brokenPartCount) {
    auto injection = MakePartErrorInjection(env, brokenPartCount, brokenPartCount);
    const ui32 dsProxyNodeId = env.Sender.NodeId();
    env.Env.Runtime->FilterFunction = [injection, dsProxyNodeId](ui32 nodeId,
            std::unique_ptr<IEventHandle>& ev) {
        if (nodeId == dsProxyNodeId && ev->GetTypeRewrite() == TEvBlobStorage::EvVGetResult) {
            auto* vget = ev->Get<TEvBlobStorage::TEvVGetResult>();
            const TVDiskID vdiskId = VDiskIDFromVDiskID(vget->Record.GetVDiskID());
            const auto candidatePart = injection->CandidatePartsByDisk.find(vdiskId);
            if (candidatePart == injection->CandidatePartsByDisk.end()) {
                return true;
            }

            for (auto& result : *vget->Record.MutableResult()) {
                const TLogoBlobID partId = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                if (injection->IsTargetBlob(partId) && partId.PartId() == candidatePart->second &&
                        result.GetStatus() == NKikimrProto::OK && injection->ShouldBreakPart(partId.PartId())) {
                    CorruptVGetResultChecksum(*vget, result);
                }
            }
        }
        return true;
    };
    return injection;
}

void TestDsProxyPutWithErrors(TBlobStorageGroupType erasure, ui32 brokenPartCount,
        NKikimrProto::EReplyStatus expectedStatus) {
    TTestEnv env(TErasureType::CrcModeWholePart, erasure);
    const TString data = GenData(16_KB, brokenPartCount);
    env.PrepareBlobId(data.size());
    const auto injection = SetVPutErrorFilter(env, brokenPartCount);

    const auto result = env.WritePrepared(data, env.Env.Runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT_C(result, "DSProxy did not finish VPut after injected errors");
    UNIT_ASSERT_VALUES_EQUAL(injection->BrokenParts.size(), brokenPartCount);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, expectedStatus, result->Get()->ToString());
}

void TestDsProxyGetWithErrors(TBlobStorageGroupType erasure, ui32 brokenPartCount,
        NKikimrProto::EReplyStatus expectedStatus) {
    TTestEnv env(TErasureType::CrcModeWholePart, erasure);
    const TString data = GenData(16_KB, brokenPartCount);
    const auto putResult = env.Write(data, env.Env.Runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT_C(putResult, "DSProxy did not finish the setup VPut");
    UNIT_ASSERT_VALUES_EQUAL(putResult->Get()->Status, NKikimrProto::OK);

    const auto injection = SetVGetErrorFilter(env, brokenPartCount);
    const auto result = env.ReadFromDsProxy(0, 0, env.Env.Runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT_C(result, "DSProxy did not finish VGet after injected errors");
    UNIT_ASSERT_VALUES_EQUAL(injection->BrokenParts.size(), brokenPartCount);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, expectedStatus, result->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, expectedStatus);
    if (expectedStatus == NKikimrProto::OK) {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
}

struct TMirror3dcErrorInjection {
    const TLogoBlobID BlobId;
    THashMap<TVDiskID, ui32> PartsByDisk;
    THashSet<TVDiskID> UnavailableDisks;
    THashSet<TVDiskID> FailedDisks;
    TVector<std::pair<ui32, std::unique_ptr<IEventHandle>>> DelayedVGetResults;
    bool DeliverHealthyVGetResults = false;

    bool IsTargetBlob(const TLogoBlobID& partId) const {
        return partId.FullID() == BlobId;
    }
};

std::shared_ptr<TMirror3dcErrorInjection> MakeMirror3dcErrorInjection(TTestEnv& env,
        ui32 additionalErrorsInSecondDc) {
    UNIT_ASSERT_VALUES_EQUAL(env.GroupInfo->Type.GetErasure(), TBlobStorageGroupType::ErasureMirror3dc);
    UNIT_ASSERT(additionalErrorsInSecondDc <= 2);

    auto injection = std::make_shared<TMirror3dcErrorInjection>(TMirror3dcErrorInjection{
        .BlobId = env.LastBlobId,
    });
    auto addDisk = [&](ui32 subgroupIdx, bool unavailable) {
        const TVDiskID vdiskId = env.GroupInfo->GetVDiskInSubgroup(subgroupIdx, env.LastBlobId.Hash());
        const ui32 partId = subgroupIdx % 3 + 1;
        UNIT_ASSERT(injection->PartsByDisk.emplace(vdiskId, partId).second);
        if (unavailable) {
            UNIT_ASSERT(injection->UnavailableDisks.insert(vdiskId).second);
        }
    };

    // Mirror3dc subgroup is a 3x3 matrix: rows are fail domains, columns are DCs.
    // The whole first DC is unavailable; one or two replicas fail in the second DC.
    for (ui32 failDomain = 0; failDomain < 3; ++failDomain) {
        addDisk(failDomain * 3, true);
    }
    for (ui32 failDomain = 0; failDomain < additionalErrorsInSecondDc; ++failDomain) {
        addDisk(failDomain * 3 + 1, false);
    }
    return injection;
}

std::shared_ptr<TMirror3dcErrorInjection> SetMirror3dcVPutErrorFilter(TTestEnv& env,
        ui32 additionalErrorsInSecondDc) {
    auto injection = MakeMirror3dcErrorInjection(env, additionalErrorsInSecondDc);
    auto* runtime = env.Env.Runtime.get();
    env.Env.Runtime->FilterFunction = [injection, runtime](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
            auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
            const TVDiskID vdiskId = VDiskIDFromVDiskID(vput->Record.GetVDiskID());
            const TLogoBlobID partId = LogoBlobIDFromLogoBlobID(vput->Record.GetBlobID());
            const auto target = injection->PartsByDisk.find(vdiskId);
            if (target != injection->PartsByDisk.end() && injection->IsTargetBlob(partId) &&
                    partId.PartId() == target->second) {
                injection->FailedDisks.insert(vdiskId);
                auto result = std::make_unique<TEvBlobStorage::TEvVPutResult>();
                result->MakeError(NKikimrProto::ERROR, "injected Mirror3dc VPut error", vput->Record);
                runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie),
                    ev->Sender.NodeId());
                return false;
            }
        }
        return true;
    };
    return injection;
}

void PutMirror3dcBlobOnAllDisks(TTestEnv& env, const TString& data) {
    env.PrepareBlobId(data.size());

    TString encryptedData = data;
    char* buffer = encryptedData.Detach();
    Encrypt(buffer, buffer, 0, encryptedData.size(), env.LastBlobId, *env.GroupInfo);

    TDataPartSet partSet;
    env.GroupInfo->Type.SplitData(env.CrcMode, encryptedData, partSet);
    UNIT_ASSERT_VALUES_EQUAL(partSet.Parts.size(), 3);

    for (ui32 subgroupIdx = 0; subgroupIdx < 9; ++subgroupIdx) {
        const ui32 partIdx = subgroupIdx % 3;
        const TVDiskID vdiskId = env.GroupInfo->GetVDiskInSubgroup(subgroupIdx, env.LastBlobId.Hash());
        env.Env.PutBlob(vdiskId, TLogoBlobID(env.LastBlobId, partIdx + 1),
            partSet.Parts[partIdx].OwnedString.ConvertToString());
    }
}

std::shared_ptr<TMirror3dcErrorInjection> SetMirror3dcVGetErrorFilter(TTestEnv& env,
        ui32 additionalErrorsInSecondDc) {
    auto injection = MakeMirror3dcErrorInjection(env, additionalErrorsInSecondDc);
    auto* runtime = env.Env.Runtime.get();
    const ui32 dsProxyNodeId = env.Sender.NodeId();
    env.Env.Runtime->FilterFunction = [injection, runtime, dsProxyNodeId](ui32 nodeId,
            std::unique_ptr<IEventHandle>& ev) {
        if (nodeId != dsProxyNodeId || ev->GetTypeRewrite() != TEvBlobStorage::EvVGetResult) {
            return true;
        }

        auto* vget = ev->Get<TEvBlobStorage::TEvVGetResult>();
        const TVDiskID vdiskId = VDiskIDFromVDiskID(vget->Record.GetVDiskID());
        bool containsTargetBlob = false;
        for (const auto& result : vget->Record.GetResult()) {
            containsTargetBlob |= injection->IsTargetBlob(LogoBlobIDFromLogoBlobID(result.GetBlobID()));
        }
        if (!containsTargetBlob) {
            return true;
        }

        const auto target = injection->PartsByDisk.find(vdiskId);
        if (target == injection->PartsByDisk.end()) {
            if (!injection->DeliverHealthyVGetResults) {
                injection->DelayedVGetResults.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return true;
        }

        // A local event may pass through the runtime filter more than once.
        bool failed = injection->FailedDisks.contains(vdiskId);
        for (auto& result : *vget->Record.MutableResult()) {
            const TLogoBlobID partId = LogoBlobIDFromLogoBlobID(result.GetBlobID());
            if (injection->IsTargetBlob(partId) && partId.PartId() == target->second &&
                    result.GetStatus() == NKikimrProto::OK) {
                if (injection->UnavailableDisks.contains(vdiskId)) {
                    result.SetStatus(NKikimrProto::ERROR);
                    result.ClearBufferData();
                    result.ClearPayloadId();
                } else {
                    CorruptVGetResultChecksum(*vget, result);
                }
                failed = true;
            }
        }
        UNIT_ASSERT_C(failed, "target Mirror3dc VDisk did not return the expected blob part: " << vget->ToString());
        injection->FailedDisks.insert(vdiskId);

        if (injection->FailedDisks.size() == injection->PartsByDisk.size()) {
            injection->DeliverHealthyVGetResults = true;
            TVector<std::pair<ui32, std::unique_ptr<IEventHandle>>> delayed;
            delayed.swap(injection->DelayedVGetResults);
            for (auto& [delayedNodeId, delayedEvent] : delayed) {
                runtime->Send(delayedEvent.release(), delayedNodeId);
            }
        }
        return true;
    };
    return injection;
}

void TestDsProxyMirror3dcPutWithErrors(ui32 additionalErrorsInSecondDc,
        NKikimrProto::EReplyStatus expectedStatus) {
    TTestEnv env(TErasureType::CrcModeWholePart, TBlobStorageGroupType::ErasureMirror3dc);
    const TString data = GenData(16_KB, 100 + additionalErrorsInSecondDc);
    env.PrepareBlobId(data.size());
    const auto injection = SetMirror3dcVPutErrorFilter(env, additionalErrorsInSecondDc);

    const auto result = env.WritePrepared(data, env.Env.Runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT_C(result, "DSProxy did not finish Mirror3dc VPut after injected errors");
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, expectedStatus, result->Get()->ToString());
    // In the 3+2 case DSProxy may stop after observing 2+2: the fail model is already exceeded,
    // so it does not have to probe the last failed disk of the fully unavailable DC.
    if (additionalErrorsInSecondDc == 2) {
        UNIT_ASSERT_VALUES_EQUAL(injection->PartsByDisk.size(), 5);
        UNIT_ASSERT_C(4 <= injection->FailedDisks.size() && injection->FailedDisks.size() <= 5,
            "DSProxy must observe at least a 2+2 failure before returning ERROR");
    } else {
        UNIT_ASSERT_VALUES_EQUAL(injection->FailedDisks.size(), injection->PartsByDisk.size());
    }
}

void TestDsProxyMirror3dcGetWithErrors(ui32 additionalErrorsInSecondDc,
        NKikimrProto::EReplyStatus expectedStatus) {
    TTestEnv env(TErasureType::CrcModeWholePart, TBlobStorageGroupType::ErasureMirror3dc);
    const TString data = GenData(16_KB, 200 + additionalErrorsInSecondDc);
    PutMirror3dcBlobOnAllDisks(env, data);
    const auto injection = SetMirror3dcVGetErrorFilter(env, additionalErrorsInSecondDc);

    // A regular Mirror3dc FastRead may finish on the first healthy replica without observing the configured
    // failure pattern. Restore-first reads all nine replicas and therefore exercises the group fail model.
    const auto result = env.ReadFromDsProxy(0, 0, env.Env.Runtime->GetClock() + TDuration::Seconds(30),
        true /* mustRestoreFirst */);
    UNIT_ASSERT_C(result, "DSProxy did not finish Mirror3dc VGet after injected errors");
    UNIT_ASSERT_VALUES_EQUAL(injection->FailedDisks.size(), injection->PartsByDisk.size());
    UNIT_ASSERT(injection->DelayedVGetResults.empty());
    UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, expectedStatus, result->Get()->ToString());
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, expectedStatus);
    if (expectedStatus == NKikimrProto::OK) {
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
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

Y_UNIT_TEST(DsProxyBlock42WritesWithOneError) {
    TestDsProxyPutWithErrors(TBlobStorageGroupType::Erasure4Plus2Block, 1, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyBlock42WritesWithTwoErrors) {
    TestDsProxyPutWithErrors(TBlobStorageGroupType::Erasure4Plus2Block, 2, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyBlock42WritesWithThreeErrors) {
    TestDsProxyPutWithErrors(TBlobStorageGroupType::Erasure4Plus2Block, 3, NKikimrProto::ERROR);
}

Y_UNIT_TEST(DsProxyBlock42ReadsWithOneError) {
    TestDsProxyGetWithErrors(TBlobStorageGroupType::Erasure4Plus2Block, 1, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyBlock42ReadsWithTwoErrors) {
    TestDsProxyGetWithErrors(TBlobStorageGroupType::Erasure4Plus2Block, 2, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyBlock42ReadsWithThreeErrors) {
    TestDsProxyGetWithErrors(TBlobStorageGroupType::Erasure4Plus2Block, 3, NKikimrProto::ERROR);
}

Y_UNIT_TEST(DsProxyMirror3dcWritesWithOneDcUnavailable) {
    TestDsProxyMirror3dcPutWithErrors(0, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyMirror3dcWritesWithOneDcUnavailableAndOneAdditionalReplicaError) {
    TestDsProxyMirror3dcPutWithErrors(1, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyMirror3dcWritesWithOneDcUnavailableAndTwoAdditionalReplicaErrors) {
    TestDsProxyMirror3dcPutWithErrors(2, NKikimrProto::ERROR);
}

Y_UNIT_TEST(DsProxyMirror3dcReadsWithOneDcUnavailable) {
    TestDsProxyMirror3dcGetWithErrors(0, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyMirror3dcReadsWithOneDcUnavailableAndOneAdditionalReplicaError) {
    TestDsProxyMirror3dcGetWithErrors(1, NKikimrProto::OK);
}

Y_UNIT_TEST(DsProxyMirror3dcReadsWithOneDcUnavailableAndTwoAdditionalReplicaErrors) {
    TestDsProxyMirror3dcGetWithErrors(2, NKikimrProto::ERROR);
}

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
