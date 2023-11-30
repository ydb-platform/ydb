#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

template<typename TDerived>
class TFaultToleranceTestBase : public TActorCoroImpl {
    TSystemEvent *FinishEv;
    std::exception_ptr *Eptr;
    NUnitTest::TTestBase *Test;

protected:
    TFaultToleranceTestRuntime& Runtime;
    const TIntrusivePtr<TBlobStorageGroupInfo>& Info;

    // a set of sets of failed disks that fit the fail model
    TSet<TBlobStorageGroupInfo::TGroupVDisks> FaultsFittingFailModel;

    // a set of sets of failed disks that exceed the fail model
    TSet<TBlobStorageGroupInfo::TGroupVDisks> FaultsExceedingFailModel;

    const ui32 TestPartCount;
    const ui32 TestPartIdx;

public:
    TFaultToleranceTestBase(TSystemEvent *finishEv, std::exception_ptr *eptr, NUnitTest::TTestBase *test, TFaultToleranceTestRuntime& runtime,
            ui32 testPartCount, ui32 testPartIdx)
        : TActorCoroImpl(8 << 20) // 8 MB stack size
        , FinishEv(finishEv)
        , Eptr(eptr)
        , Test(test)
        , Runtime(runtime)
        , Info(Runtime.GroupInfo)
        , TestPartCount(testPartCount)
        , TestPartIdx(testPartIdx)
    {}

    void GenerateFailModel() {
        const ui32 numDisks = Runtime.VDisks.size();
        for (ui64 mask = 0; mask < ((ui64)1 << numDisks); ++mask) {
            TBlobStorageGroupInfo::TGroupVDisks disks = TBlobStorageGroupInfo::TGroupVDisks::CreateFromMask(&Info->GetTopology(), mask);
            bool fits = Info->GetQuorumChecker().CheckFailModelForGroup(disks);
            (fits ? FaultsFittingFailModel : FaultsExceedingFailModel).emplace(std::move(disks));
        }
    }

    void Run() override final {
        GenerateFailModel();
        try {
            static_cast<TDerived *>(this)->RunTestAction();
        } catch (...) {
            *Eptr = std::current_exception();
        }
        FinishEv->Signal();
    }

    void BeforeResume() override final {
        if (!NUnitTest::NPrivate::GetCurrentTest()) {
            NUnitTest::NPrivate::SetUnittestThread(true);
            NUnitTest::NPrivate::SetCurrentTest(Test);
        }
    }

    TAutoPtr<TEvBlobStorage::TEvPutResult> PutWithResult(const TLogoBlobID& id, const TString& buffer,
            TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault) {
        SendToBSProxy(GetActorContext(), Info->GroupID, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max(),
                    NKikimrBlobStorage::TabletLog, tactic));
        auto resp = WaitForSpecificEvent<TEvBlobStorage::TEvPutResult>(&TFaultToleranceTestBase::ProcessUnexpectedEvent);
        CTEST << (TStringBuilder() << "PutResult: " << resp->Get()->ToString() << Endl);
        if (resp->Get()->Status == NKikimrProto::OK && Info->Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3of4) {
            auto layout = GetActualPartLayout(id);
            const ui32 disksWithData = layout.GetDisksWithPart(0) | layout.GetDisksWithPart(1);
            const ui32 disksWithMetadata = layout.GetDisksWithPart(2);
            const ui32 numDisksWithData = PopCount(disksWithData);
            const ui32 numDisksWithAny = PopCount(disksWithData | disksWithMetadata);
            UNIT_ASSERT(numDisksWithAny >= 5);
            auto printLayout = [&] {
                TStringStream s;
                layout.Output(s, Info->Type);
                return s.Str();
            };
            switch (tactic) {
                case TEvBlobStorage::TEvPut::TacticDefault:
                case TEvBlobStorage::TEvPut::TacticMinLatency:
                    UNIT_ASSERT_C(numDisksWithData >= 3 && numDisksWithData <= 4, "numDisksWithData# " << numDisksWithData
                        << " layout# " << printLayout());
                    break;

                case TEvBlobStorage::TEvPut::TacticMaxThroughput:
                    UNIT_ASSERT_VALUES_EQUAL(numDisksWithData, 3);
                    break;

                default:
                    Y_ABORT();
            }
        }
        return resp->Release();
    }

    NKikimrProto::EReplyStatus PutToVDisk(ui32 vdiskOrderNum, const TLogoBlobID& id, const TString& part) {
        Send(Info->GetActorId(vdiskOrderNum), new TEvBlobStorage::TEvVPut(id, TRope(part), Info->GetVDiskId(vdiskOrderNum),
            false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog));
        auto ev = WaitForSpecificEvent<TEvBlobStorage::TEvVPutResult>(&TFaultToleranceTestBase::ProcessUnexpectedEvent);
        return ev->Get()->Record.GetStatus();
    }

    TSubgroupPartLayout GetActualPartLayout(const TLogoBlobID& id) {
        // issue queries
        TBlobStorageGroupInfo::TVDiskIds vdiskIds;
        TBlobStorageGroupInfo::TServiceIds serviceIds;
        Info->PickSubgroup(id.Hash(), &vdiskIds, &serviceIds);
        for (ui32 i = 0; i < Info->Type.BlobSubgroupSize(); ++i) {
            Send(serviceIds[i], TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vdiskIds[i], TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None, Nothing(),
                id, id).release());
        }

        // collect answers
        TSubgroupPartLayout layout;
        for (ui32 i = 0; i < Info->Type.BlobSubgroupSize(); ++i) {
            auto ev = WaitForSpecificEvent<TEvBlobStorage::TEvVGetResult>(&TFaultToleranceTestBase::ProcessUnexpectedEvent);
            const TVDiskID& vdiskId = VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID());

            //google::protobuf::TextFormat::Printer p;
            //p.SetSingleLineMode(true);
            //TString s;
            //p.PrintToString(ev->Get()->Record, &s);
            //CTEST << "  " << s << Endl;

            for (auto& item : ev->Get()->Record.GetResult()) {
                for (ui32 partId : item.GetParts()) {
                    layout.AddItem(Info->GetIdxInSubgroup(vdiskId, id.Hash()), partId - 1, Info->Type);
                }
            }
        }

        return layout;
    }

    void Put(const TLogoBlobID& id, const TString& buffer, NKikimrProto::EReplyStatus expectedStatus = NKikimrProto::OK,
            TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault) {
        UNIT_ASSERT_VALUES_EQUAL(PutWithResult(id, buffer, tactic)->Status, expectedStatus);
    }

    void CheckBlob(const TLogoBlobID& id, bool mustRestoreFirst, NKikimrProto::EReplyStatus status, const TString& buffer) {
        TString data;
        NKikimrProto::EReplyStatus res = GetBlob(id, mustRestoreFirst, buffer ? &data : nullptr);
        UNIT_ASSERT_VALUES_EQUAL(res, status);
        if (res == NKikimrProto::OK && buffer) {
            UNIT_ASSERT_VALUES_EQUAL(data, buffer);
        }
    }

    NKikimrProto::EReplyStatus GetBlob(const TLogoBlobID& id, bool mustRestoreFirst, TString *data, bool isRepl = false) {
        auto query = std::make_unique<TEvBlobStorage::TEvGet>(id, 0U /*shift*/, 0U /*size*/, TInstant::Max(),
            NKikimrBlobStorage::FastRead, mustRestoreFirst, !data);
        query->PhantomCheck = isRepl;
        SendToBSProxy(GetActorContext(), Info->GroupID, query.release());
        auto resp = WaitForSpecificEvent<TEvBlobStorage::TEvGetResult>(&TFaultToleranceTestBase::ProcessUnexpectedEvent);
        TEvBlobStorage::TEvGetResult *msg = resp->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
        const TEvBlobStorage::TEvGetResult::TResponse& item = msg->Responses[0];
        if (data) {
            *data = item.Buffer.ConvertToString();
        }
        return item.Status;
    }

    void Delete(const TLogoBlobID& from, const TLogoBlobID& to, const TBlobStorageGroupInfo::TGroupVDisks& disks, bool wipe) {
        ui32 responsesPending = 0;
        for (const auto& pair : Runtime.VDisks) {
            const TVDiskID& vdiskId = pair.first;
            TBlobStorageGroupInfo::TGroupVDisks curDisk(&Info->GetTopology(), vdiskId);
            if (disks & curDisk) {
                auto func = wipe
                        ? TEvVMockCtlRequest::CreateWipeOutBlobsRequest
                        : TEvVMockCtlRequest::CreateDeleteBlobsRequest;
                GetActorContext().Send(pair.second, func(from, to));
                ++responsesPending;
            }
        }
        while (responsesPending--) {
            auto resp = WaitForSpecificEvent<TEvVMockCtlResponse>(&TFaultToleranceTestBase::ProcessUnexpectedEvent);
            // Cerr << (TStringBuilder() << "]] SpecEventDelete(wipe=" << wipe << "): " << resp->Get()->ToString() << Endl);
        }
    }

    void Delete(const TLogoBlobID& id, const TBlobStorageGroupInfo::TGroupVDisks& disks, bool wipe) {
        Delete(id, TLogoBlobID(id, TLogoBlobID::MaxPartId), disks, wipe);
    }

    void SetFailedDisks(const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) {
        ui32 responsesPending = 0;
        for (const auto& pair : Runtime.VDisks) {
            bool errorMode = static_cast<bool>(failedDisks & TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), pair.first));
            GetActorContext().Send(pair.second, TEvVMockCtlRequest::CreateErrorModeRequest(errorMode));
            ++responsesPending;
        }
        while (responsesPending--) {
            WaitForSpecificEvent<TEvVMockCtlResponse>(&TFaultToleranceTestBase::ProcessUnexpectedEvent);
        }
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
        Y_ABORT("unexpected event received: Type# %08" PRIx32, ev->GetTypeRewrite());
    }
};

} // NKikimr
