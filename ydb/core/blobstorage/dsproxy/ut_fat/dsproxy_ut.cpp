#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_nodemon.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>
#include <ydb/core/blobstorage/vdisk/vdisk_services.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/tablet.h>

#include <ydb/core/mon_alloc/profiler.h>

#include <ydb/core/protos/blobstorage_pdisk_config.pb.h>
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/interconnect/poller_actor.h>
#include <ydb/library/actors/interconnect/mock/ic_mock.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/util/affinity.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/random/mersenne.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>
#include <util/string/escape.h>
#include <util/string/printf.h>
#include <util/system/backtrace.h>
#include <util/system/defaults.h>
#include <util/system/event.h>
#include <util/system/sanitizers.h>

#define RT_EXECUTOR_POOL 0


namespace NKikimr {

#define TEST_RESPONSE_3(msg, st, sz) \
do { \
    UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::msg, "Unexpected message " << (int)LastResponse.Message); \
    UNIT_ASSERT_EQUAL_C(LastResponse.Status, NKikimrProto::st, "Unexpected status " \
            << StatusToString(LastResponse.Status) << " deadVDisks " << Env->DeadVDisksMask); \
    UNIT_ASSERT_VALUES_EQUAL_C(LastResponse.Data.size(), sz, "Data size " \
        << (int)LastResponse.Data.size() << " while expected " << (sz)); \
} while(false)

#define TEST_RESPONSE(msg, st, sz, d) \
do { \
    TEST_RESPONSE_3(msg, st, sz); \
    UNIT_ASSERT_C(!((sz) > 0 && TString(LastResponse.Data[0]) != TString(d)), "Got '" \
        << EscapeC(TString(LastResponse.Data[0])).c_str() << "' instead of '" << EscapeC(TString(d)).c_str() << "'"); \
} while(false)


#define TEST_RESPONSE_FULLCHECK(msg, st, sz, d) \
do { \
    TEST_RESPONSE_3(msg, st, sz); \
    for (size_t i = 0; i < sz; ++i) { \
        UNIT_ASSERT_C(TString(LastResponse.Data[i]) == TString(d[i]), "Got " << i << "'th element '" \
            << EscapeC(TString(LastResponse.Data[i])).c_str() << "' instead of '" << EscapeC(TString(d[i])).c_str() << "' at test step " << TestStep); \
    } \
} while (false)

#define VERBOSE_COUT(str) \
do { \
    if (IsVerbose) { \
        bool isOk = false; \
        do { \
            try { \
                Cerr << str << Endl; \
                isOk = true; \
            } catch (TSystemError err) { \
                Y_UNUSED(err); \
            } \
        } while (!isOk); \
    } \
} while(false)


constexpr ui32 TEST_TIMEOUT = NSan::PlainOrUnderSanitizer(1800000, 2400000);
static bool IsVerbose = false;
static bool IsProfilerEnabled = true;


TString TestData2(
        "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");

static TString StatusToString(const NKikimrProto::EReplyStatus status) {
    return NKikimrProto::EReplyStatus_Name(status);
}


struct ITestParametrs : public TThrRefBase {
    virtual ~ITestParametrs()
    {
    }
};

struct TTestArgs {
    ui64 BadVDiskMask = 0;
    TBlobStorageGroupType::EErasureSpecies ErasureSpecies;
    ui32 SleepMilliseconds = 0;
    bool StartBadDisks = true;
    ui32 FailDomainCount = 0;
    ui32 DrivesPerFailDomain = 0;
    TIntrusivePtr<ITestParametrs> Parametrs = nullptr;
    bool EnablePutBatching = DefaultEnablePutBatching;
    NPDisk::EDeviceType DeviceType = NPDisk::DEVICE_TYPE_ROT;
};

struct TTestEnvironment : public TThrRefBase {
    ui32 VDiskCount;
    ui64 DeadVDisksMask;
    TVector<TActorId> VDisks;
    TVector<TActorId> PDisks;
    TVector<TVDiskID> VDiskIds;

    bool ShouldBeUnwritable;
    bool ShouldBeUndiscoverable;

    ui64 FailDomainCount;
    ui64 DrivesPerFailDomain;
    ui32 FailedDiskCount;
    TBlobStorageGroupType GroupType;

    TActorId ProxyId;
    TActorId ProxyTestId;

    TSystemEvent DoneEvent;
    yexception LastException;
    volatile bool IsLastExceptionSet = false;


    TTestEnvironment(const TTestArgs &args)
        : DeadVDisksMask(args.StartBadDisks ? 0 : args.BadVDiskMask)
        , FailedDiskCount(0)
        , GroupType(args.ErasureSpecies)
        , ProxyId(MakeBlobStorageProxyID(0))
        , ProxyTestId(MakeBlobStorageProxyID(1))
    {
        for (int i = 0; i < 32; ++i) {
            if (args.BadVDiskMask & (1 << i)) {
                ++FailedDiskCount;
            }
        }

        ShouldBeUnwritable = FailedDiskCount > GroupType.Handoff();
        ShouldBeUndiscoverable = FailedDiskCount > GroupType.ParityParts();

        VDiskCount = args.FailDomainCount * args.DrivesPerFailDomain;
        if (VDiskCount == 0) {
            VDiskCount = GroupType.BlobSubgroupSize();
            DrivesPerFailDomain = 1;
            FailDomainCount = VDiskCount;
        } else {
            DrivesPerFailDomain = args.DrivesPerFailDomain;
            FailDomainCount = args.FailDomainCount;
        }

        VDisks.resize(VDiskCount);
        PDisks.resize(VDiskCount);
        VDiskIds.reserve(VDiskCount);
        for (ui32 failDomainIdx = 0; failDomainIdx < FailDomainCount; ++failDomainIdx) {
            for (ui32 driveIdx = 0; driveIdx < DrivesPerFailDomain; ++driveIdx) {
                ui32 i = GetVDiskTestIdx(failDomainIdx, driveIdx);
                VDisks[i] = MakeBlobStorageVDiskID(2, i + 1, 1);
                PDisks[i] = MakeBlobStoragePDiskID(2, i + 1);
                VDiskIds.emplace_back(0, 1, 0, failDomainIdx, driveIdx);
            }
        }
    }

    ui64 GetVDiskTestIdx(ui64 failDomain, ui64 orderNumber) const {
        return failDomain * DrivesPerFailDomain + orderNumber;
    }

    ui64 GetVDiskTestIdx(const TVDiskID &vDiskId) const {
        ui64 failDomain = vDiskId.FailDomain;
        ui64 orderNumber = vDiskId.VDisk;
        return GetVDiskTestIdx(failDomain, orderNumber);
    }
};

class TTestBlobStorageProxy : public TActor<TTestBlobStorageProxy> {
protected:
    struct TResponseData {
        enum EMessage {
            MessageNone
            , MessageGetResult
            , MessagePutResult
            , MessageVGetResult
            , MessageVPutResult
            , MessageVBlockResult
            , MessageRangeResult
            , MessageDiscoverResult
            , MessageCollectGarbageResult
            , MessageStatusResult
            , MessageBlockResult
            , MessageStartProfilerResult
            , MessageStopProfilerResult
            , MessageVStatusResult
            , MessageVCompactResult
            , MessageProxyQueueState
            , MessageProxySessionsState
        };
        EMessage Message;
        NKikimrProto::EReplyStatus Status;
        TVector<TString> Data;
        TVector<NKikimrProto::EReplyStatus> ItemStatus;
        TVector<TLogoBlobID> ItemIds;
        TString Profile;
        ui32 BlockedGeneration;
        bool LogoBlobsCompacted;
        TStorageStatusFlags StatusFlags;
        TActorId Sender;
        TVDiskID VDiskId;
        bool IsConnected;
        TIntrusivePtr<TGroupQueues> GroupQueues;

        TResponseData()
            : Message(MessageNone)
            , Status(NKikimrProto::OK)
            , BlockedGeneration(0)
            , LogoBlobsCompacted(false)
            , StatusFlags(0)
        {
        }

        void Clear() {
            Message = MessageNone;
            Status = NKikimrProto::OK;
            Data.resize(0);
            ItemStatus.resize(0);
            ItemIds.resize(0);
            Profile.resize(0);
            BlockedGeneration = 0;
            LogoBlobsCompacted = false;
            StatusFlags.Raw = 0;
            Sender = TActorId();
        }
    };

    TResponseData LastResponse;
    const TActorId Proxy;
    const TBlobStorageGroupType::EErasureSpecies ErasureSpecies;
    TIntrusivePtr<TBlobStorageGroupInfo> BsInfo;
    int TestStep;
    int InitStep;
    ui32 InitVDiskIdx;

    ui32 ReadyQueueCount = 0;
    ui32 QueueCount = 0;
    TIntrusivePtr<TGroupQueues> GroupQueues;

    const TIntrusivePtr<TTestEnvironment> Env;
    const TIntrusivePtr<ITestParametrs> Parametrs;

    virtual void TestFSM(const TActorContext &ctx) = 0;
    void ActTestFSM(const TActorContext &ctx) {
        try {
            switch (InitStep) {
            case 0:
            {
                ctx.Send(Env->ProxyId, new TEvRequestProxySessionsState);
                InitStep = 3;
                break;

            case 3:
                UNIT_ASSERT(LastResponse.Message == TResponseData::MessageProxySessionsState);
                GroupQueues = std::move(LastResponse.GroupQueues);
                UNIT_ASSERT(GroupQueues);
                auto &failDomains = GroupQueues->FailDomains;
                for (ui64 failDomainIdx = 0; failDomainIdx < failDomains.size(); ++failDomainIdx) {
                    auto &failDomain = failDomains[failDomainIdx];
                    for (ui64 vDiskIdx = 0; vDiskIdx < failDomain.VDisks.size(); ++vDiskIdx) {
                        ui64 vDiskTestIdx = Env->GetVDiskTestIdx(failDomainIdx, vDiskIdx);
                        if (Env->DeadVDisksMask & (1ull << vDiskTestIdx)) {
                            continue;
                        }
                        auto &queues = failDomain.VDisks[vDiskIdx].Queues;
                        ctx.Send(queues.PutTabletLog.ActorId, new TEvRequestProxyQueueState);
                        ctx.Send(queues.PutAsyncBlob.ActorId, new TEvRequestProxyQueueState);
                        ctx.Send(queues.PutUserData.ActorId, new TEvRequestProxyQueueState);
                        ctx.Send(queues.GetAsyncRead.ActorId, new TEvRequestProxyQueueState);
                        ctx.Send(queues.GetFastRead.ActorId, new TEvRequestProxyQueueState);
                        ctx.Send(queues.GetDiscover.ActorId, new TEvRequestProxyQueueState);
                        QueueCount += 6;
                    }
                }
                InitStep = 6;
                break;
            }
            case 6:
                UNIT_ASSERT(LastResponse.Message == TResponseData::MessageProxyQueueState);
                if (LastResponse.IsConnected) {
                    ReadyQueueCount++;
                } else {
                    ctx.Send(LastResponse.Sender, new TEvRequestProxyQueueState);
                }

                if (ReadyQueueCount != QueueCount) {
                    break;
                }
                InitStep = 8;
                [[fallthrough]];

            case 8:
                while (Env->DeadVDisksMask & (1ull << InitVDiskIdx)) {
                    ++InitVDiskIdx;
                }
                [[fallthrough]];

            case 10:
            {
                bool isSendNeeded = false;
                if (InitStep == 8 || LastResponse.Status == NKikimrProto::NOTREADY || LastResponse.Status == NKikimrProto::ERROR) {
                    if (LastResponse.Status == NKikimrProto::NOTREADY || LastResponse.Status == NKikimrProto::ERROR) {
                        Sleep(TDuration::MilliSeconds(50));
                    }
                    isSendNeeded = true;
                    InitStep = 10;
                } else {
                    ++InitVDiskIdx;
                    while (Env->DeadVDisksMask & (1ull << InitVDiskIdx)) {
                        ++InitVDiskIdx;
                    }

                    if (InitVDiskIdx < Env->VDiskCount) {
                        isSendNeeded = true;
                    } else {
                        InitStep = 20;
                        LastResponse.Clear();
                    }
                }
                if (isSendNeeded) {
                    ui32 domains = BsInfo->GetTotalFailDomainsNum();
                    ui32 drives = Env->VDiskCount / domains;
                    ui32 domainIdx = InitVDiskIdx / drives;
                    ui32 driveIdx = InitVDiskIdx - domainIdx * drives;

                    TVDiskID vDiskId(0, 1, 0, domainIdx, driveIdx);
                    TLogoBlobID from(1, 0, 0, 0, 0, 0, 1);
                    TLogoBlobID to(1, 0, 0, 0, 0, 0, TLogoBlobID::MaxPartId);
                    auto x = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vDiskId,
                                                                            TInstant::Max(),
                                                                            NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                            TEvBlobStorage::TEvVGet::EFlags::None,
                                                                            {},
                                                                            from,
                                                                            to);

                    auto& msgId = *x->Record.MutableMsgQoS()->MutableMsgId();
                    msgId.SetMsgId(1);
                    msgId.SetSequenceId(1);
                    auto queueId = x->Record.GetMsgQoS().GetExtQueueId();
                    GroupQueues->Send(*this, BsInfo->GetTopology(), std::move(x), 0, NWilson::TTraceId(), vDiskId, queueId);
                  break;
                }
                [[fallthrough]];
            }
            case 20:
                TestFSM(ctx);
                break;
            }
            LastResponse.Clear();
        }
        catch (yexception ex) {
            Env->IsLastExceptionSet = true;
            Env->LastException = ex;
            Env->DoneEvent.Signal();
        }
        VERBOSE_COUT("InitStep# " << InitStep);
    }

    void HandleStartProfilerResult(TEvProfiler::TEvStartResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageStartProfilerResult;
        TEvProfiler::TEvStartResult *msg = ev->Get();
        VERBOSE_COUT("HandleStartProfilerResult: " << (msg->IsOk() ? "OK" : "ERROR"));
        ActTestFSM(ctx);
    }

    void HandleStopProfilerResult(TEvProfiler::TEvStopResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageStopProfilerResult;
        TEvProfiler::TEvStopResult *msg = ev->Get();
        VERBOSE_COUT("HandleStopProfilerResult: " << (msg->IsOk() ? "OK" : "ERROR"));
        LastResponse.Profile = msg->Profile();
        ActTestFSM(ctx);
    }

    void HandleBlockResult(TEvBlobStorage::TEvBlockResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageBlockResult;
        TEvBlobStorage::TEvBlockResult *msg = ev->Get();
        VERBOSE_COUT("HandleBlockResult: " << StatusToString(msg->Status));
        LastResponse.Status = msg->Status;
        ActTestFSM(ctx);
    }

    void HandlePutResult(TEvBlobStorage::TEvPutResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessagePutResult;
        TEvBlobStorage::TEvPutResult *msg = ev->Get();
        VERBOSE_COUT("HandlePutResult: " << StatusToString(msg->Status));
        LastResponse.Status = msg->Status;
        LastResponse.StatusFlags = msg->StatusFlags;
        ActTestFSM(ctx);
    }

    void HandleCollectGarbageResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageCollectGarbageResult;
        TEvBlobStorage::TEvCollectGarbageResult *msg = ev->Get();
        VERBOSE_COUT("HandleCollectGarbageResult: " << StatusToString(msg->Status));
        LastResponse.Status = msg->Status;
        ActTestFSM(ctx);
    }

    void HandleGetResult(TEvBlobStorage::TEvGetResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageGetResult;
        TEvBlobStorage::TEvGetResult *msg = ev->Get();

        VERBOSE_COUT("HandleGetResult: " << StatusToString(msg->Status));

        if (msg->Status == NKikimrProto::OK) {
            VERBOSE_COUT(" ResponseSz = " << (int)msg->ResponseSz);
            LastResponse.Data.resize(msg->ResponseSz);
            LastResponse.ItemStatus.resize(msg->ResponseSz);
            LastResponse.ItemIds.resize(msg->ResponseSz);
            for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                TEvBlobStorage::TEvGetResult::TResponse &response = msg->Responses[i];
                LastResponse.ItemStatus[i] = response.Status;
                LastResponse.ItemIds[i] = response.Id;
                VERBOSE_COUT("  response[" << i <<"]: " << StatusToString(response.Status));
                if (response.Status == NKikimrProto::OK) {
                    //TODO: Process response.Id (should be same logoblobid as in request)
                    LastResponse.Data[i] = response.Buffer.ConvertToString();
                    VERBOSE_COUT(" shift: " << response.Shift << " data: " << response.Buffer.ConvertToString());
                }
            }
        }
        LastResponse.Status = msg->Status;
        ActTestFSM(ctx);
    }

    void HandleRangeResult(TEvBlobStorage::TEvRangeResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageRangeResult;
        TEvBlobStorage::TEvRangeResult *msg = ev->Get();
        VERBOSE_COUT("HandleRangeResult: " << StatusToString(msg->Status));

        if (msg->Status == NKikimrProto::OK) {
            VERBOSE_COUT(" Responses count = " << (int)msg->Responses.size());
            LastResponse.Data.resize(msg->Responses.size());
            LastResponse.ItemStatus.resize(msg->Responses.size());
            LastResponse.ItemIds.resize(msg->Responses.size());
            for (ui32 i = 0; i < msg->Responses.size(); ++i) {
                TEvBlobStorage::TEvRangeResult::TResponse &response = msg->Responses[i];
                LastResponse.Data[i] = response.Buffer;
                LastResponse.ItemStatus[i] = NKikimrProto::OK;
                LastResponse.ItemIds[i] = response.Id;
                VERBOSE_COUT("  response[" << i << "] id: " << response.Id.ToString() << " data: " << response.Buffer);
            }
        }
        LastResponse.Status = msg->Status;
        ActTestFSM(ctx);
    }

    void HandleDiscoverResult(TEvBlobStorage::TEvDiscoverResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageDiscoverResult;
        TEvBlobStorage::TEvDiscoverResult *msg = ev->Get();
        VERBOSE_COUT("HandleDiscoverResult# " << msg->ToString());

        if (msg->Status == NKikimrProto::OK) {
            LastResponse.Data.resize(1);
            LastResponse.ItemStatus.resize(1);
            LastResponse.ItemIds.resize(1);
            LastResponse.Data[0] = msg->Buffer;
            LastResponse.ItemStatus[0] = msg->Status;
            LastResponse.ItemIds[0] = msg->Id;
            LastResponse.BlockedGeneration = msg->BlockedGeneration;
        }
        LastResponse.Status = msg->Status;
        ActTestFSM(ctx);
    }

    void HandleVGetResult(TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageVGetResult;
        const NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;

        VERBOSE_COUT("HandleVGetResult: " << StatusToString(record.GetStatus()));

        if (record.GetStatus() == NKikimrProto::NOTREADY) {
            LastResponse.Status = record.GetStatus();
            ActTestFSM(ctx);
            return;
        }

        if (record.GetStatus() == NKikimrProto::OK) {
            VERBOSE_COUT(" Result size = " << (int)record.ResultSize());
            LastResponse.Data.resize(record.ResultSize());
            LastResponse.ItemStatus.resize(record.ResultSize());
            LastResponse.ItemIds.resize(record.ResultSize());
            for (ui32 i = 0, e = (ui32)record.ResultSize(); i != e; ++i) {
                const NKikimrBlobStorage::TQueryResult &result = record.GetResult(i);
                const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                const NKikimrProto::EReplyStatus replyStatus = result.GetStatus();
                LastResponse.ItemStatus[i] = replyStatus;
                LastResponse.ItemIds[i] = blobId;
                VERBOSE_COUT("  response[" << i << "] id: " << blobId.ToString()
                    << " Status: " << StatusToString(replyStatus));

                if (replyStatus == NKikimrProto::OK) {
                    LastResponse.Data[i] = ev->Get()->GetBlobData(result).ConvertToString();
                    VERBOSE_COUT(" data: " << LastResponse.Data[i]);
                }
            }
        }
        LastResponse.Status = record.GetStatus();
        ActTestFSM(ctx);
    }

    void HandleVPutResult(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageVPutResult;
        const NKikimrBlobStorage::TEvVPutResult &record = ev->Get()->Record;

        //VERBOSE_COUT("HandleVPutResult: " << StatusToString(record.GetStatus()));
        VERBOSE_COUT("HandleVPutResult: " << ev->Get()->ToString());

        LastResponse.Status = record.GetStatus();
        ActTestFSM(ctx);
    }

    void HandleVBlockResult(TEvBlobStorage::TEvVBlockResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageVBlockResult;
        const NKikimrBlobStorage::TEvVBlockResult &record = ev->Get()->Record;

        VERBOSE_COUT("HandleVBlockResult: " << StatusToString(record.GetStatus()));

        LastResponse.Status = record.GetStatus();
        ActTestFSM(ctx);
    }

    void HandleVStatusResult(TEvBlobStorage::TEvVStatusResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageVStatusResult;
        const NKikimrBlobStorage::TEvVStatusResult &record = ev->Get()->Record;

        VERBOSE_COUT("HandleVStatusResult: " << StatusToString(record.GetStatus()) <<
            " Compacted: " << record.GetLogoBlobs().GetCompacted());

        LastResponse.Status = record.GetStatus();
        LastResponse.LogoBlobsCompacted = record.GetLogoBlobs().GetCompacted();
        ActTestFSM(ctx);
    }

    void HandleStatusResult(TEvBlobStorage::TEvStatusResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageStatusResult;
        TEvBlobStorage::TEvStatusResult *msg = ev->Get();
        VERBOSE_COUT("HandleStatusResult# " << msg->ToString());
        LastResponse.Status = msg->Status;
        LastResponse.StatusFlags = msg->StatusFlags;
        ActTestFSM(ctx);
    }

    void HandleVCompactResult(TEvBlobStorage::TEvVCompactResult::TPtr &ev, const TActorContext &ctx) {
        LastResponse.Message = TResponseData::MessageVCompactResult;
        const NKikimrBlobStorage::TEvVCompactResult &record = ev->Get()->Record;

        VERBOSE_COUT("HandleVCompactResult: " << StatusToString(record.GetStatus()));

        LastResponse.Status = record.GetStatus();
        ActTestFSM(ctx);
    }

    void HandleBoot(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx) {
        VERBOSE_COUT("HandleBoot");
        ActTestFSM(ctx);
        Y_UNUSED(ev);
    }

    void HandleProxyQueueState(TEvProxyQueueState::TPtr &ev, const TActorContext &ctx) {
        VERBOSE_COUT("HandleProxyQueueState");
        LastResponse.Message = TResponseData::MessageProxyQueueState;
        LastResponse.VDiskId = ev->Get()->VDiskId;
        LastResponse.IsConnected = ev->Get()->IsConnected;
        LastResponse.Sender = ev->Sender;
        ActTestFSM(ctx);
    }

    void HandleProxySessionsState(TEvProxySessionsState::TPtr &ev, const TActorContext &ctx) {
        VERBOSE_COUT("HandleProxySessionsState");
        LastResponse.Message = TResponseData::MessageProxySessionsState;
        LastResponse.GroupQueues = std::move(ev->Get()->GroupQueues);
        LastResponse.Sender = ev->Sender;
        ActTestFSM(ctx);
    }

public:
    TTestBlobStorageProxy(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TActor(&TThis::StateRegister)
        , Proxy(proxy)
        , ErasureSpecies(bsInfo->Type.GetErasure())
        , BsInfo(bsInfo)
        , TestStep(0)
        , InitStep(0)
        , InitVDiskIdx(0)
        , Env(env)
        , Parametrs(parametrs)
    {}

    STFUNC(StateRegister) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvBoot, HandleBoot);
            HFunc(TEvBlobStorage::TEvPutResult, HandlePutResult);
            HFunc(TEvBlobStorage::TEvGetResult, HandleGetResult);
            HFunc(TEvBlobStorage::TEvRangeResult, HandleRangeResult);
            HFunc(TEvBlobStorage::TEvVGetResult, HandleVGetResult);
            HFunc(TEvBlobStorage::TEvVPutResult, HandleVPutResult);
            HFunc(TEvBlobStorage::TEvVBlockResult, HandleVBlockResult);
            HFunc(TEvBlobStorage::TEvVStatusResult, HandleVStatusResult);
            HFunc(TEvBlobStorage::TEvStatusResult, HandleStatusResult);
            HFunc(TEvBlobStorage::TEvVCompactResult, HandleVCompactResult);
            HFunc(TEvBlobStorage::TEvDiscoverResult, HandleDiscoverResult);
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, HandleCollectGarbageResult);
            HFunc(TEvBlobStorage::TEvBlockResult, HandleBlockResult);
            HFunc(TEvProfiler::TEvStartResult, HandleStartProfilerResult);
            HFunc(TEvProfiler::TEvStopResult, HandleStopProfilerResult);
            HFunc(TEvProxyQueueState, HandleProxyQueueState);
            HFunc(TEvProxySessionsState, HandleProxySessionsState);
        }
    }
};

class TTestBlobStorageProxyBlockSet : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvBlock");
                ctx.Send(Proxy, new TEvBlobStorage::TEvBlock(1, 0, TInstant::Max()));
                break;
            case 10:
                TEST_RESPONSE(MessageBlockResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyBlockSet(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyBlockCheck : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 0, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "xxx", TInstant::Max()));
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessagePutResult, BLOCKED, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 1, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "xxx", TInstant::Max()));
                break;
            }
            case 20:
            {
                TEST_RESPONSE(MessagePutResult, BLOCKED, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 1, 0, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "xxx", TInstant::Max()));
                break;
            }
            case 30:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyBlockCheck(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};


char DefaultCharGenerator(int i) {
    return i % 256;
}

template <const TVector<size_t>& Order>
class TTestIterativePut: public TTestBlobStorageProxy {
    void TestFSM(const TActorContext& ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);

        if (!TestStep) {
            UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                                "Unexpected " << (int)LastResponse.Message);
            for (auto idx : Order) {
                char ch = DefaultCharGenerator(idx);
                TLogoBlobID logoblobid(1, idx, 0, 0, 1, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, TString(ch), TInstant::Max()));
            }
        } else if (TestStep < (int)Order.size()) {
            TEST_RESPONSE(MessagePutResult, OK, 0, "");
        } else {
            TEST_RESPONSE(MessagePutResult, OK, 0, "");
            VERBOSE_COUT("Done");
            Env->DoneEvent.Signal();
        }
        TestStep++;
    }

public:
    TTestIterativePut(const TActorId& proxy, const TIntrusivePtr<TBlobStorageGroupInfo>& bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {
    }
};

template <ui64 PutInFlight, ui64 MaxSendPut>
class TTestInFlightPuts: public TTestBlobStorageProxy {
    TString Data;

    void TestFSM(const TActorContext& ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        ui64 testStep = TestStep;
        if (!testStep) {
            UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                                "Unexpected " << (int)LastResponse.Message);
            for (ui64 idx = 0; idx < PutInFlight && idx < MaxSendPut; ++idx) {
                TLogoBlobID logoblobid(1, idx, 0, 0, Data.size(), 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, Data, TInstant::Max()));
            }
        } else if (testStep <= MaxSendPut - PutInFlight) {
            TEST_RESPONSE(MessagePutResult, OK, 0, "");
            ui64 idx = testStep + PutInFlight - 1;
            TLogoBlobID logoblobid(1, idx, 0, 0, Data.size(), 0);
            ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, Data, TInstant::Max()));
        } else if (testStep < MaxSendPut) {
            TEST_RESPONSE(MessagePutResult, OK, 0, "");
        } else {
            TEST_RESPONSE(MessagePutResult, OK, 0, "");
            VERBOSE_COUT("Done");
            Env->DoneEvent.Signal();
        }
        TestStep++;
    }

public:
    TTestInFlightPuts(const TActorId& proxy, const TIntrusivePtr<TBlobStorageGroupInfo>& bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {
        Data.reserve(4096);
        for (ui64 i = 0; i < 4096; ++i) {
            Data.push_back('a');
        }
    }
};

template <const TVector<size_t>& Order>
class TTestOrderGet: public TTestBlobStorageProxy {
    size_t GetIdx(int i) {
        return Order ? Order[i] : i;
    }

    void InitExpectedValues() {
        ExpectedValues.resize(Order.size());
        for (size_t idx = 0; idx != Order.size(); idx++) {
            ExpectedValues[idx] = TString(DefaultCharGenerator(GetIdx(idx)));
        }
    }

    void TestFSM(const TActorContext& ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);

        if (!TestStep) {
            UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                                "Unexpected " << (int)LastResponse.Message);

            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> q(new TEvBlobStorage::TEvGet::TQuery[Order.size()]);
            for (size_t idx = 0; idx < Order.size(); ++idx) {
                q[idx].Set(TLogoBlobID(1, GetIdx(idx), 0, 0, 1, 0));
            }

            ctx.Send(Proxy, new TEvBlobStorage::TEvGet(q, Order.size(), TInstant::Max(),
                                                    NKikimrBlobStorage::EGetHandleClass::FastRead));
        } else {
            InitExpectedValues();
            TEST_RESPONSE_FULLCHECK(MessageGetResult, OK, Order.size(), ExpectedValues);

            VERBOSE_COUT("Done");
            Env->DoneEvent.Signal();
        }
        TestStep++;
    }

public:
    TTestOrderGet(const TActorId& proxy, const TIntrusivePtr<TBlobStorageGroupInfo>& bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {
    }

private:
    TVector<TString> ExpectedValues;
};

template <int StartIdx, int RangeLength>
class TTestRangeGet: public TTestBlobStorageProxy {
    void InitExpectedValues() {
        ExpectedValues.resize(RangeSize);
        for (int i = 0; i != RangeSize; i++) {
            ExpectedValues[i] = TString(DefaultCharGenerator(StartIdx + i * DIdx));
        }
    }

    void TestFSM(const TActorContext& ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);

        if (!TestStep) {
            UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                                "Unexpected " << (int)LastResponse.Message);

            TLogoBlobID from(1, StartIdx, 0, 0, 1, 0);
            TLogoBlobID to(1, StartIdx + RangeLength, 0, 0, 1, 0);
            ctx.Send(Proxy, new TEvBlobStorage::TEvRange(1, from, to, true, TInstant::Max()));
        } else {
            InitExpectedValues();
            TEST_RESPONSE_FULLCHECK(MessageRangeResult, OK, (size_t)RangeSize, ExpectedValues);

            VERBOSE_COUT("Done");
            Env->DoneEvent.Signal();
        }
        TestStep++;
    }

public:
    TTestRangeGet(const TActorId& proxy, const TIntrusivePtr<TBlobStorageGroupInfo>& bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {
    }

private:
    const int RangeSize = std::abs(RangeLength);
    const int DIdx = RangeLength > 0 ? 1 : -1;
    TVector<TString> ExpectedValues;
};


class TTestBlobStorageProxyBlock : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvBlock");
                ctx.Send(Proxy, new TEvBlobStorage::TEvBlock(1, 0, TInstant::Max()));
                break;
            case 10:
            {
                TEST_RESPONSE(MessageBlockResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 0, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "xxx", TInstant::Max()));
                break;
            }
            case 20:
            {
                TEST_RESPONSE(MessagePutResult, BLOCKED, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 1, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "xxx", TInstant::Max()));
                break;
            }
            case 30:
            {
                TEST_RESPONSE(MessagePutResult, BLOCKED, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TString data = "aaa";
                TLogoBlobID logoblobid(1, 1, 0, 0, data.size(), 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, data, TInstant::Max()));
                break;
            }
            case 40:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvBlock");
                ctx.Send(Proxy, new TEvBlobStorage::TEvBlock(1, 1, TInstant::Max()));
                break;
            case 50:
            {
                TEST_RESPONSE(MessageBlockResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 1, 0, 0, 3, 1);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "xxx", TInstant::Max()));
                break;
            }
            case 60:
            {
                TEST_RESPONSE(MessagePutResult, BLOCKED, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 1, 1, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "yyy", TInstant::Max()));
                break;
            }
            case 70:
                TEST_RESPONSE(MessagePutResult, BLOCKED, 0, "");

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 80:
            {
                TString data = "aaa";
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageDiscoverResult,
                        "Unexpected message " << (int)LastResponse.Message);
                UNIT_ASSERT_EQUAL_C(LastResponse.Status, NKikimrProto::OK, "Unexpected status "
                        << StatusToString(LastResponse.Status));
                VERBOSE_COUT("Data: " << LastResponse.Data[0]);

                TEST_RESPONSE(MessageDiscoverResult, OK, 1, data);
                UNIT_ASSERT_VALUES_EQUAL_C(LastResponse.BlockedGeneration, 1, "Unexpected BlockedGeneration = " <<
                    LastResponse.BlockedGeneration);
                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 2, 0, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "bbb", TInstant::Max()));
                break;
            }
            case 90:

                TEST_RESPONSE(MessagePutResult, OK, 0, "");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyBlock(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};


class TTestBlobStorageProxyPut : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvPut");

                TLogoBlobID logoblobid(1, 0, 0, 0, testData2.size(), 0);
                VERBOSE_COUT(" put logoblobid# " << logoblobid.ToString());
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData2, TInstant::Max()));
                break;
            }
            case 10:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyPut(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyPutInvalidSize : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 0, 0, testData2.size(), 0);
                std::unique_ptr<TEvBlobStorage::TEvPut> put(new TEvBlobStorage::TEvPut(logoblobid, testData2, TInstant::Max()));
                const_cast<TLogoBlobID&>(put->Id) = TLogoBlobID(1, 0, 0, 0, 1, 0);

                ctx.Send(Proxy, put.release());
                break;
            }
            case 10:
                TEST_RESPONSE(MessagePutResult, ERROR, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyPutInvalidSize(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyPutFail : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 0, 0, testData2.size(), 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData2, TInstant::Max()));
                break;
            }
            case 10:
                TEST_RESPONSE(MessagePutResult, ERROR, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyPutFail(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyGet : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 0, 0, testData2.size(), 0);
                VERBOSE_COUT(" get logoblobid# " << logoblobid.ToString());
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, true));
                break;
            }
            case 10:
                TEST_RESPONSE(MessageGetResult, OK, 1, testData2);

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGet(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};


class TTestBlobStorageProxyGetFail : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 0, 0, 100, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 10:
                TEST_RESPONSE(MessageGetResult, OK, 1, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGetFail(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyGetTimeout : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 0, 0, 0, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 10:
                // TODO: TEST_RESPONSE(MessageGetResult, TIMEOUT, 0, "");
                TEST_RESPONSE(MessageGetResult, ERROR, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGetTimeout(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyPutGetMany: public TTestBlobStorageProxy {
    TString Prepare(ui64 idx, ui64 size) {
        TString data;
        data.resize(size);
        ui8 *d = (ui8*)const_cast<char*>(data.data());
        for (ui64 i = 0; i < size; ++i) {
            d[i] = 'A' + ((idx + i) & 0xf);
        }
        return data;
    }

    void TestFSM(const TActorContext &ctx) {
        const ui64 size = 1 << 20;
        const ui64 count = 50;

        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        if (TestStep == 0) {
            UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                "Unexpected " << (int)LastResponse.Message);
        } else if (TestStep <= int(count)) {
            TEST_RESPONSE(MessagePutResult, OK, 0, "");
        } else if (TestStep == int(count) + 1) {
            TEST_RESPONSE_3(MessageGetResult, OK, count);
            VERBOSE_COUT("Done");
            Env->DoneEvent.Signal();
            ++TestStep;
            return;
        } else {
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        }

        if (TestStep < int(count)) {
            VERBOSE_COUT(" Sending TEvPut");
            ui64 idx = TestStep;
            TString data = Prepare(idx, size);
            TLogoBlobID id(1, 2, 3, 4, size, idx);
            ctx.Send(Proxy, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        } else {
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> q(new TEvBlobStorage::TEvGet::TQuery[count]);
            for (ui64 idx = 0; idx < count; ++idx) {
                q[idx].Set(TLogoBlobID(1, 2, 3, 4, size, idx), size/4 - 100, 200);
            }
            ctx.Send(Proxy, new TEvBlobStorage::TEvGet(q, count, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        }

        ++TestStep;
    }
public:
    TTestBlobStorageProxyPutGetMany(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyPutGetStatus: public TTestBlobStorageProxy {
    TString Prepare(ui64 idx, ui64 size) {
        TString data;
        data.resize(size);
        ui8 *d = (ui8*)const_cast<char*>(data.data());
        for (ui64 i = 0; i < size; ++i) {
            d[i] = 'A' + ((idx + i) & 0xf);
        }
        return data;
    }

    void TestFSM(const TActorContext &ctx) {
        const ui64 size = 2 << 20;
        const ui64 count = 64;
        const int threshold = 10;

        VERBOSE_COUT("Test step " << TestStep << " Flags: " << LastResponse.StatusFlags.ToString()
                << " Line " << __LINE__);
        if (TestStep == 0) {
            UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                "Unexpected " << (int)LastResponse.Message);
        } else if (TestStep < int(count)) {
            if (LastResponse.Status == NKikimrProto::OK) {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");
                if (LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusIsValid)) {
                    if (LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange)) {
                        if (TestStep < int(count) - threshold) {
                            TestStep = int(count) - threshold;
                        }
                    }
                    UNIT_ASSERT(!(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceRed)));
                }
            } else {
                TEST_RESPONSE(MessagePutResult, ERROR, 0, "");
                if (LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusIsValid)) {
                    UNIT_ASSERT(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange));
                    if (LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange)) {
                        if (TestStep < int(count) - threshold) {
                            TestStep = int(count) - threshold;
                        }
                    }
                    UNIT_ASSERT(!(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceRed)));
                }
            }
        } else if (TestStep == int(count)) {
            if (LastResponse.Status == NKikimrProto::OK) {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");
                if (LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusIsValid)) {
                    UNIT_ASSERT(!(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceRed)));
                    UNIT_ASSERT(!(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange)));
                }
            } else {
                TEST_RESPONSE(MessagePutResult, ERROR, 0, "");
                if (LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusIsValid)) {
                    UNIT_ASSERT(!(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceRed)));
                    UNIT_ASSERT(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange));
                    UNIT_ASSERT(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove));
                }
            }
        } else if (TestStep == int(count) + 1) {
            TEST_RESPONSE(MessageStatusResult, OK, 0, "");
            if (LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusIsValid)) {
                UNIT_ASSERT(!(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusDiskSpaceRed)));
            } else {
                UNIT_ASSERT(LastResponse.StatusFlags.Raw & ui32(NKikimrBlobStorage::StatusIsValid));
            }
            VERBOSE_COUT("Done");
            Env->DoneEvent.Signal();
            ++TestStep;
            return;
        } else {
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
        }

        if (TestStep < int(count)) {
            VERBOSE_COUT(" Sending TEvPut");
            ui64 idx = TestStep;
            TString data = Prepare(idx, size);
            TLogoBlobID id(1, 2, 3, 4, size, idx);
            ctx.Send(Proxy, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        } else {
            ctx.Send(Proxy, new TEvBlobStorage::TEvStatus(TInstant::Max()));
        }

        ++TestStep;
    }
public:
    TTestBlobStorageProxyPutGetStatus(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

template <int vDiskIdx>
class TTestBlobStorageProxyVPutVGet : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        TLogoBlobID blobId(1, 0, 0, 0, testData2.size(), 0);
        TString encryptedTestData2;
        encryptedTestData2.resize(testData2.size());
        Encrypt(encryptedTestData2.Detach(), testData2.data(), 0, testData2.size(), blobId, *BsInfo);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvVPut");
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                TBlobStorageGroupType type(ErasureSpecies);
                NKikimr::TDataPartSet partSet;
                type.SplitData((TErasureType::ECrcMode)blobId.CrcMode(), encryptedTestData2, partSet);

                TLogoBlobID logoblobid(1, 0, 0, 0, testData2.size(), 0, 1);

                TAutoPtr<TEvBlobStorage::TEvVPut> vPut(
                   new TEvBlobStorage::TEvVPut(logoblobid, partSet.Parts[0].OwnedString, vDiskId, false,
                                               nullptr, TInstant::Max(), NKikimrBlobStorage::AsyncBlob));
                ctx.Send(Env->VDisks[vDiskIdx], vPut.Release());
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessageVPutResult, OK, 0, "");
                VERBOSE_COUT(" Sending TEvVGet");
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                TLogoBlobID id(1, 0, 0, 0, testData2.size(), 0, 1);
                auto x = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId,
                                                                         TInstant::Max(),
                                                                         NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                         TEvBlobStorage::TEvVGet::EFlags::None,
                                                                         {},
                                                                         {id});
                ctx.Send(Env->VDisks[vDiskIdx], x.release());
                break;
            }
            case 20:
            {
                TBlobStorageGroupType type(ErasureSpecies);
                NKikimr::TDataPartSet partSet;
                type.SplitData((TErasureType::ECrcMode)blobId.CrcMode(), encryptedTestData2, partSet);
                TEST_RESPONSE(MessageVGetResult, OK, 1, partSet.Parts[0].OwnedString.ConvertToString());

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyVPutVGet(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

template <int vDiskIdx>
class TTestBlobStorageProxyVPutVGetLimit : public TTestBlobStorageProxy {
    int Iteration;
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending 10 TEvVPut");

                for (int i = 0; i < 10; ++i) {
                    TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                    TLogoBlobID logoblobid(1, 0, i, 0, testData2.size(), 0, 1);

                    const ui32 blobSubgroupSize = BsInfo->Type.BlobSubgroupSize();
                    TBlobStorageGroupInfo::TServiceIds vdisksSvc;
                    TBlobStorageGroupInfo::TVDiskIds vdisksId;
                    BsInfo->PickSubgroup(logoblobid.Hash(), &vdisksId, &vdisksSvc);
                    ui32 partIdx = 0;
                    for (ui32 idx = 0; idx < blobSubgroupSize; ++idx) {
                        if (vdisksId[idx] == vDiskId) {
                            partIdx = idx;
                        }
                    }
                    if (partIdx >= BsInfo->Type.TotalPartCount()) {
                        partIdx = 0;
                    }

                    TLogoBlobID id(1, 0, i/*step*/, 0, testData2.size(), 0, partIdx + 1);

                    TBlobStorageGroupType type(ErasureSpecies);
                    NKikimr::TDataPartSet partSet;

                    TLogoBlobID blobId(1, 0, i/*step*/, 0, testData2.size(), 0);
                    TString encryptedTestData2;
                    encryptedTestData2.resize(testData2.size());
                    Encrypt(encryptedTestData2.Detach(), testData2.data(), 0, testData2.size(), blobId, *BsInfo);

                    type.SplitData((TErasureType::ECrcMode)blobId.CrcMode(), encryptedTestData2, partSet);

                    TAutoPtr<TEvBlobStorage::TEvVPut> vPut(
                        new TEvBlobStorage::TEvVPut(id, partSet.Parts[partIdx].OwnedString, vDiskId, false,
                        nullptr, TInstant::Max(), NKikimrBlobStorage::AsyncBlob));
                    auto& msgId = *vPut->Record.MutableMsgQoS()->MutableMsgId();
                    msgId.SetMsgId(i);
                    msgId.SetSequenceId(1);
                    ctx.Send(Env->VDisks[vDiskIdx], vPut.Release());
                }
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessageVPutResult, OK, 0, "");
                ++Iteration;
                if (Iteration < 10) {
                    return;
                }
                VERBOSE_COUT(" Sending TEvVGet");
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                TLogoBlobID id1(1, 0, 0, 0, testData2.size(), 0, 1);
                TLogoBlobID id2(1, 0, 10, 0, testData2.size(), 0, 1);
                auto x = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vDiskId,
                                                                        TInstant::Max(),
                                                                        NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                        TEvBlobStorage::TEvVGet::EFlags::None,
                                                                        {},
                                                                        id1,
                                                                        id2,
                                                                        5);
                ctx.Send(Env->VDisks[vDiskIdx], x.release());
                break;
            }
            case 20:
            {
                TEST_RESPONSE_3(MessageVGetResult, OK, 5);
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyVPutVGetLimit(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
        , Iteration(0)
    {}
};

enum EVDiskIdxMode {
    VDIM_TestIdx,
    VDIM_IdxInSubgroup
};

class TTestBlobStorageProxyVPut : public TTestBlobStorageProxy {
public:
    struct TParametrs : public ITestParametrs {
        using TPtr = TIntrusivePtr<TParametrs>;

        TString TestData;
        ui64 PartId;
        EVDiskIdxMode Mode = VDIM_IdxInSubgroup;
        ui64 VDiskIdx = Max<ui64>(); // default value for VDIM_IdxInSubgroup

        TParametrs(TString testData = "", ui64 partId = 0, EVDiskIdxMode mode = VDIM_IdxInSubgroup,
                ui64 vDiskIdx = Max<ui64>())
            : TestData(testData)
            , PartId(partId)
            , Mode(mode)
            , VDiskIdx(vDiskIdx)
        {}

        virtual ~TParametrs()
        {}
    };

private:
    TParametrs& GetParametrs() {
        TParametrs *param = dynamic_cast<TParametrs*>(Parametrs.Get());
        UNIT_ASSERT(param);
        return *param;
    }

    void TestFSM(const TActorContext &ctx) {
        TParametrs &parametrs = GetParametrs();
        UNIT_ASSERT(parametrs.PartId <= BsInfo->Type.TotalPartCount());
        UNIT_ASSERT(parametrs.PartId > 0);
        TLogoBlobID blobId(1, 0, 0, 0, parametrs.TestData.size(), 0);
        TLogoBlobID logoblobid(1, 0, 0, 0, parametrs.TestData.size(), 0, parametrs.PartId);
        UNIT_ASSERT(parametrs.VDiskIdx < BsInfo->Type.BlobSubgroupSize() || parametrs.VDiskIdx == Max<ui64>());
        TMaybe<TVDiskID> vDiskId;
        ui64 realVDiskIdx = parametrs.VDiskIdx;

        switch (parametrs.Mode) {
            case VDIM_TestIdx:
                UNIT_ASSERT(parametrs.VDiskIdx != Max<ui64>());
                vDiskId = Env->VDiskIds[parametrs.VDiskIdx];
                break;
            case VDIM_IdxInSubgroup:
                ui64 idxInSubgroup = (parametrs.VDiskIdx == Max<ui64>()) ? (parametrs.PartId  - 1) : parametrs.VDiskIdx;
                vDiskId = BsInfo->GetVDiskInSubgroup(idxInSubgroup, blobId.Hash());
                realVDiskIdx = Env->GetVDiskTestIdx(*vDiskId);
                if (*vDiskId != Env->VDiskIds[realVDiskIdx]) {
                    TStringBuilder str;
                    str << "partId# " << parametrs.PartId;
                    str << " idxInSubgroup# " << idxInSubgroup;
                    str << " vDiskId# " << vDiskId->ToString();
                    str << " realVDiskIdx# " << realVDiskIdx;
                    str << " vDisks# [";
                    for (ui64 idx = 0; idx < Env->VDiskIds.size(); ++idx) {
                        str << (idx ? " " : "") << Env->VDiskIds[idx].ToString();
                    }
                    str << "]";
                    UNIT_ASSERT_C(*vDiskId == Env->VDiskIds[realVDiskIdx], str);
                }
                break;
        }
        UNIT_ASSERT(vDiskId);
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);

        TString encryptedTestData;
        encryptedTestData.resize(parametrs.TestData.size());
        Encrypt(encryptedTestData.Detach(), parametrs.TestData.data(), 0, parametrs.TestData.size(), blobId, *BsInfo);

        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvVPut to " << vDiskId->ToString() << " blob " << logoblobid.ToString());

                NKikimr::TDataPartSet partSet;
                BsInfo->Type.SplitData((TErasureType::ECrcMode)blobId.CrcMode(), encryptedTestData, partSet);

                TAutoPtr<TEvBlobStorage::TEvVPut> vPut(
                        new TEvBlobStorage::TEvVPut(logoblobid, partSet.Parts[parametrs.PartId-1].OwnedString, *vDiskId,
                            false, nullptr, TInstant::Max(), NKikimrBlobStorage::AsyncBlob));
                ctx.Send(Env->VDisks[realVDiskIdx], vPut.Release());
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessageVPutResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyVPut(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

template <int vDiskIdx, int partId, bool &isNoData>
class TTestBlobStorageProxyVGet : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");

        TLogoBlobID id(1, 0, 0, 0, testData2.size(), 0, partId);
        TLogoBlobID blobId(1, 0, 0, 0, testData2.size(), 0);
        TString encryptedTestData2;
        encryptedTestData2.resize(testData2.size());
        Encrypt(encryptedTestData2.Detach(), testData2.data(), 0, testData2.size(), blobId, *BsInfo);

        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvVGet");
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                auto x = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId,
                                                                         TInstant::Max(),
                                                                         NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                         TEvBlobStorage::TEvVGet::EFlags::None,
                                                                         {},
                                                                         {id});
                ctx.Send(Env->VDisks[vDiskIdx], x.release());
                break;
            }
            case 10:
            {
                if (LastResponse.Message == TResponseData::MessageVGetResult) {
                    if (LastResponse.Status == NKikimrProto::OK) {
                        if (LastResponse.ItemStatus.size() == 1) {
                            if (LastResponse.ItemStatus[0] == NKikimrProto::NOT_YET) {
                                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                                VERBOSE_COUT("NOT_YET, retry, vDiskId# " << vDiskId.ToString());
                                TestStep -= 10;

                                TLogoBlobID id(1, 0, 0, 0, encryptedTestData2.size(), 0, partId);
                                auto x = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId,
                                                                                         TInstant::Max(),
                                                                                         NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                                         TEvBlobStorage::TEvVGet::EFlags::None,
                                                                                         {},
                                                                                         {id});
                                ctx.Send(Env->VDisks[vDiskIdx], x.release());
                                break;
                            } else if (LastResponse.ItemStatus[0] == NKikimrProto::NODATA) {
                                isNoData = true;
                                VERBOSE_COUT("Done");
                                Env->DoneEvent.Signal();
                                break;
                            } else if (LastResponse.ItemStatus[0] != NKikimrProto::OK) {
                                UNIT_ASSERT(false);
                            }
                        }
                    }
                }
                TBlobStorageGroupType type(ErasureSpecies);
                NKikimr::TDataPartSet partSet;
                type.SplitData((TErasureType::ECrcMode)blobId.CrcMode(), encryptedTestData2, partSet);
                TEST_RESPONSE(MessageVGetResult, OK, 1, partSet.Parts[0].OwnedString.ConvertToString());

                isNoData = false;
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyVGet(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

template <int vDiskIdx>
class TTestBlobStorageProxyVGetFail : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvVGet");
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                TLogoBlobID from(1, 0, 0, 0, 0, 0, 1);
                TLogoBlobID to(1, 0, 0, 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId);
                auto x = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vDiskId,
                                                                        TInstant::Max(),
                                                                        NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                        TEvBlobStorage::TEvVGet::EFlags::None,
                                                                        {},
                                                                        from,
                                                                        to);
                ctx.Send(Env->VDisks[vDiskIdx], x.release());
                break;
            }
            case 10:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageVGetResult,
                        "Unexpected message " << (int)LastResponse.Message);
                UNIT_ASSERT_EQUAL_C(LastResponse.Status, NKikimrProto::OK,
                        "Unexpected status " << StatusToString(LastResponse.Status));
                if (LastResponse.Data.size() != 0) {
                    UNIT_ASSERT_EQUAL_C(LastResponse.Data.size(), 1,
                            "Data size " << (int)LastResponse.Data.size() << " while expected 0 or 1");
                    UNIT_ASSERT_EQUAL_C(LastResponse.ItemStatus.size(), 1,
                            "ItemStatus size " << (int)LastResponse.ItemStatus.size() << " while expected 1");
//                    UNIT_ASSERT_EQUAL_C(LastResponse.ItemStatus[0], NKikimrProto::NOT_YET,
//                            "Unexpected item status " << StatusToString(LastResponse.ItemStatus[0]));

                    TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                    auto x = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId,
                                                                             TInstant::Max(),
                                                                             NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                             TEvBlobStorage::TEvVGet::EFlags::None,
                                                                             {},
                                                                             {LastResponse.ItemIds[0]});
                    auto& msgId = *x->Record.MutableMsgQoS()->MutableMsgId();
                    msgId.SetMsgId(0);
                    msgId.SetSequenceId(1);

                    ctx.Send(Env->VDisks[vDiskIdx], x.release());
                } else {
                    VERBOSE_COUT("Done");
                    Env->DoneEvent.Signal();
                }
            }
                break;
            case 20:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageVGetResult,
                        "Unexpected message " << (int)LastResponse.Message);
                UNIT_ASSERT_EQUAL_C(LastResponse.Status, NKikimrProto::OK,
                        "Unexpected status " << StatusToString(LastResponse.Status));
                UNIT_ASSERT_EQUAL_C(LastResponse.ItemStatus.size(), 1,
                        "Unexpected result size " << LastResponse.ItemStatus.size());
                if (LastResponse.ItemStatus[0] != NKikimrProto::NOT_YET &&
                        LastResponse.ItemStatus[0] != NKikimrProto::NODATA) {
                    UNIT_ASSERT_EQUAL_C(LastResponse.ItemStatus[0], NKikimrProto::NOT_YET,
                            "Unexpected item status " << StatusToString(LastResponse.ItemStatus[0]));
                }

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
            }
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyVGetFail(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};


class TTestBlobStorageProxyVBlockVPutVGet : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData("xxx");
        TVDiskID vDiskId(0, 1, 0, 0, 0);

        switch (TestStep) {
            case 0:
            {
                VERBOSE_COUT(" Starting, LastResponse.Message# " << (i32)LastResponse.Message <<
                        " None#" << (i32)TResponseData::MessageNone);
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                TAutoPtr<TEvBlobStorage::TEvVBlock> x(new TEvBlobStorage::TEvVBlock(1, 1, vDiskId, TInstant::Max()));
                auto& msgId = *x->Record.MutableMsgQoS()->MutableMsgId();
                msgId.SetMsgId(0);
                msgId.SetSequenceId(1);

                ctx.Send(Env->VDisks[0], x.Release());
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessageVBlockResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvVPut");
                TBlobStorageGroupType type(ErasureSpecies);
                NKikimr::TDataPartSet partSet;
                TLogoBlobID logoblobid(1, 1, 1, 0, testData.size(), 3, 1);
                type.SplitData((TErasureType::ECrcMode)logoblobid.CrcMode(), testData, partSet);

                TAutoPtr<TEvBlobStorage::TEvVPut> x(
                    new TEvBlobStorage::TEvVPut(logoblobid, partSet.Parts[0].OwnedString, vDiskId, false,
                        nullptr, TInstant::Max(), NKikimrBlobStorage::AsyncBlob));
                auto& msgId = *x->Record.MutableMsgQoS()->MutableMsgId();
                msgId.SetMsgId(0);
                msgId.SetSequenceId(1);

                ctx.Send(Env->VDisks[0], x.Release());
                break;
            }
            case 20:
            {
                TEST_RESPONSE(MessageVPutResult, BLOCKED, 0, "");
                VERBOSE_COUT(" Sending TEvVGet");

                TLogoBlobID id(1, 1, 1, 0, testData.size(), 3);

                auto x = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId,
                                                                         TInstant::Max(),
                                                                         NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                         TEvBlobStorage::TEvVGet::EFlags::None,
                                                                         {},
                                                                         {id});

                auto& msgId = *x->Record.MutableMsgQoS()->MutableMsgId();
                msgId.SetMsgId(0);
                msgId.SetSequenceId(1);

                ctx.Send(Env->VDisks[0], x.release());
                break;
            }
            case 30:
            {
                TEST_RESPONSE(MessageVGetResult, OK, 1, "");
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyVBlockVPutVGet(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};



class TTestBlobStorageProxyGarbageMark : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvCollectGarbage Keep");
                std::unique_ptr<TVector<TLogoBlobID>> Keep;
                Keep.reset(new TVector<TLogoBlobID>);
                Keep->push_back(TLogoBlobID(1, 0, 0, 0, 104, 0));
                VERBOSE_COUT(" keep logoblobid# " << (*Keep)[0].ToString());
                ctx.Send(Proxy, new TEvBlobStorage::TEvCollectGarbage(1, 0, 0, false, 0, 0, Keep.release(), nullptr,
                    TInstant::Max()));
                break;
            }
            case 10:
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGarbageMark(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyGarbageUnmark : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvCollectGarbage DoNotKeep");
                std::unique_ptr<TVector<TLogoBlobID>> DoNotKeep;
                DoNotKeep.reset(new TVector<TLogoBlobID>);
                DoNotKeep->push_back(TLogoBlobID(1, 0, 0, 0, 104, 0));
                VERBOSE_COUT(" donotkeep logoblobid# " << (*DoNotKeep)[0].ToString());
                ctx.Send(Proxy, new TEvBlobStorage::TEvCollectGarbage(1, 2, 0, false, 0, 0, nullptr,
                    DoNotKeep.release(), TInstant::Max()));
                break;
            }
            case 10:
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGarbageUnmark(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyGarbageCollect : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvCollectGarbage collect logoblobid");
                ctx.Send(Proxy, new TEvBlobStorage::TEvCollectGarbage(1, 1, 0, true, 0, 0, nullptr, nullptr,
                            TInstant::Max()));
                break;
            }
            case 10:
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGarbageCollect(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyGarbageCollectHuge : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvCollectGarbage collect");
                ui64 keepCount = 12300;
                ui64 doNotKeepCount = 45600;
                std::unique_ptr<TVector<TLogoBlobID>> keep(new TVector<TLogoBlobID>);
                std::unique_ptr<TVector<TLogoBlobID>> doNotKeep(new TVector<TLogoBlobID>);
                keep->reserve(keepCount);
                doNotKeep->reserve(doNotKeepCount);
                for (ui64 idx = 0; idx < keepCount; ++idx) {
                    keep->emplace_back(123, 2, idx, 0, 100500, 1);
                }
                for (ui64 idx = 0; idx < doNotKeepCount; ++idx) {
                    doNotKeep->emplace_back(123, 1, idx, 0, 100500, 1);
                }
                std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> ev(new TEvBlobStorage::TEvCollectGarbage(
                    123, 3, 0, true, 2, 0, keep.release(), doNotKeep.release(), TInstant::Max()));

                ctx.Send(Proxy, ev.release());
                break;
            }
            case 10:
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGarbageCollectHuge(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyEmptyGet : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(9999/*tabletId*/, 0, 0, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessageGetResult, OK, 1, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(9999/*tabletId*/, 0, 0, 0, 3, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "xxx", TInstant::Max()));
                break;
            }
            case 20:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyEmptyGet(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};


class TTestBlobStorageProxyGarbageCollectComplex : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 0, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "test0", TInstant::Max()));
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 1, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "test1", TInstant::Max()));
                break;
            }
            case 20:
            {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 2, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "test2", TInstant::Max()));
                break;
            }
            case 30:
            {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT(" Sending ");
                std::unique_ptr<TVector<TLogoBlobID>> Keep;
                Keep.reset(new TVector<TLogoBlobID>);
                Keep->push_back(TLogoBlobID(1, 0, 0, 0, 5, 0));
                ctx.Send(Proxy, new TEvBlobStorage::TEvCollectGarbage(1, 0, 0, true, 0, 1, Keep.release(),
                    nullptr, TInstant::Max()));
                break;
            }
            case 40:
            {
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 0, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 50:
            {
                TEST_RESPONSE(MessageGetResult, OK, 1, "test0");

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 1, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 60:
            {
                // TODO: Find a way to wait for the full compaction to complete.
                //
                //TEST_RESPONSE(MessageGetResult, OK, 1, "");
                TEST_RESPONSE_3(MessageGetResult, OK, 1);

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 2, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 70:
            {
                TEST_RESPONSE(MessageGetResult, OK, 1, "test2");

                VERBOSE_COUT(" Sending TEvCollectGarbage");
                std::unique_ptr<TVector<TLogoBlobID>> DoNotKeep;
                DoNotKeep.reset(new TVector<TLogoBlobID>);
                DoNotKeep->push_back(TLogoBlobID(1, 0, 0, 0, 5, 0));
                ctx.Send(Proxy, new TEvBlobStorage::TEvCollectGarbage(1, 0, 0, false, 0, 0, nullptr,
                    DoNotKeep.release(), TInstant::Max()));
                break;
            }
            case 80:
            {
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 0, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 90:
            {
                // TODO: Find a way to wait for the full compaction to complete.
                //
                //TEST_RESPONSE(MessageGetResult, OK, 1, "");
                TEST_RESPONSE_3(MessageGetResult, OK, 1);

                VERBOSE_COUT(" Sending TEvCollectGarbage once again");
                std::unique_ptr<TVector<TLogoBlobID>> DoNotKeep;
                DoNotKeep.reset(new TVector<TLogoBlobID>);
                DoNotKeep->push_back(TLogoBlobID(1, 0, 0, 0, 5, 0));
                ctx.Send(Proxy, new TEvBlobStorage::TEvCollectGarbage(1, 0, 0, false, 0, 0, nullptr,
                    DoNotKeep.release(), TInstant::Max()));
                break;
            }
            case 100:
            {
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGarbageCollectComplex(const TActorId &proxy,
            const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyGarbageCollectAfterLargeData : public TTestBlobStorageProxy {
    TString Prepare(ui64 idx, ui64 size) {
        TString data;
        data.resize(size);
        ui8 *d = (ui8*)const_cast<char*>(data.data());
        for (ui64 i = 0; i < size; ++i) {
            d[i] = 'A' + ((idx + i) & 0xf);
        }
        return data;
    }
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 0, 0, 5, 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, "test0", TInstant::Max()));
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");
                TString largeData = Prepare(0, 32 << 20);

                VERBOSE_COUT(" Sending TEvPut");
                TLogoBlobID logoblobid(1, 0, 1, 0, largeData.size(), 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, largeData, TInstant::Max()));
                break;
            }
            case 20:
            {
                TEST_RESPONSE(MessagePutResult, ERROR, 0, "");

                VERBOSE_COUT(" Sending ");
                std::unique_ptr<TVector<TLogoBlobID>> Keep;
                Keep.reset(new TVector<TLogoBlobID>);
                Keep->push_back(TLogoBlobID(1, 0, 0, 0, 5, 0));
                ctx.Send(Proxy, new TEvBlobStorage::TEvCollectGarbage(1, 0, 0, true, 0, 1, Keep.release(),
                    nullptr, TInstant::Max()));
                break;
            }
            case 30:
            {
                TEST_RESPONSE(MessageCollectGarbageResult, OK, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
            {
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
            }
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyGarbageCollectAfterLargeData(const TActorId &proxy,
            const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};




class TTestBlobStorageProxySimpleDiscover : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        TString testData(
            "Server is a man with drinks, a click is a sound produced with fingers, a notebook is made of paper.");
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                TLogoBlobID logoblobid(1, 0, 0, 0, testData.size(), 0);
                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData, TInstant::Max()));
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                TLogoBlobID logoblobid(1, 0, 1, 0, testData2.size(), 0);
                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData2, TInstant::Max()));
                break;
            }
            case 20:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 30:
                TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 40:
                TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxySimpleDiscover(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyDiscover : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 10:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageDiscoverResult,
                        "Unexpected message type# " << (int)LastResponse.Message);

                if (LastResponse.Status != NKikimrProto::OK) {
                    VERBOSE_COUT("ReSending TEvDiscover (due to " << (int)LastResponse.Status << ")");
                    ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                    TestStep -= 10;
                    break;
                }

                TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 20:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageDiscoverResult,
                        "Unexpected message type# " << (int)LastResponse.Message);
                if (LastResponse.Status != NKikimrProto::OK) {
                    VERBOSE_COUT("ReSending TEvDiscover (due to " << (int)LastResponse.Status << ")");
                    ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                    TestStep -= 10;
                    break;
                }
                TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyDiscover(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyDiscoverFail : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 10:
                TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, "");

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 20:
                TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyDiscoverFail(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyDiscoverEmpty : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 10:
                TEST_RESPONSE(MessageDiscoverResult, NODATA, 0, "");

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 20:
                TEST_RESPONSE(MessageDiscoverResult, NODATA, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyDiscoverEmpty(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyDiscoverTimeout : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 10:
                TEST_RESPONSE(MessageDiscoverResult, TIMEOUT, 0, "");

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 20:
                TEST_RESPONSE(MessageDiscoverResult, TIMEOUT, 0, "");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyDiscoverTimeout(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyLongTailDiscoverPut : public TTestBlobStorageProxy {
    ui64 MsgIdx;
    ui64 Iteration;

    ui64 FindCookieFor(TLogoBlobID id, TVDiskID vDiskId, ui32 *outPartIdx) {
        ui32 cookie = 0;
        const ui32 blobSubgroupSize = BsInfo->Type.BlobSubgroupSize();
        while (true) {
            ++cookie;
            TLogoBlobID blobId(id.TabletID(), id.Generation(), id.Step(), id.Channel(), id.BlobSize(), cookie);
            TBlobStorageGroupInfo::TServiceIds vdisksSvc;
            TBlobStorageGroupInfo::TVDiskIds vdisksId;
            BsInfo->PickSubgroup(blobId.Hash(), &vdisksId, &vdisksSvc);
            for (ui32 idx = 0; idx < blobSubgroupSize; ++idx) {
                if (vdisksId[idx] == vDiskId) {
                    *outPartIdx = idx;
                    return cookie;
                }
            }
        }
    }

    void TestFSM(const TActorContext &ctx) {
        TString testData(
            "Server is a man with drinks, a click is a sound produced with fingers, a notebook is made of paper.");
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        TString testData3(
            "Test data for direct VDisk put operations 1234567890.");
        VERBOSE_COUT("Test step# " << TestStep << " Iteration# " << Iteration << " Line " << __LINE__);
        bool isFirstFallthrough = false;

        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                TLogoBlobID logoblobid(1, 0, 0, 0, testData.size(), 0);
                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData, TInstant::Max()));
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                TLogoBlobID logoblobid(1, 0, 1, 0, testData2.size(), 0);
                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData2, TInstant::Max()));
                break;
            }
            case 20:
                isFirstFallthrough = true;
                TestStep += 10;
                [[fallthrough]];
            case 30:
            {
                if (isFirstFallthrough) {
                    TEST_RESPONSE(MessagePutResult, OK, 0, "");
                } else {
                    TEST_RESPONSE(MessageVPutResult, OK, 0, "");
                }

                TBlobStorageGroupType type(ErasureSpecies);
                NKikimr::TDataPartSet partSet;
                TLogoBlobID from0(1, 0, 2 + Iteration/*step*/, 0, testData3.size(), 0);
                type.SplitData((TErasureType::ECrcMode)from0.CrcMode(), testData3, partSet);


                TVDiskID vDiskId(0, 1, 0, 0, 0);
                ui32 partIdx = 0;
                ui32 cookie = FindCookieFor(from0, vDiskId, &partIdx);
                if (partIdx >= BsInfo->Type.TotalPartCount()) {
                    partIdx = 0;
                }

                ui32 vDiskIdx = vDiskId.FailDomain * BsInfo->GetNumVDisksPerFailDomain() + vDiskId.VDisk;

                TLogoBlobID from(1, 0, 2 + Iteration/*step*/, 0, testData3.size(), cookie, partIdx + 1);
                VERBOSE_COUT(" Sending TEvVPut partId# " << (partIdx + 1) << " cookie# " << cookie);

                TAutoPtr<TEvBlobStorage::TEvVPut> vPut(
                    new TEvBlobStorage::TEvVPut(from, partSet.Parts[0].OwnedString, vDiskId, false, nullptr,
                                                TInstant::Max(), NKikimrBlobStorage::TabletLog));
                auto& msgId = *vPut->Record.MutableMsgQoS()->MutableMsgId();
                msgId.SetMsgId(MsgIdx);
                msgId.SetSequenceId(9990);
                ++MsgIdx;
                ctx.Send(Env->VDisks[vDiskIdx], vPut.Release());
                ++Iteration;
                if (Iteration < 100) {
                    return;
                }
                break;
            }

            case 40:
                MsgIdx = 0;
                TestStep += 10;
                [[fallthrough]];
            case 50:
            {
                TEST_RESPONSE(MessageVPutResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvVPut");
                TVDiskID vDiskId(0, 1, 0, 1, 0);
                TLogoBlobID from0(1, 0, 2 + Iteration/*step*/, 0, testData3.size(), 0);
                ui32 partIdx = 0;
                ui32 cookie = FindCookieFor(from0, vDiskId, &partIdx);
                if (partIdx >= BsInfo->Type.TotalPartCount()) {
                    partIdx = 1;
                }

                TBlobStorageGroupType type(ErasureSpecies);
                NKikimr::TDataPartSet partSet;
                TLogoBlobID from(1, 0, 2 + Iteration/*step*/, 0, testData3.size(), cookie, partIdx + 1);
                type.SplitData((TErasureType::ECrcMode)from.CrcMode(), testData3, partSet);


                TAutoPtr<TEvBlobStorage::TEvVPut> vPut(
                    new TEvBlobStorage::TEvVPut(from, partSet.Parts[0].OwnedString, vDiskId, false, nullptr,
                                                TInstant::Max(), NKikimrBlobStorage::TabletLog));
                auto& msgId = *vPut->Record.MutableMsgQoS()->MutableMsgId();
                msgId.SetMsgId(MsgIdx);
                msgId.SetSequenceId(9990);
                ++MsgIdx;
                ui32 vDiskIdx = vDiskId.FailDomain * BsInfo->GetNumVDisksPerFailDomain() + vDiskId.VDisk;
                ctx.Send(Env->VDisks[vDiskIdx], vPut.Release());
                ++Iteration;
                if (Iteration < 200) {
                    return;
                }
                break;
            }
            case 60:
                TEST_RESPONSE(MessageVPutResult, OK, 0, "");
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyLongTailDiscoverPut(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
        , MsgIdx(0)
        , Iteration(0)
    {}
};

class TTestBlobStorageProxyLongTailDiscover : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        TString testData(
            "Server is a man with drinks, a click is a sound produced with fingers, a notebook is made of paper.");
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        TString testData3(
            "Test data for direct VDisk put operations 1234567890.");
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 10:
                TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);


                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 20:
                TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyLongTailDiscover(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyPartialGet : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        TString testData(
            "Server is a man with drinks, a click is a sound produced with fingers, a notebook is made of paper.");
        TLogoBlobID logoblobid(1, 0, 0, 0, testData.size(), 0);
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData, TInstant::Max()));
                break;
            case 10:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");

                VERBOSE_COUT(" Sending TEvGet shift 7 size 11");
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 7, 11, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            case 20:
                TEST_RESPONSE(MessageGetResult, OK, 1, TString(testData, 7, 11));

                VERBOSE_COUT(" Sending TEvGet shift 0 size 11");
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 11, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            case 30:
                TEST_RESPONSE(MessageGetResult, OK, 1, TString(testData, 0, 11));

                VERBOSE_COUT(" Sending TEvGet shift 7 size 0");
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 7, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            case 40:
                TEST_RESPONSE(MessageGetResult, OK, 1, TString(testData, 7, testData.size() - 7));

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyPartialGet(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestEmptyRange : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);
                VERBOSE_COUT(" Sending TEvRange");
                // Get range.
                TLogoBlobID from(1, 7/*generation*/, 0, 0, 0, 0);
                TLogoBlobID to(1, 8/*generation*/, 0, 0, 0, 0);
                VERBOSE_COUT(" Sending TEvRange, expecting 0");
                ctx.Send(Proxy, new TEvBlobStorage::TEvRange(1, from, to, true, TInstant::Max()));
                break;
            }
            case 10:
                TEST_RESPONSE(MessageRangeResult, OK, 0, "");
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestEmptyRange(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};


class TTestBlobStorageProxyBasic1 : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        TString testData(
            "Server is a man with drinks, a click is a sound produced with fingers, a notebook is made of paper.");
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        bool isFirstFallthrough = false;
        switch (TestStep) {
            case 0:
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                if (IsProfilerEnabled) {
                    VERBOSE_COUT(" Sending TEvStartProfiler");
                    ctx.Send(MakeProfilerID(1), new TEvProfiler::TEvStart(0));
                    TestStep += 5;
                    return;
                }
                TestStep += 5;
                [[fallthrough]];
            case 5:
            {
                if (IsProfilerEnabled) {
                    UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageStartProfilerResult,
                        "Unexpected " << (int)LastResponse.Message);
                }
                // Get.
                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1/*tabletId*/, 0, 0, 0, testData.size(), 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                TestStep += 5;
                return;
            }
            case 10:
            {
                if (!Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessageGetResult, OK, 1, "");
                }
                // Put.
                TLogoBlobID logoblobid(1, 0, 0, 0, testData.size(), 0);
                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData, TInstant::Max()));
                break;
            }
            case 20:
            {
                if (Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessagePutResult, ERROR, 0, "");
                } else {
                    TEST_RESPONSE(MessagePutResult, OK, 0, "");
                }
                // Get again.
                TLogoBlobID logoblobid(1, 0, 0, 0, testData.size(), 0);
                VERBOSE_COUT(" Sending TEvGet");
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 30:
                isFirstFallthrough = true;
                [[fallthrough]];
            case 40:
            case 50:
            case 60:
            {
                if (isFirstFallthrough) {
                    if (!Env->ShouldBeUnwritable) {
                        TEST_RESPONSE(MessageGetResult, OK, 1, testData);
                    }
                } else {
                    if (Env->ShouldBeUnwritable) {
                        TEST_RESPONSE(MessagePutResult, ERROR, 0, "");
                    } else {
                        TEST_RESPONSE(MessagePutResult, OK, 0, "");
                    }
                }
                // Put.
                ui32 iteration = (TestStep - 30)/10;
                TLogoBlobID logoblobid(1, iteration + 3/*generation*/, 0, 0, testData.size(), 0);
                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData, TInstant::Max()));
                break;
            }
            case 70:
                isFirstFallthrough = true;
                [[fallthrough]];
            case 80:
            case 90:
            case 100:
            {
                ui32 iteration = (TestStep - 70)/10;
                if (isFirstFallthrough) {
                    if (Env->ShouldBeUnwritable) {
                        TEST_RESPONSE(MessagePutResult, ERROR, 0, "");
                    } else {
                        TEST_RESPONSE(MessagePutResult, OK, 0, "");
                    }
                } else {
                    if (!Env->ShouldBeUnwritable) {
                        TEST_RESPONSE(MessageGetResult, OK, 1, testData);
                    }
                }
                // Get.
                TLogoBlobID logoblobid(1, iteration + 3/*generation*/, 0, 0, testData.size(), 0);
                VERBOSE_COUT(" Sending TEvGet");
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 110:
            {
                if (!Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessageGetResult, OK, 1, testData);
                }
                // Get range.
                TLogoBlobID from(1, 7/*generation*/, 0, 0, 0, 0);
                TLogoBlobID to(1, 6/*generation*/, 0, 0, 0, 0);
                VERBOSE_COUT(" Sending TEvRange, expecting 1");
                ctx.Send(Proxy, new TEvBlobStorage::TEvRange(1, from, to, true, TInstant::Max()));
                break;
            }
            case 120:
            {
                if (!Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessageRangeResult, OK, 1, testData);
                }
                // Get range.
                TLogoBlobID from(1, 7/*generation*/, 0, 0, 0, 0);
                TLogoBlobID to(1, 2/*generation*/, 0, 0, 0, 0);
                VERBOSE_COUT(" Sending TEvRange, expecting 4");
                ctx.Send(Proxy, new TEvBlobStorage::TEvRange(1, from, to, true, TInstant::Max()));
                break;
            }
            case 130:
            {
                if (!Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessageRangeResult, OK, 4, testData);
                    for (int i = 0; i < 4; ++i) {
                        if (LastResponse.Data[i] != testData) {
                            ythrow TWithBackTrace<yexception>() << "Item " << i << " is '" << LastResponse.Data[i]
                                << "' instead of '" << testData << "'";
                        }
                    }
                }

                // Get range.
                TLogoBlobID from(1, 2/*generation*/, 0, 0, 0, 0);
                TLogoBlobID to(1, 7/*generation*/, 0, 0, TLogoBlobID::MaxBlobSize, 0);
                VERBOSE_COUT(" Sending TEvRange, expecting 4");
                ctx.Send(Proxy, new TEvBlobStorage::TEvRange(1, from, to, true, TInstant::Max()));
                break;
            }
            case 140:
            {
                if (!Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessageRangeResult, OK, 4, testData);
                    for (int i = 0; i < 4; ++i) {
                        if (LastResponse.Data[i] != testData) {
                            ythrow TWithBackTrace<yexception>() << "Item " << i << " is '" << LastResponse.Data[i]
                                << "' instead of '" << testData << "'";
                        }
                    }
                }

                // Get range.
                TLogoBlobID from(1, 7/*generation*/, 0, 0, 0, 0);
                TLogoBlobID to(1, 8/*generation*/, 0, 0, TLogoBlobID::MaxBlobSize, 0);
                VERBOSE_COUT(" Sending TEvRange, expecting 0");
                ctx.Send(Proxy, new TEvBlobStorage::TEvRange(1, from, to, true, TInstant::Max()));
                break;
            }
            case 150:
            {
                if (!Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessageRangeResult, OK, 0, "");
                }
                // Get range.
                TLogoBlobID from(1, 2/*generation*/, 0, 0, 0, 0);
                TLogoBlobID to(1, 3/*generation*/, 0, 0, TLogoBlobID::MaxBlobSize, 0);
                VERBOSE_COUT(" Sending TEvRange, expecting 1");
                ctx.Send(Proxy, new TEvBlobStorage::TEvRange(1, from, to, true, TInstant::Max()));
                break;
            }
            case 160:
                isFirstFallthrough = true;
                [[fallthrough]];
            case 170:
            case 180:
            case 190:
            {
                ui32 iteration = (TestStep - 160)/10;
                if (isFirstFallthrough) {
                    if (!Env->ShouldBeUnwritable) {
                        TEST_RESPONSE(MessageRangeResult, OK, 1, testData);
                    }
                } else {
                    //TEST_RESPONSE(MessageVGetResult, OK, 1, testData);
                    //ui32 idx = iteration - 1;
                }
                // Get 1 part
                ui32 domains = BsInfo->GetTotalFailDomainsNum();
                ui32 drives = Env->VDiskCount / domains;
                ui32 domainIdx = iteration / drives;
                ui32 driveIdx = iteration - domainIdx * drives;

                TVDiskID vDiskId(0, 1, 0, domainIdx, driveIdx);

                TLogoBlobID from(1, 3/*generation*/, 0, 0, 0, testData.size(), 1);
                TLogoBlobID to(1, 3/*generation*/, 0, 0, 0, testData.size(), TLogoBlobID::MaxPartId);
                auto x = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vDiskId,
                                                                        TInstant::Max(),
                                                                        NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                        TEvBlobStorage::TEvVGet::EFlags::None,
                                                                        {},
                                                                        from,
                                                                        to);
                VERBOSE_COUT(" Sending TEvVGet");
                ctx.Send(Env->VDisks[iteration], x.release());
                break;
            }
            case 200:
            {
                //TEST_RESPONSE(MessageVGetResult, OK, 1, testData);
                // Get again.
                TLogoBlobID logoblobid(1, 0, 0, 0, testData.size(), 0);
                VERBOSE_COUT(" Sending TEvGet shift 7 size 11");
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(logoblobid, 7, 11, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 210:
            {
                if (!Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessageGetResult, OK, 1, TString(testData, 7, 11));
                }
                // Put.
                TLogoBlobID logoblobid(1, 10/*generation*/, 0, 0, testData2.size(), 0);
                VERBOSE_COUT(" Sending TEvPut");
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, testData2, TInstant::Max()));
                break;
            }
            case 220:
                if (Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessagePutResult, ERROR, 0, "");
                } else {
                    TEST_RESPONSE(MessagePutResult, OK, 0, "");
                }
                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 230:
                if (Env->ShouldBeUndiscoverable) {
                    TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, nullptr);
                } else {
                    TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);
                }
                VERBOSE_COUT("Sending TEvDiscover, read body = false, expecting OK");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, false, false, TInstant::Max(), 0, true));
                break;
            case 240:
                if (Env->ShouldBeUndiscoverable) {
                    TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, 0);
                } else {
                    TEST_RESPONSE(MessageDiscoverResult, OK, 1, "");
                }
                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting NODATA");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(2/*tabletId*/, 0, true, false, TInstant::Max(), 0, true));
                break;
            case 250:
                if (Env->ShouldBeUndiscoverable) {
                    TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, 0);
                } else {
                    TEST_RESPONSE(MessageDiscoverResult, NODATA, 0, "");
                }
                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting OK + data");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 260:
                if (Env->ShouldBeUndiscoverable) {
                    TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, 0);
                } else {
                    TEST_RESPONSE(MessageDiscoverResult, OK, 1, testData2);
                }
                VERBOSE_COUT("Sending TEvDiscover, read body = false, expecting OK");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(1, 0, false, true, TInstant::Max(), 0, true));
                break;
            case 270:
                if (Env->ShouldBeUndiscoverable) {
                    TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, 0);
                } else {
                    TEST_RESPONSE(MessageDiscoverResult, OK, 1, "");
                }
                VERBOSE_COUT("Sending TEvDiscover, read body = true, expecting NODATA");
                ctx.Send(Proxy, new TEvBlobStorage::TEvDiscover(2/*tabletId*/, 0, true, true, TInstant::Max(), 0, true));
                break;
            case 280:
            {
                if (Env->ShouldBeUndiscoverable) {
                    TEST_RESPONSE(MessageDiscoverResult, ERROR, 0, 0);
                } else {
                    TEST_RESPONSE(MessageDiscoverResult, NODATA, 0, "");
                }
                VERBOSE_COUT(" Sending TEvPut");
                TString putData = TString::Uninitialized(1 << 18);
                memset(const_cast<char*>(putData.data()), 1, putData.size());

                TLogoBlobID logoblobid(1, 11, 0, 0, putData.size(), 0);
                ctx.Send(Proxy, new TEvBlobStorage::TEvPut(logoblobid, putData, TInstant::Max()));
                break;
            }
            case 290:
                if (Env->ShouldBeUnwritable) {
                    TEST_RESPONSE(MessagePutResult, ERROR, 0, "");
                } else {
                    TEST_RESPONSE(MessagePutResult, OK, 0, "");
                }
                if (IsProfilerEnabled) {
                    VERBOSE_COUT(" Sending TEvStopProfiler");
                    ctx.Send(MakeProfilerID(1), new TEvProfiler::TEvStop(0));
                    break;
                }
                [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
            case 300:
                if (IsProfilerEnabled) {
                    UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageStopProfilerResult,
                        "Unexpected " << (int)LastResponse.Message);
                    VERBOSE_COUT(LastResponse.Profile);
                }
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyBasic1(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestGetMultipart : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData2(
            "But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born..");
        switch (TestStep) {
            case 0:
            {
                UNIT_ASSERT_EQUAL_C(LastResponse.Message, TResponseData::MessageNone,
                    "Unexpected " << (int)LastResponse.Message);

                VERBOSE_COUT(" Sending TEvGet");
                TLogoBlobID logoblobid(1, 0, 0, 0, testData2.size(), 0);
                TArrayHolder<TEvBlobStorage::TEvGet::TQuery> Queries(new TEvBlobStorage::TEvGet::TQuery[2]);
                ui32 blobSize = testData2.size();
                Queries[0].Set(logoblobid, 0, blobSize / 4);
                Queries[1].Set(logoblobid, 3 * blobSize / 4, blobSize / 4);
                ctx.Send(Proxy, new TEvBlobStorage::TEvGet(Queries, 2, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead));
                break;
            }
            case 10:
            {
                ui32 blobSize = testData2.size();
                TString p1 = testData2.substr(0, blobSize / 4);
                TString p2 = testData2.substr(3 * blobSize / 4, blobSize / 4);
                TEST_RESPONSE(MessageGetResult, OK, 2, p1);
                UNIT_ASSERT_VALUES_EQUAL_C(LastResponse.Data[1], p2,
                    "Got '" << LastResponse.Data[1] << "' instead of '" << p2 << "'");

                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestGetMultipart(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

template <ui32 vDiskIdx>
class TTestVDiskCompacted : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        switch (TestStep) {
            case 0:
            {
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                TAutoPtr<TEvBlobStorage::TEvVCompact> vCompact(new TEvBlobStorage::TEvVCompact(
                    vDiskId, NKikimrBlobStorage::TEvVCompact::ASYNC));
                VERBOSE_COUT("Sending EvVCompact to vDiskIdx: " << vDiskIdx);
                ctx.Send(Env->VDisks[vDiskIdx], vCompact.Release());
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessageVCompactResult, OK, 0, "");
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                TAutoPtr<TEvBlobStorage::TEvVStatus> vStatus(new TEvBlobStorage::TEvVStatus(vDiskId));
                VERBOSE_COUT("Sending EvVStatus to vDiskIdx: " << vDiskIdx);
                ctx.Send(Env->VDisks[vDiskIdx], vStatus.Release());
                break;
            }
            case 20:
            {
                TEST_RESPONSE(MessageVStatusResult, OK, 0, "");
                if (LastResponse.LogoBlobsCompacted) {
                    VERBOSE_COUT("Done");
                    Env->DoneEvent.Signal();
                    break;
                }
                Sleep(TDuration::MilliSeconds(50));
                TVDiskID vDiskId(0, 1, 0, vDiskIdx , 0);
                TAutoPtr<TEvBlobStorage::TEvVStatus> vStatus(new TEvBlobStorage::TEvVStatus(vDiskId));
                VERBOSE_COUT("Sending EvVStatus to vDiskIdx: " << vDiskIdx);
                ctx.Send(Env->VDisks[vDiskIdx], vStatus.Release());
                return;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestVDiskCompacted(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyVPutVCollectVGetRace : public TTestBlobStorageProxy {
    void TestFSM(const TActorContext &ctx) {
        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TString testData("xxx");
        TVDiskID vDiskId(0, 1, 0, 0, 0);

        switch (TestStep) {
            case 0:
            {
                VERBOSE_COUT(" Sending TEvVPut");
                TBlobStorageGroupType type(ErasureSpecies);
                NKikimr::TDataPartSet partSet;
                TLogoBlobID logoblobid(1, 1, 1, 0, testData.size(), 3, 1);
                type.SplitData((TErasureType::ECrcMode)logoblobid.CrcMode(), testData, partSet);

                TAutoPtr<TEvBlobStorage::TEvVPut> x(
                    new TEvBlobStorage::TEvVPut(logoblobid, partSet.Parts[0].OwnedString, vDiskId, false,
                        nullptr, TInstant::Max(), NKikimrBlobStorage::AsyncBlob));
                auto& msgId = *x->Record.MutableMsgQoS()->MutableMsgId();
                msgId.SetMsgId(0);
                msgId.SetSequenceId(1);

                ctx.Send(Env->VDisks[0], x.Release());
                break;
            }
            case 10:
            {
                TEST_RESPONSE(MessageVPutResult, OK, 0, "");
                VERBOSE_COUT(" Sending TEvVGet");

                TLogoBlobID id(1, 1, 1, 0, testData.size(), 3);

                auto keep = std::make_unique<TVector<TLogoBlobID>>();
                keep->push_back(id);

                auto collect = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(
                    1, 1, 1,
                    /* channel */ 0,
                    /* collect */ true,
                    /* collect gen */ 1,
                    /* collect step */ 1,
                    /* hard */ false,
                    /* keep */ keep.get(),
                    /* donotkeep */ nullptr,
                    vDiskId,
                    TInstant::Max());
                collect->Record.MutableMsgQoS()->MutableMsgId()->SetMsgId(0);
                collect->Record.MutableMsgQoS()->MutableMsgId()->SetSequenceId(1);
                ctx.Send(Env->VDisks[0], collect.release());

                auto x = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId,
                                                                         TInstant::Max(),
                                                                         NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                         TEvBlobStorage::TEvVGet::EFlags::None,
                                                                         {},
                                                                         {id});

                auto& msgId = *x->Record.MutableMsgQoS()->MutableMsgId();
                msgId.SetMsgId(0);
                msgId.SetSequenceId(2);

                ctx.Send(Env->VDisks[0], x.release());
                break;
            }
            case 20:
            {
                TEST_RESPONSE(MessageVGetResult, OK, 1, testData);
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestBlobStorageProxyVPutVCollectVGetRace(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

TMaybe<TGroupStat::EKind> PutHandleClassToGroupStatKind(NKikimrBlobStorage::EPutHandleClass handleClass) {
    switch (handleClass) {
    case NKikimrBlobStorage::TabletLog:
        return  TGroupStat::EKind::PUT_TABLET_LOG;

    case NKikimrBlobStorage::UserData:
        return TGroupStat::EKind::PUT_USER_DATA;

    default:
        return {};
    }
}

class TTestBlobStorageProxyForRequest : public TTestBlobStorageProxy {
protected:
    void TestFSM(const TActorContext &ctx) override {
        if (!TestStep) {
            TIntrusivePtr<TDsProxyNodeMon> nodeMon = new TDsProxyNodeMon(NKikimr::AppData(ctx)->Counters, true);
            TString name = Sprintf("%09" PRIu32, 0);
            TIntrusivePtr<::NMonitoring::TDynamicCounters> group = GetServiceCounters(
                NKikimr::AppData(ctx)->Counters, "dsproxy")->GetSubgroup("blobstorageproxy", name);
            TIntrusivePtr<::NMonitoring::TDynamicCounters> percentileGroup = GetServiceCounters(
                    NKikimr::AppData(ctx)->Counters, "dsproxy_percentile")->GetSubgroup("blobstorageproxy", name);
            TIntrusivePtr<::NMonitoring::TDynamicCounters> overviewGroup = GetServiceCounters(
                    NKikimr::AppData(ctx)->Counters, "dsproxy_overview");
            Mon = new TBlobStorageGroupProxyMon(group, percentileGroup, overviewGroup, BsInfo, nodeMon, false);

            TIntrusivePtr<::NMonitoring::TDynamicCounters> DynCounters = new ::NMonitoring::TDynamicCounters();
            StoragePoolCounters = new NKikimr::TStoragePoolCounters(DynCounters, "", {});
            PerDiskStatsPtr = new TDiskResponsivenessTracker::TPerDiskStats;
        }
    }

    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TIntrusivePtr<NKikimr::TStoragePoolCounters> StoragePoolCounters;
    TDiskResponsivenessTracker::TPerDiskStatsPtr PerDiskStatsPtr;

    TTestBlobStorageProxyForRequest(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxy(proxy, bsInfo, env, parametrs)
    {}
};

class TTestBlobStorageProxyBatchedPutRequestDoesNotContainAHugeBlob : public TTestBlobStorageProxyForRequest {
    void TestFSM(const TActorContext &ctx) {
        TTestBlobStorageProxyForRequest::TestFSM(ctx);

        VERBOSE_COUT("Test step " << TestStep << " Line " << __LINE__);
        TVector<TLogoBlobID> blobIds = {
            TLogoBlobID(72075186224047637, 1, 863, 1, Data1.size(), 24576),
            TLogoBlobID(72075186224047637, 1, 2194, 1, Data2.size(), 12288)
        };

        switch (TestStep) {
            case 0: {
                TBatchedVec<TEvBlobStorage::TEvPut::TPtr> batched(2);
                batched[0] = GetPut(blobIds[0], Data1);
                batched[1] = GetPut(blobIds[1], Data2);

                TMaybe<TGroupStat::EKind> kind = PutHandleClassToGroupStatKind(HandleClass);
                IActor *reqActor = CreateBlobStorageGroupPutRequest(BsInfo, GroupQueues,
                        Mon, batched, false, PerDiskStatsPtr, kind,TInstant::Now(),
                        StoragePoolCounters, HandleClass, Tactic, false);

                ctx.Register(reqActor);
                break;
            }
            case 10:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");
                break;
            case 20:
                TEST_RESPONSE(MessagePutResult, OK, 0, "");
                VERBOSE_COUT("Done");
                Env->DoneEvent.Signal();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }

    TEvBlobStorage::TEvPut::TPtr GetPut(TLogoBlobID id, const TString &data) {
        std::unique_ptr<TEvBlobStorage::TEvPut> put = std::make_unique<TEvBlobStorage::TEvPut>(id, data,
                TInstant::Max(), HandleClass, Tactic);
        return static_cast<TEventHandle<TEvBlobStorage::TEvPut>*>(new IEventHandle(SelfId(), SelfId(), put.release()));
    }

    TEvBlobStorage::TEvPut::ETactic Tactic = TEvBlobStorage::TEvPut::TacticDefault;
    NKikimrBlobStorage::EPutHandleClass HandleClass = NKikimrBlobStorage::TabletLog;
    TString Data1;
    TString Data2;
public:
    TTestBlobStorageProxyBatchedPutRequestDoesNotContainAHugeBlob(const TActorId &proxy, const TIntrusivePtr<TBlobStorageGroupInfo> &bsInfo,
            const TIntrusivePtr<TTestEnvironment> &env, const TIntrusivePtr<ITestParametrs> &parametrs)
        : TTestBlobStorageProxyForRequest(proxy, bsInfo, env, parametrs)
    {
        Data1.resize(MaxBatchedPutSize - 1, 'a');
        Data2.resize(1, 'a');
    }
};

#define PROXY_UNIT_TEST(a) UNIT_TEST(a)

class TBlobStorageProxyTest: public TTestBase {
    UNIT_TEST_SUITE(TBlobStorageProxyTest);
        PROXY_UNIT_TEST(TestGetMultipart);
        PROXY_UNIT_TEST(TestGetFail);
        PROXY_UNIT_TEST(TestCollectGarbagePersistence);
        PROXY_UNIT_TEST(TestPersistence);
        PROXY_UNIT_TEST(TestDoubleEmptyGet);
        PROXY_UNIT_TEST(TestPartialGetBlock);
        PROXY_UNIT_TEST(TestPartialGetStripe);
        PROXY_UNIT_TEST(TestPartialGetMirror);
        PROXY_UNIT_TEST(TestBlock);
        PROXY_UNIT_TEST(TestBlockPersistence);
        PROXY_UNIT_TEST(TestGetAndRangeGetManyBlobs);
        PROXY_UNIT_TEST(TestInFlightPuts);
        PROXY_UNIT_TEST(TestCollectGarbage);
        PROXY_UNIT_TEST(TestHugeCollectGarbage);
        PROXY_UNIT_TEST(TestCollectGarbageAfterLargeData);
        PROXY_UNIT_TEST(TestNormal);
        PROXY_UNIT_TEST(TestDoubleGroups);
        PROXY_UNIT_TEST(TestQuadrupleGroups);
        PROXY_UNIT_TEST(TestSingleFailure);
        PROXY_UNIT_TEST(TestDoubleFailure);
        PROXY_UNIT_TEST(TestNormalMirror);
        PROXY_UNIT_TEST(TestSingleFailureMirror);
        PROXY_UNIT_TEST(TestDoubleFailureMirror3Plus2);
        PROXY_UNIT_TEST(TestDoubleFailureStripe4Plus2);
        PROXY_UNIT_TEST(TestProxySimpleDiscover);
        PROXY_UNIT_TEST(TestProxySimpleDiscoverNone);
        PROXY_UNIT_TEST(TestProxySimpleDiscoverMaxi);
        PROXY_UNIT_TEST(TestProxyGetSingleTimeout);
        PROXY_UNIT_TEST(TestProxyPutSingleTimeout);
        PROXY_UNIT_TEST(TestProxyPutInvalidSize);
        PROXY_UNIT_TEST(TestProxyDiscoverSingleTimeout);
        PROXY_UNIT_TEST(TestEmptyRange);
        PROXY_UNIT_TEST(TestPutGetMany);

        PROXY_UNIT_TEST(TestPutGetStatusErasureMirror3);
        PROXY_UNIT_TEST(TestPutGetStatusErasure3Plus1Block);
        PROXY_UNIT_TEST(TestPutGetStatusErasure3Plus1Stripe);
        PROXY_UNIT_TEST(TestPutGetStatusErasure4Plus2Block);
        PROXY_UNIT_TEST(TestPutGetStatusErasure3Plus2Block);
        PROXY_UNIT_TEST(TestPutGetStatusErasure4Plus2Stripe);
        PROXY_UNIT_TEST(TestPutGetStatusErasure3Plus2Stripe);
        PROXY_UNIT_TEST(TestPutGetStatusErasureMirror3Plus2);

        PROXY_UNIT_TEST(TestVPutVCollectVGetRace);

        PROXY_UNIT_TEST(TestVPutVGet);
        PROXY_UNIT_TEST(TestVPutVGetLimit);
        PROXY_UNIT_TEST(TestVPutVGetPersistence);
        PROXY_UNIT_TEST(TestVGetNoData);
        PROXY_UNIT_TEST(TestProxyLongTailDiscover);
        PROXY_UNIT_TEST(TestProxyLongTailDiscoverMaxi);
        PROXY_UNIT_TEST(TestProxyLongTailDiscoverSingleFailure);
        PROXY_UNIT_TEST(TestProxyRestoreOnDiscoverBlock);
        PROXY_UNIT_TEST(TestProxyRestoreOnGetBlock);
        PROXY_UNIT_TEST(TestProxyRestoreOnGetStripe);
        PROXY_UNIT_TEST(TestProxyRestoreOnGetMirror);
        PROXY_UNIT_TEST(TestProxyRestoreOnGetMirror3Plus2);
        PROXY_UNIT_TEST(TestVBlockVPutVGet);
        PROXY_UNIT_TEST(TestEmptyDiscover);
        PROXY_UNIT_TEST(TestEmptyDiscoverMaxi);
        PROXY_UNIT_TEST(TestCompactedGetMultipart);
        PROXY_UNIT_TEST(TestBatchedPutRequestDoesNotContainAHugeBlob);
//TODO: Think of another way to emulate VDisk errors
//        PROXY_UNIT_TEST(TestProxyGetDoubleTimeout);
//        PROXY_UNIT_TEST(TestProxyDiscoverDoubleTimeout);
//        PROXY_UNIT_TEST(TestProxyLongTailDiscoverDoubleFailure);
//        PROXY_UNIT_TEST(TestProxyPutDoubleTimeout);
    UNIT_TEST_SUITE_END();

    TIntrusivePtr<TTestEnvironment> LastTestEnv;
    TMap<TString, TIntrusivePtr<NPDisk::TSectorMap>> SectorMapByPath;
public:

    void TestGetFail() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyGetFail>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestVPutVGet() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyVPutVGet<1>>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestVPutVGetLimit() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyVPutVGetLimit<1>>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestVPutVGetPersistence() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        ui64 partId = 1;
        ui64 vDiskIdx = 1;
        TTestArgs args{0, erasureSpecies};
        args.Parametrs = new TTestBlobStorageProxyVPut::TParametrs(TestData2, partId, VDIM_TestIdx, vDiskIdx);
        TestBlobStorage<TTestBlobStorageProxyVPut>(tempDir().c_str(), args);
        static bool isNoData = false;
        TestBlobStorage<TTestBlobStorageProxyVGet<1, 1, isNoData>>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestVGetNoData() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyVGetFail<1>>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestEmptyRange() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestEmptyRange>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestProxyGetSingleTimeout() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyGet>(2, erasureSpecies, tempDir().c_str(), 0, false);
        SectorMapByPath.clear();
    }

    void TestProxyGetDoubleTimeout() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Block;
        TestBlobStorage<TTestBlobStorageProxyPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyGetTimeout>(6, erasureSpecies, tempDir().c_str(), 0, false);
        SectorMapByPath.clear();
    }

    void TestProxyPutSingleTimeout() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyPut>(2, erasureSpecies, nullptr, 0, false);
        SectorMapByPath.clear();
    }

    void TestProxyPutInvalidSize() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyPutInvalidSize>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }


    void TestProxyPutDoubleTimeout() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyPutFail>(6, erasureSpecies, nullptr, 0, false);
        SectorMapByPath.clear();
    }

    void TestProxyDiscoverSingleTimeout() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyDiscover>(2, erasureSpecies, tempDir().c_str(), 0, false);
        SectorMapByPath.clear();
    }

    void TestEmptyDiscover() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyDiscoverEmpty>(6, erasureSpecies, tempDir().c_str(), 0, false);
        SectorMapByPath.clear();
    }

    void TestEmptyDiscoverMaxi() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyDiscoverEmpty>(6, erasureSpecies, tempDir().c_str(), 0, false,
            8, 4);
        SectorMapByPath.clear();
    }

    void TestProxyDiscoverDoubleTimeout() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyDiscoverFail>(6, erasureSpecies, tempDir().c_str(), 0, false);
        SectorMapByPath.clear();
    }

    void TestProxyLongTailDiscoverSingleFailure() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscoverPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscover>(0, erasureSpecies, tempDir().c_str());
        for (int i = 0; i < 6; ++i) {
            TestBlobStorage<TTestBlobStorageProxyDiscover>(1 << i, erasureSpecies, tempDir().c_str());
        }
        SectorMapByPath.clear();
    }

    void TestProxyLongTailDiscoverDoubleFailure() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscoverPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscover>(0, erasureSpecies, tempDir().c_str());
        for (int i = 0; i < 4; ++i) {
            for (int j = i + 1; j < 5; ++j) {
                ui32 badDisksMask = (1 << i) | (1 << j);
                TestBlobStorage<TTestBlobStorageProxyDiscoverFail>(badDisksMask, erasureSpecies,
                    tempDir().c_str());
            }
        }
        SectorMapByPath.clear();
    }

    void TestProxyLongTailDiscover() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscoverPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscover>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestProxySimpleDiscover() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxySimpleDiscover>(0, erasureSpecies, nullptr);
        SectorMapByPath.clear();
    }

    void TestProxySimpleDiscoverNone() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::ErasureNone;
        TestBlobStorage<TTestBlobStorageProxySimpleDiscover>(0, erasureSpecies, nullptr);
        SectorMapByPath.clear();
    }

    void TestProxyLongTailDiscoverMaxi() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscoverPut>(0, erasureSpecies, tempDir().c_str(), 0, false, 8, 4);
        TestBlobStorage<TTestBlobStorageProxyLongTailDiscover>(0, erasureSpecies, tempDir().c_str(), 0, false, 8, 4);
        SectorMapByPath.clear();
    }

    void TestProxySimpleDiscoverMaxi() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxySimpleDiscover>(0, erasureSpecies, nullptr, 0, false, 8, 4);
        SectorMapByPath.clear();
    }

    void TestDoubleEmptyGet() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyEmptyGet>(0, erasureSpecies, nullptr);
        TestBlobStorage<TTestBlobStorageProxyEmptyGet>(0, erasureSpecies, nullptr);
        SectorMapByPath.clear();
    }

    void TestPersistence() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyPut>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyGet>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestBlock() {
        TestBlobStorage<TTestBlobStorageProxyBlock>(0, TBlobStorageGroupType::Erasure4Plus2Stripe,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestBlockPersistence() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestBlobStorageProxyBlockSet>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyBlockCheck>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestGetAndRangeGetManyBlobs() {
        TTempDir tempDir;
        TMersenne<ui64> prng(42);
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;

        constexpr int actionCount = 17'000;
        constexpr int startIndx = 1;

        static TVector<size_t> order(actionCount);
        Iota(order.begin(), order.end(), (size_t)startIndx);

        // Put
        Shuffle(order.begin(), order.end(), prng);
        TestBlobStorage<TTestIterativePut<order>>(0, erasureSpecies, tempDir().c_str());

        // Get
        Shuffle(order.begin(), order.end(), prng);
        TestBlobStorage<TTestOrderGet<order>>(0, erasureSpecies, tempDir().c_str());

        order.clear();

        // Forward range
        TestBlobStorage<TTestRangeGet<startIndx, actionCount>>(0, erasureSpecies, tempDir().c_str());

        // Backward range
        TestBlobStorage<TTestRangeGet<actionCount, -actionCount>>(0, erasureSpecies, tempDir().c_str());

        SectorMapByPath.clear();
    }

    void TestInFlightPuts() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure4Plus2Stripe;
        TestBlobStorage<TTestInFlightPuts<256, 1'000>>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestHugeCollectGarbage() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyGarbageCollectHuge>(0, erasureSpecies, nullptr);
        SectorMapByPath.clear();
    }

    void TestCollectGarbage() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyGarbageCollectComplex>(0, erasureSpecies, nullptr);
        SectorMapByPath.clear();
    }

    void TestCollectGarbageAfterLargeData() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TestBlobStorage<TTestBlobStorageProxyGarbageCollectAfterLargeData>(0, erasureSpecies, nullptr);
        SectorMapByPath.clear();
    }

    void TestCollectGarbagePersistence() {
        TTempDir tempDir;
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        VERBOSE_COUT("Phase 0");
        TestBlobStorage<TTestBlobStorageProxyPut>(0, erasureSpecies, tempDir().c_str());
        VERBOSE_COUT("Phase 1");
        TestBlobStorage<TTestBlobStorageProxyGarbageMark>(0, erasureSpecies, tempDir().c_str());
        VERBOSE_COUT("Phase 2");
        TestBlobStorage<TTestBlobStorageProxyGet>(0, erasureSpecies, tempDir().c_str());
        VERBOSE_COUT("Phase 3");
        TestBlobStorage<TTestBlobStorageProxyGarbageCollect>(0, erasureSpecies, tempDir().c_str());
        VERBOSE_COUT("Phase 4");
        TestBlobStorage<TTestBlobStorageProxyGet>(0, erasureSpecies, tempDir().c_str());
        VERBOSE_COUT("Phase 5");
        TestBlobStorage<TTestBlobStorageProxyGarbageUnmark>(0, erasureSpecies, tempDir().c_str());
        VERBOSE_COUT("Phase 6");
        TestBlobStorage<TTestBlobStorageProxyGarbageCollect>(0, erasureSpecies, tempDir().c_str());
        VERBOSE_COUT("Phase 7");
        // TODO: Find a way to wait for the full compaction to complete.
        //
        //TestBlobStorage<TTestBlobStorageProxyGetFail>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void RemoveVDiskData(ui32 vDiskIdx, TString directory) {
        TString databaseDirectory = Sprintf("%s/vdisk_%d", directory.c_str(), vDiskIdx);
        auto it = SectorMapByPath.begin();
        while (it != SectorMapByPath.end()) {
            auto next = it;
            ++next;
            if (it->first.StartsWith(databaseDirectory)) {
                SectorMapByPath.erase(it);
            }
            it = next;
        }
    }


    template <int vDiskIdx, int partId>
    void TestProxyRestoreOnGet(TBlobStorageGroupType::EErasureSpecies erasureSpecies) {
        TTempDir tempDir;

        TBlobStorageGroupType type(erasureSpecies);
        TTestArgs args{0, erasureSpecies};
        TIntrusivePtr<TTestBlobStorageProxyVPut::TParametrs> param =
                new TTestBlobStorageProxyVPut::TParametrs(TestData2, 0);
        args.Parametrs = param.Get();

        for (param->PartId = 1; param->PartId  <= type.TotalPartCount(); ++param->PartId ) {
            TestBlobStorage<TTestBlobStorageProxyVPut>(tempDir().c_str(), args);
        }

        static bool isNoData = false;
        TestBlobStorage<TTestBlobStorageProxyVGet<vDiskIdx, partId, isNoData>>(0, erasureSpecies, tempDir().c_str());
        if (isNoData) {
            SectorMapByPath.clear();
            return;
        }
        RemoveVDiskData(vDiskIdx, tempDir());
        //TestBlobStorage<TTestBlobStorageProxyVGetFail<vDiskIdx>>(0, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyGet>(0, erasureSpecies, tempDir().c_str());

        // Hands here
        TestBlobStorage<TTestBlobStorageProxyVGet<vDiskIdx, partId, isNoData>>(0, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestProxyRestoreOnDiscoverBlock() {
        TBlobStorageGroupType::EErasureSpecies erasureSpecies = TBlobStorageGroupType::Erasure3Plus1Block;
        TTempDir tempDir;
        const ui32 vDiskIdx = 2;
        const ui32 handoffVDiskIdx = 1;
        ui32 badDiskMask = 1 << handoffVDiskIdx;
        TestBlobStorage<TTestBlobStorageProxyPut>(badDiskMask, erasureSpecies, tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyBlockSet>(0, erasureSpecies, tempDir().c_str());
        badDiskMask = 1 << vDiskIdx;
        TestBlobStorage<TTestBlobStorageProxyVGetFail<handoffVDiskIdx>>(badDiskMask, erasureSpecies,
            tempDir().c_str());
        TestBlobStorage<TTestBlobStorageProxyDiscover>(badDiskMask, erasureSpecies, tempDir().c_str());
        static bool isNoData = false;
        TestBlobStorage<TTestBlobStorageProxyVGet<handoffVDiskIdx, 1, isNoData>>(badDiskMask, erasureSpecies,
            tempDir().c_str());
        UNIT_ASSERT(!isNoData);
        TestBlobStorage<TTestBlobStorageProxyBlockCheck>(badDiskMask, erasureSpecies, tempDir().c_str());
        SectorMapByPath.clear();
    }

    void TestProxyRestoreOnGetBlock() {
        TestProxyRestoreOnGet<2, 1>(TBlobStorageGroupType::Erasure3Plus1Block);
    }

    void TestProxyRestoreOnGetStripe() {
        TestProxyRestoreOnGet<2, 1>(TBlobStorageGroupType::Erasure3Plus1Stripe);
    }

    void TestProxyRestoreOnGetMirror() {
        TestProxyRestoreOnGet<2, 1>(TBlobStorageGroupType::ErasureMirror3);
    }

    void TestProxyRestoreOnGetMirror3Plus2() {
        TestProxyRestoreOnGet<2, 1>(TBlobStorageGroupType::ErasureMirror3Plus2);
    }

    void TestPartialGetBlock() {
        TestBlobStorage<TTestBlobStorageProxyPartialGet>(0, TBlobStorageGroupType::Erasure3Plus1Block,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestPartialGetStripe() {
        TestBlobStorage<TTestBlobStorageProxyPartialGet>(0, TBlobStorageGroupType::Erasure3Plus1Stripe,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestPartialGetMirror() {
        TestBlobStorage<TTestBlobStorageProxyPartialGet>(0, TBlobStorageGroupType::ErasureMirror3,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestPartialGetNone() {
        TestBlobStorage<TTestBlobStorageProxyPartialGet>(0, TBlobStorageGroupType::ErasureNone,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestNormal() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(0, TBlobStorageGroupType::Erasure4Plus2Block,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestSingleFailure() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(1, TBlobStorageGroupType::Erasure4Plus2Block,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestDoubleFailure() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(3, TBlobStorageGroupType::Erasure4Plus2Block,
            nullptr);
        SectorMapByPath.clear();
    }

    void TestDoubleGroups() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(0, TBlobStorageGroupType::Erasure4Plus2Stripe,
            nullptr, 0, true, 8, 2);
        SectorMapByPath.clear();
    }

    void TestTrippleGroups() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(0, TBlobStorageGroupType::Erasure4Plus2Stripe,
            nullptr, 0, true, 8, 3);
        SectorMapByPath.clear();
    }

    void TestQuadrupleGroups() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(0, TBlobStorageGroupType::Erasure4Plus2Stripe,
            nullptr, 0, true, 8, 4);
        SectorMapByPath.clear();
    }

    void TestNormalMirror() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(0, TBlobStorageGroupType::ErasureMirror3, nullptr);
        SectorMapByPath.clear();
    }

    void TestSingleFailureMirror() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(1, TBlobStorageGroupType::ErasureMirror3, nullptr);
        SectorMapByPath.clear();
    }

    void TestDoubleFailureMirror3Plus2() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(3, TBlobStorageGroupType::ErasureMirror3Plus2, nullptr);
        SectorMapByPath.clear();
    }

    void TestNormalNone() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(0, TBlobStorageGroupType::ErasureNone, nullptr);
        SectorMapByPath.clear();
    }

    void TestDoubleFailureStripe4Plus2() {
        TestBlobStorage<TTestBlobStorageProxyBasic1>(3, TBlobStorageGroupType::Erasure4Plus2Stripe, nullptr);
        SectorMapByPath.clear();
    }

    void TestGetMultipart() {
        TTempDir tempDir;
        TestBlobStorage<TTestBlobStorageProxyPut>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        TestBlobStorage<TTestGetMultipart>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        SectorMapByPath.clear();
    }

    void TestCompactedGetMultipart() {
        return;  // TODO KIKIMR-2244
        TTempDir tempDir;
        TestBlobStorage<TTestBlobStorageProxyPut>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        TestBlobStorage<TTestVDiskCompacted<0>>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        TestBlobStorage<TTestVDiskCompacted<1>>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        TestBlobStorage<TTestVDiskCompacted<2>>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        TestBlobStorage<TTestVDiskCompacted<3>>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        TestBlobStorage<TTestVDiskCompacted<4>>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        TestBlobStorage<TTestGetMultipart>(0, TBlobStorageGroupType::Erasure3Plus1Block, tempDir().data());
        SectorMapByPath.clear();
    }

    void TestVBlockVPutVGet() {
        TestBlobStorage<TTestBlobStorageProxyVBlockVPutVGet>(0, TBlobStorageGroupType::ErasureNone, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetMany() {
        TestBlobStorage<TTestBlobStorageProxyPutGetMany>(0, TBlobStorageGroupType::Erasure4Plus2Block, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasureMirror3() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::ErasureMirror3, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasure3Plus1Block() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::Erasure3Plus1Block, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasure3Plus1Stripe() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::Erasure3Plus1Stripe, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasure4Plus2Block() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::Erasure4Plus2Block, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasure3Plus2Block() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::Erasure3Plus2Block, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasure4Plus2Stripe() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::Erasure4Plus2Stripe, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasure3Plus2Stripe() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::Erasure3Plus2Stripe, nullptr);
        SectorMapByPath.clear();
    }

    void TestPutGetStatusErasureMirror3Plus2() {
        TestBlobStorage<TTestBlobStorageProxyPutGetStatus>(0, TBlobStorageGroupType::ErasureMirror3Plus2, nullptr);
        SectorMapByPath.clear();
    }

    void TestVPutVCollectVGetRace() {
        TestBlobStorage<TTestBlobStorageProxyVPutVCollectVGetRace>(0, TBlobStorageGroupType::ErasureNone, nullptr);
        SectorMapByPath.clear();
    }

    void TestBatchedPutRequestDoesNotContainAHugeBlob() {
        TTestArgs args{0, TBlobStorageGroupType::ErasureNone};
        args.DeviceType = NPDisk::DEVICE_TYPE_NVME;
        TestBlobStorage<TTestBlobStorageProxyBatchedPutRequestDoesNotContainAHugeBlob>(nullptr, args);
        SectorMapByPath.clear();
    }

    THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 nodeId, ::NMonitoring::TDynamicCounters &counters,
            TIntrusivePtr<TTableNameserverSetup> &nameserverTable, TInterconnectMock &interconnectMock) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = nodeId;
        setup->ExecutorsCount = 4;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[4]);
#if RT_EXECUTOR_POOL
        setup->Executors[0].Reset(new TRtExecutorPool(0, 1));
        setup->Executors[1].Reset(new TRtExecutorPool(1, 2));
        setup->Executors[2].Reset(new TRtExecutorPool(2, 10));
        setup->Executors[3].Reset(new TRtExecutorPool(3, 2));
#else
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 1, 20));
        setup->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20));
        setup->Executors[2].Reset(new TIOExecutorPool(2, 10));
        setup->Executors[3].Reset(new TBasicExecutorPool(3, 2, 20));
#endif
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

        setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, 0));

        ui64 nodeCount = nameserverTable->StaticNodeTable.size() + 1;
        setup->LocalServices.emplace_back(
            GetNameserviceActorId(),
            TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::ReadAsFilled, 0)
        );
        TIntrusivePtr<TInterconnectProxyCommon> icCommon = new TInterconnectProxyCommon();
        icCommon->NameserviceId = GetNameserviceActorId();
        icCommon->MonCounters = counters.GetSubgroup("counters", "interconnect");
        icCommon->TechnicalSelfHostName = "127.0.0.1";
        setup->Interconnect.ProxyActors.resize(nodeCount);
        //ui64 basePort = 12000;
        for (ui32 xnode : xrange<ui32>(1, nodeCount)) {
            if (xnode != nodeId) {
                //IActor *actor = new TInterconnectProxyTCP(xnode, icCommon);
                IActor *actor = interconnectMock.CreateProxyMock(nodeId, xnode, icCommon);
                setup->Interconnect.ProxyActors[xnode] = TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0);
            }
        }

        TActorSetupCmd profilerSetup(CreateProfilerActor(nullptr, "."), TMailboxType::Simple, 0);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeProfilerID(nodeId), std::move(profilerSetup)));

        const TActorId nameserviceId = GetNameserviceActorId();
        TActorSetupCmd nameserviceSetup(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(nameserviceId, std::move(nameserviceSetup)));

        return setup;
    }

    TIntrusivePtr<NActors::NLog::TSettings> AddLoggerActor(THolder<TActorSystemSetup> &setup,
            ::NMonitoring::TDynamicCounters &counters) {

        NActors::TActorId loggerActorId = NActors::TActorId(setup->NodeId, "logger");
        TIntrusivePtr<NActors::NLog::TSettings> logSettings(
            new NActors::NLog::TSettings(loggerActorId, NActorsServices::LOGGER,
                IsVerbose ? NLog::PRI_ERROR : NLog::PRI_CRIT,
                IsVerbose ? NLog::PRI_ERROR : NLog::PRI_CRIT,
                0));
        logSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );
        logSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );

        if (IsVerbose) {
            TString explanation;
            //logSettings->SetLevel(NLog::PRI_CRIT, NKikimrServices::BS_LOCALRECOVERY, explanation);
            //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_PROXY_COLLECT, explanation);
            logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_PROXY_DISCOVER, explanation);
            logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_PROXY_PUT, explanation);
            logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_PROXY_GET, explanation);
            logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_VDISK_GET, explanation);
            logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_VDISK_PUT, explanation);
            logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_SKELETON, explanation);
            logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_QUEUE, explanation);
            // logSettings->TimeThresholdMs = 5000;
            // logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_IFACE, explanation);
            //logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_LOCALRECOVERY, explanation);
        }
        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(logSettings,
                NActors::CreateStderrBackend(),
                counters.GetSubgroup("counters", "utils"));
        NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 2);
        std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(loggerActorId, std::move(loggerActorCmd));
        setup->LocalServices.push_back(std::move(loggerActorPair));

        return logSettings;
    }

    template <class T>
    void TestBlobStorage(ui64 badVDiskMask, TBlobStorageGroupType::EErasureSpecies erasureSpecies,
            const char* dir, ui32 sleepMilliseconds = 0, bool startBadDisks = true, ui32 failDomains = 0,
            ui32 drivesPerFailDomain = 0) {
        TTestArgs args;
        args.BadVDiskMask = badVDiskMask;
        args.ErasureSpecies = erasureSpecies;
        args.SleepMilliseconds = sleepMilliseconds;
        args.StartBadDisks = startBadDisks;
        args.FailDomainCount = failDomains;
        args.DrivesPerFailDomain = drivesPerFailDomain;
        TestBlobStorage<T>(dir, args);
    }

    template <class T>
    void TestBlobStorage(const char* dir, const TTestArgs &args = TTestArgs()) {
        TIntrusivePtr<TTestEnvironment> env = new TTestEnvironment(args);

        TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());
        TPortManager pm;
        nameserverTable->StaticNodeTable[1] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12001));
        nameserverTable->StaticNodeTable[2] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12002));

        TIntrusivePtr<TBlobStorageGroupInfo> bsInfo(new TBlobStorageGroupInfo(args.ErasureSpecies,
                env->DrivesPerFailDomain, env->FailDomainCount, 1, &env->VDisks));

        // first node
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters());

        TInterconnectMock interconnect;

        TAppData appData(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
        appData.Counters = counters;
        auto ioContext = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();
        appData.IoContextFactory = ioContext.get();

        THolder<TActorSystemSetup> setup1 = BuildActorSystemSetup(1, *counters, nameserverTable, interconnect);
        THolder<TActorSystemSetup> setup2 = BuildActorSystemSetup(2, *counters, nameserverTable, interconnect);

        TIntrusivePtr<TDsProxyNodeMon> dsProxyNodeMon(new TDsProxyNodeMon(counters, true));
        TDsProxyPerPoolCounters perPoolCounters(counters);
        TIntrusivePtr<TStoragePoolCounters> storagePoolCounters = perPoolCounters.GetPoolCounters("pool_name");
        std::unique_ptr<IActor> proxyActor{CreateBlobStorageGroupProxyConfigured(TIntrusivePtr(bsInfo), false,
            dsProxyNodeMon, TIntrusivePtr(storagePoolCounters), args.EnablePutBatching, DefaultEnableVPatch)};
        TActorSetupCmd bsproxySetup(proxyActor.release(), TMailboxType::Revolving, 3);
        setup1->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(env->ProxyId, std::move(bsproxySetup)));

        TTempDir tempDir;
        NPDisk::TKey mainKey = 123;
        NPDisk::TKey badMainKey = 124;
        for (ui32 failDomainIdx = 0; failDomainIdx < env->FailDomainCount; ++failDomainIdx) {
            for (ui32 driveIdx = 0; driveIdx < env->DrivesPerFailDomain; ++driveIdx) {
                ui32 i = env->GetVDiskTestIdx(failDomainIdx, driveIdx);
                bool isBad = (args.BadVDiskMask & (1 << i));
                if (!args.StartBadDisks && isBad) {
                    continue;
                }
                TString databaseDirectory = Sprintf(isBad ? "%s/vdisk_bad_%d" : "%s/vdisk_%d",
                    (dir ? dir : tempDir().c_str()), i);
                MakeDirIfNotExist(databaseDirectory.c_str());

                // Format a pDisk with an (in)valid key set
                ui64 pDiskGuid = 123;
                ui32 chunkSize = 32 << 20;
                ui64 diskSizeBytes = 32ull << 30ull;
                ui64 pDiskCategory = 0;
                TActorId pdiskId = env->PDisks[i];
                TString filePath = databaseDirectory + "/pdisk.dat";
                if (!SectorMapByPath[filePath]) {
                    SectorMapByPath[filePath].Reset(new NPDisk::TSectorMap(diskSizeBytes));
                    FormatPDisk(filePath, diskSizeBytes, 4 << 10, chunkSize, pDiskGuid,
                            0x123, 0x456, 0x789, isBad ? badMainKey : mainKey, "", false, false,
                            SectorMapByPath[filePath], false);
                }

                TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(filePath, pDiskGuid, i + 1, pDiskCategory);
                pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
                pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
                pDiskConfig->SectorMap = SectorMapByPath[filePath];
                pDiskConfig->EnableSectorEncryption = !pDiskConfig->SectorMap;

                NPDisk::TMainKey mainKeys = NPDisk::TMainKey{ .Keys = { mainKey }, .IsInitialized = true };
                TActorSetupCmd pDiskSetup(
                    CreatePDisk(pDiskConfig.Get(), mainKeys, counters),
                    TMailboxType::Revolving, 0);
                setup2->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(pdiskId, std::move(pDiskSetup)));

                TVDiskConfig::TBaseInfo baseInfo(
                    env->VDiskIds[i],
                    pdiskId,
                    mainKey,
                    i + 1,
                    args.DeviceType,
                    0,
                    NKikimrBlobStorage::TVDiskKind::Default,
                    2,
                    {});
                TIntrusivePtr<TVDiskConfig> vDiskConfig = new TVDiskConfig(baseInfo);
                vDiskConfig->MaxLogoBlobDataSize = 2u << 20u;
                vDiskConfig->HullCutLogDuration = TDuration::MilliSeconds(10);
                vDiskConfig->HullCompSchedulingInterval = TDuration::MilliSeconds(10);
                vDiskConfig->LevelCompaction = true;
                vDiskConfig->FreshCompaction = true;
                vDiskConfig->GCOnlySynced = false;
                vDiskConfig->HullCompLevelRateThreshold = 0.1;
                vDiskConfig->SkeletonFrontQueueBackpressureCheckMsgId = false;
                vDiskConfig->UseCostTracker = false;

                IActor* vDisk = CreateVDisk(vDiskConfig, bsInfo, counters);
                TActorSetupCmd vDiskSetup(vDisk, TMailboxType::Revolving, 0);
                setup2->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(env->VDisks[i], std::move(vDiskSetup)));
            }
        }

        TActorSetupCmd proxyTestSetup(new T(env->ProxyId, bsInfo.Get(), env, args.Parametrs),
                TMailboxType::Revolving, 0);
        setup1->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(env->ProxyTestId, std::move(proxyTestSetup)));

        //////////////////////////////////////////////////////////////////////////////
        GetServiceCounters(counters, "utils");
        TIntrusivePtr<NActors::NLog::TSettings> logSettings1 = AddLoggerActor(setup1, *counters);
        TIntrusivePtr<NActors::NLog::TSettings> logSettings2 = AddLoggerActor(setup2, *counters);

        //////////////////////////////////////////////////////////////////////////////
        std::unique_ptr<TActorSystem> actorSystem1;
        actorSystem1.reset(new TActorSystem(setup1, &appData, logSettings1));
        actorSystem1->Start();
        std::unique_ptr<TActorSystem> actorSystem2;
        actorSystem2.reset(new TActorSystem(setup2, &appData, logSettings2));
        actorSystem2->Start();
        EnableActorCallstack();
        try {
            VERBOSE_COUT("Sending TEvBoot to testproxy");
            actorSystem1->Send(env->ProxyTestId, new TEvTablet::TEvBoot(1, 0, nullptr, TActorId(),
                        nullptr));
            VERBOSE_COUT("Done");

            bool isDone = env->DoneEvent.Wait(TEST_TIMEOUT);
            UNIT_ASSERT_C(isDone, "test timeout, badVDiskMask = " << args.BadVDiskMask);

            if (isDone) {
                Sleep(TDuration::MilliSeconds(args.SleepMilliseconds));
            }

            for (ui32 i = 0; i != env->VDiskCount; ++i) {
                actorSystem2->Send(env->VDisks[i], new NActors::TEvents::TEvPoisonPill());
            }

            actorSystem1->Stop();
            actorSystem2->Stop();
            env->DoneEvent.Reset();
            DisableActorCallstack();
        } catch (yexception ex) {
            env->IsLastExceptionSet = true;
            env->LastException = ex;
            DisableActorCallstack();
            actorSystem1->Stop();
            actorSystem2->Stop();
        } catch (...) {
            DisableActorCallstack();
            actorSystem1->Stop();
            actorSystem2->Stop();
            throw;
        }
        if (env->IsLastExceptionSet) {
            env->IsLastExceptionSet = false;
            ythrow env->LastException;
        }
        LastTestEnv = env;
    }
};

UNIT_TEST_SUITE_REGISTRATION(TBlobStorageProxyTest);

} // namespace NKikimr
