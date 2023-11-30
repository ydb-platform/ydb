#include "service_actor.h"
#include "interval_gen.h"
#include "size_gen.h"
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/interconnect_channels.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/util/rope.h>

namespace NKikimr {

    namespace {

        class TVDiskLoadActor : public TActorBootstrapped<TVDiskLoadActor> {
            enum {
                EvTryToIssuePuts = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
                EvTryToCollect,
            };

            struct TEvTryToIssuePuts : public TEventLocal<TEvTryToIssuePuts, EvTryToIssuePuts>
            {};

            struct TEvTryToCollect : public TEventLocal<TEvTryToCollect, EvTryToCollect>
            {};

            const TActorId ParentActorId;
            const ui64 Tag;

            const TIntrusivePtr<TBlobStorageGroupInfo> Info;
            const TVDiskID VDiskId;
            const TActorId VDiskActorId;
            const TBlobStorageGroupType GType;
            TActorId QueueActorId;

            const ui64 TabletId;
            const ui32 Channel;
            const ui32 Generation;

            const ui64 DurationSeconds;
            const ui32 InFlightPutsMax;
            const ui64 InFlightPutBytesMax;

            const NKikimrBlobStorage::EPutHandleClass PutHandleClass;
            const ui32 StepDistance;

            TSizeGenerator PutSizeGenerator;
            TIntervalGenerator PutIntervalGenerator;
            TIntervalGenerator CollectIntervalGenerator;

            TInstant StartTime;

            bool IsConnected = false;

            ui32 CollectStep = 0;
            ui32 CollectCounter = 1;
            TInstant NextCollectRequestTimestamp;
            bool EvTryToCollectScheduled = false;

            ui32 BlobStep = 1;
            ui32 BlobCookie = 0;
            TInstant NextWriteRequestTimestamp;
            ui32 InFlightPuts = 0;
            ui32 TEvVPutsSent = 0;
            ui64 InFlightPutBytes = 0;
            ui64 BytesWritten = 0;
            TMap<ui64, ui32> InFlightRequests;
            ui64 PutCookie = 1;
            bool EvTryToIssuePutsScheduled = false;

            TDeque<TLogoBlobID> WrittenBlobs;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_LOAD_PDISK_WRITE;
            }

            TVDiskLoadActor(const NKikimr::TEvLoadTestRequest::TVDiskLoad& cmd,
                    const NActors::TActorId& parent, ui64 tag)
                : ParentActorId(parent)
                , Tag(tag)
                , Info(TBlobStorageGroupInfo::Parse(cmd.GetGroupInfo(), nullptr, nullptr))
                , VDiskId(VDiskIDFromVDiskID(cmd.GetVDiskId()))
                , VDiskActorId(Info->GetActorId(VDiskId))
                , GType(Info->Type)
                , TabletId(cmd.GetTabletId())
                , Channel(cmd.GetChannel())
                , Generation(cmd.GetGeneration())
                , DurationSeconds(cmd.GetDurationSeconds())
                , InFlightPutsMax(cmd.GetInFlightPutsMax())
                , InFlightPutBytesMax(cmd.GetInFlightPutBytesMax())
                , PutHandleClass(cmd.GetPutHandleClass())
                , StepDistance(cmd.GetStepDistance())
                , PutSizeGenerator(cmd.GetWriteSizes())
                , PutIntervalGenerator(cmd.GetWriteIntervals())
                , CollectIntervalGenerator(cmd.GetBarrierAdvanceIntervals())
            {
            }

            void Bootstrap(const TActorContext& ctx) {
                LOG_INFO(ctx, NKikimrServices::BS_LOAD_TEST, "Load actor starter, erasure# %s",
                        GType.ToString().data());
                Become(&TVDiskLoadActor::StateFunc);
                StartTime = TAppData::TimeProvider->Now();
                ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
                CreateQueueBackpressure(ctx);
            }

            void CreateQueueBackpressure(const TActorContext& ctx) {
                using namespace NBackpressure;
                auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

                NKikimrBlobStorage::EVDiskQueueId queueId = NKikimrBlobStorage::Unknown;
                switch (PutHandleClass) {
                    case NKikimrBlobStorage::TabletLog:
                        queueId = NKikimrBlobStorage::PutTabletLog;
                        break;
                    case NKikimrBlobStorage::AsyncBlob:
                        queueId = NKikimrBlobStorage::PutAsyncBlob;
                        break;
                    case NKikimrBlobStorage::UserData:
                        queueId = NKikimrBlobStorage::PutUserData;
                        break;
                }
                Y_ABORT_UNLESS(queueId != NKikimrBlobStorage::Unknown);

                TIntrusivePtr<TFlowRecord> flowRecord(new TFlowRecord);
                QueueActorId = ctx.Register(CreateVDiskBackpressureClient(
                    Info,
                    VDiskId,
                    queueId,
                    counters,
                    MakeIntrusive<TBSProxyContext>(counters),
                    TQueueClientId(EQueueClientType::VDiskLoad, TAppData::RandomProvider->GenRand64()),
                    "",
                    TInterconnectChannels::IC_BLOBSTORAGE,
                    ctx.ExecutorThread.ActorSystem->NodeId == VDiskActorId.NodeId(),
                    TDuration::Minutes(1),
                    flowRecord,
                    NMonitoring::TCountableBase::EVisibility::Public
                ));
            }

            void HandlePoison(const TActorContext& ctx) {
                ctx.Send(QueueActorId, new TEvents::TEvPoisonPill);
                ctx.Send(ParentActorId, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, "Poison pill"));
                Die(ctx);
            }

            void TryToIssuePuts(const TActorContext& ctx) {
                const TInstant now = TAppData::TimeProvider->Now();

                while (IsConnected &&
                        now >= NextWriteRequestTimestamp &&
                        InFlightPuts < InFlightPutsMax &&
                        InFlightPutBytes < InFlightPutBytesMax) {
                    TLogoBlobID logoBlobId;

                    if (WrittenBlobs && TAppData::RandomProvider->GenRandReal2() < 0.25) {
                        size_t index = TAppData::RandomProvider->GenRand64() % WrittenBlobs.size();
                        logoBlobId = WrittenBlobs[index];
                    } else {
                        const ui32 size = PutSizeGenerator.Generate();
                        logoBlobId = TLogoBlobID(TabletId, Generation, BlobStep, Channel, size, BlobCookie);
                        ++BlobCookie;
                        WrittenBlobs.push_back(logoBlobId);
                    }

                    logoBlobId = TLogoBlobID(logoBlobId, 1 + TAppData::RandomProvider->GenRand64() % GType.TotalPartCount());

                    IssuePutRequest(logoBlobId, PutCookie, ctx);
                    NextWriteRequestTimestamp = now + PutIntervalGenerator.Generate();
                    ++InFlightPuts;
                    InFlightPutBytes += logoBlobId.BlobSize();
                    InFlightRequests.emplace(PutCookie, logoBlobId.BlobSize());
                    ++PutCookie;
                }

                if (now < NextWriteRequestTimestamp && !EvTryToIssuePutsScheduled) {
                    const TDuration remain = NextWriteRequestTimestamp - now;
                    ctx.Schedule(remain, new TEvTryToIssuePuts);
                    EvTryToIssuePutsScheduled = true;
                }
            }

            void IssuePutRequest(const TLogoBlobID& logoBlobId, ui64 cookie, const TActorContext& ctx) {
                TSharedData data = TSharedData::Uninitialized(logoBlobId.BlobSize());
                std::fill_n(data.mutable_data(), logoBlobId.BlobSize(), 'X');
                TRope whole(MakeIntrusive<TRopeSharedDataBackend>(data));
                TDataPartSet parts;
                GType.SplitData((TErasureType::ECrcMode)logoBlobId.CrcMode(), whole, parts);
                auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(logoBlobId,
                        parts.Parts[logoBlobId.PartId() - 1].OwnedString, VDiskId, true, &cookie, TInstant::Max(), PutHandleClass);
                ctx.Send(QueueActorId, ev.release());
                ++TEvVPutsSent;
            }

            void HandleTryToIssuePuts(const TActorContext& ctx) {
                Y_ABORT_UNLESS(EvTryToIssuePutsScheduled);
                EvTryToIssuePutsScheduled = false;
                TryToIssuePuts(ctx);
            }

            void Handle(TEvBlobStorage::TEvVPutResult::TPtr& ev, const TActorContext& ctx) {
                TEvBlobStorage::TEvVPutResult *msg = ev->Get();
                const auto& record = msg->Record;

                auto it = InFlightRequests.find(record.GetCookie());
                Y_ABORT_UNLESS(it != InFlightRequests.end());
                const ui32 size = it->second;
                InFlightRequests.erase(it);

                --InFlightPuts;
                InFlightPutBytes -= size;
                if (record.GetStatus() == NKikimrProto::OK) {
                    BytesWritten += size;
                }

                TryToIssuePuts(ctx);
            }

            void TryToCollect(const TActorContext& ctx) {
                const TInstant now = TAppData::TimeProvider->Now();
                if (IsConnected && now >= NextCollectRequestTimestamp) {
                    if (CollectStep < BlobStep + StepDistance) {
                        const ui32 collectStep = CollectStep++;
                        auto ev = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(TabletId, Generation, CollectCounter++,
                                Channel, true, Generation, collectStep, false, nullptr, nullptr, VDiskId,
                                TInstant::Max());
                        ctx.Send(QueueActorId, ev.release());
                        NextCollectRequestTimestamp = now + CollectIntervalGenerator.Generate();

                        auto comp = [](ui32 step, const TLogoBlobID& logoBlobId) {
                            return step < logoBlobId.Step();
                        };
                        auto pos = std::upper_bound(WrittenBlobs.begin(), WrittenBlobs.end(), collectStep, comp);
                        WrittenBlobs.erase(WrittenBlobs.begin(), pos);
                    }
                    ++BlobStep;
                    BlobCookie = 0;
                }
                if (!EvTryToCollectScheduled) {
                    const TDuration remain = NextCollectRequestTimestamp - now;
                    ctx.Schedule(remain, new TEvTryToCollect);
                    EvTryToCollectScheduled = true;
                }
            }

            void HandleTryToCollect(const TActorContext& ctx) {
                Y_ABORT_UNLESS(EvTryToCollectScheduled);
                TryToCollect(ctx);
            }

            void Handle(TEvBlobStorage::TEvVCollectGarbageResult::TPtr& /*ev*/, const TActorContext& ctx) {
                Y_ABORT_UNLESS(EvTryToCollectScheduled);
                EvTryToCollectScheduled = false;
                TryToCollect(ctx);
            }

            void Handle(TEvProxyQueueState::TPtr& ev, const TActorContext& ctx) {
                IsConnected = ev->Get()->IsConnected;
                if (IsConnected) {
                    TryToIssuePuts(ctx);
                    TryToCollect(ctx);
                }
            }

            void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
                TStringStream str;

#define NAMED_PARAM(NAME, PARAM)            \
                TABLER() {                  \
                    TABLED() {              \
                        str << NAME;        \
                    }                       \
                    TABLED() {              \
                        str << PARAM;       \
                    }                       \
                }

#define PARAM(NAME)                    \
                TABLER() {               \
                    TABLED() {           \
                        str << #NAME;  \
                    }                  \
                    TABLED() {           \
                        str << NAME;   \
                    }                  \
                }

                HTML(str) {
                    TABLE() {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {
                                    str << "Parameter";
                                }
                                TABLEH() {
                                    str << "Value";
                                }
                            }
                        }
                        TABLEBODY() {
                            NAMED_PARAM("Elapsed time / Duration", (TAppData::TimeProvider->Now() - StartTime).Seconds()
                                    << "s / " << DurationSeconds << "s");
                            PARAM(TabletId)
                            PARAM(Channel)
                            PARAM(Generation)
                            PARAM(NextCollectRequestTimestamp)
                            PARAM(CollectStep)
                            PARAM(NextWriteRequestTimestamp)
                            PARAM(BlobStep)
                            PARAM(BlobCookie)
                            PARAM(InFlightPuts)
                            PARAM(InFlightPutBytes)
                            PARAM(PutCookie)
                            PARAM(IsConnected)
                            PARAM(VDiskId)
                            PARAM(VDiskActorId)
                            PARAM(BytesWritten)
                            PARAM(TEvVPutsSent)
                            TString avgSpeed = Sprintf("%.3lf %s", (double) BytesWritten / (1 << 20) /
                                    (TAppData::TimeProvider->Now() - StartTime).Seconds(), "MB/s");
                            NAMED_PARAM("Average speed", avgSpeed);
                        }
                    }
                }
#undef NAMED_PARAM
#undef PARAM
                ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
            }

            STRICT_STFUNC(StateFunc, {
                CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
                CFunc(EvTryToIssuePuts, HandleTryToIssuePuts);
                CFunc(EvTryToCollect, HandleTryToCollect);
                HFunc(TEvBlobStorage::TEvVPutResult, Handle);
                HFunc(TEvBlobStorage::TEvVCollectGarbageResult, Handle);
                HFunc(TEvProxyQueueState, Handle);
                HFunc(NMon::TEvHttpInfo, Handle);
            })
        };

    } // <anonymous>

    IActor *CreateVDiskWriterLoadTest(const NKikimr::TEvLoadTestRequest::TVDiskLoad& cmd,
            const NActors::TActorId& parent, ui64 tag) {
        return new TVDiskLoadActor(cmd, parent, tag);
    }

} // NKikimr
