#include "blobstorage_repl.h"
#include "blobstorage_replproxy.h"
#include "blobstorage_replbroker.h"
#include "blobstorage_hullrepljob.h"
#include "query_donor.h"
#include <ydb/library/actors/interconnect/watchdog_timer.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/vdisk/common/circlebuf.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/queue.h>
#include <util/generic/deque.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Internal Repl messages
    ////////////////////////////////////////////////////////////////////////////
    TEvReplFinished::TInfo::TInfo()
        : Start(TAppData::TimeProvider->Now())
        , End()
        , KeyPos()
        , Eof(false)
    {}

    TEvReplFinished::TInfo::~TInfo()
    {}

    ////////////////////////////////////////////////////////////////////////////
    // TEvReplFinished::TInfo
    ////////////////////////////////////////////////////////////////////////////
    TString TEvReplFinished::TInfo::ToString() const {
        return TStringBuilder()
            << "{KeyPos# " << KeyPos
            << " Eof# " << (Eof ? "true" : "false")
            << " Items# " << Items()
            << "}";
    }

    void TEvReplFinished::TInfo::OutputHtml(IOutputStream &str) const {
#define PARAM(NAME, VALUE) TABLER() { \
            TABLED() { str << #NAME; } \
            TABLED() { str << VALUE; } \
        }

#define PARAM_V(NAME) PARAM(NAME, NAME)

#define GROUP(NAME) TABLER() TABLED_ATTRS({{"colspan", "2"}}) COLLAPSED_BUTTON_CONTENT(CreateGuidAsString(), NAME) TABLE()

        HTML(str) {
            SMALL() {
                TABLE() {
                    TABLER() {
                        TABLEH() { str << "Parameter"; }
                        TABLEH() { str << "Value"; }
                    }
                    STRONG() {
                        PARAM(Summary, ItemsRecovered << "/" << ItemsPlanned << "/" << ItemsTotal);
                    }
                    PARAM(Start, ToStringLocalTimeUpToSeconds(Start));
                    PARAM(End, ToStringLocalTimeUpToSeconds(End));
                    PARAM(Duration, End - Start);
                    PARAM_V(KeyPos);
                    PARAM_V(Eof);
                    PARAM_V(UnrecoveredNonphantomBlobs);
                    PARAM_V(DonorVDiskId);
                    PARAM_V(DropDonor);
                    GROUP("Plan Generation Stats") {
                        PARAM_V(ItemsTotal);
                        PARAM_V(ItemsPlanned);
                        PARAM_V(WorkUnitsTotal);
                        PARAM_V(WorkUnitsPlanned);
                    }
                    GROUP("Plan Execution Stats") {
                        PARAM_V(ItemsRecovered);
                        PARAM_V(ItemsNotRecovered);
                        PARAM_V(ItemsException);
                        PARAM_V(ItemsPartiallyRecovered);
                        PARAM_V(ItemsPhantom);
                        PARAM_V(ItemsNonPhantom);
                        PARAM_V(WorkUnitsPerformed);
                    }
                    GROUP("Detailed Stats") {
                        PARAM_V(BytesRecovered);
                        PARAM_V(LogoBlobsRecovered);
                        PARAM_V(HugeLogoBlobsRecovered);
                        PARAM_V(ChunksWritten);
                        PARAM_V(SstBytesWritten);
                        PARAM_V(MetadataBlobs);
                    }
                    GROUP("Durations") {
                        PARAM_V(PreparePlanDuration);
                        PARAM_V(TokenWaitDuration);
                        PARAM_V(ProxyWaitDuration);
                        PARAM_V(MergeDuration);
                        PARAM_V(PDiskDuration);
                        PARAM_V(CommitDuration);
                        PARAM_V(OtherDuration);
                        PARAM_V(PhantomDuration);
                    }
                    GROUP("VDisk Stats") {
                        PARAM_V(ProxyStat->VDiskReqs);
                        PARAM_V(ProxyStat->VDiskRespOK);
                        PARAM_V(ProxyStat->VDiskRespRACE);
                        PARAM_V(ProxyStat->VDiskRespERROR);
                        PARAM_V(ProxyStat->VDiskRespDEADLINE);
                        PARAM_V(ProxyStat->VDiskRespOther);
                        PARAM_V(ProxyStat->LogoBlobGotIt);
                        PARAM_V(ProxyStat->LogoBlobNoData);
                        PARAM_V(ProxyStat->LogoBlobNotOK);
                        PARAM_V(ProxyStat->LogoBlobDataSize);
                        PARAM_V(ProxyStat->OverflowedMsgs);
                    }
                }
            }
        }
#undef GROUP
#undef PARAM_V
#undef PARAM
    }



    ////////////////////////////////////////////////////////////////////////////
    // TReplScheduler
    ////////////////////////////////////////////////////////////////////////////
    class TReplScheduler : public TActorBootstrapped<TReplScheduler> {
        enum {
            EvResumeForce = EventSpaceBegin(TEvents::ES_PRIVATE),
        };
        struct TEvResumeForce : TEventLocal<TEvResumeForce, EvResumeForce> {};

        typedef TCircleBuf<TEvReplFinished::TInfoPtr> THistory;
        enum EState {
            Plan,
            AwaitToken,
            WaitQueues,
            Replication,
            Relaxation,
            Finished
        };
        static const unsigned HistorySize = 12;

        struct TDonorQueueItem {
            TVDiskID VDiskId;
            TActorId QueueActorId;
            ui32 NodeId;
            ui32 PDiskId;
            ui32 VSlotId;
        };

        std::shared_ptr<TReplCtx> ReplCtx;
        ui32 NextMinREALHugeBlobInBytes;
        THistory History;
        EState State;
        TInstant LastReplStart;
        TInstant LastReplEnd;
        TInstant LastReplQuantumStart;
        TInstant LastReplQuantumEnd;
        TQueueActorMapPtr QueueActorMapPtr;
        TBlobIdQueuePtr BlobsToReplicatePtr;
        TBlobIdQueuePtr UnreplicatedBlobsPtr = std::make_shared<TBlobIdQueue>();
        TUnreplicatedBlobRecords UnreplicatedBlobRecords;
        TMilestoneQueue MilestoneQueue;
        TActorId ReplJobActorId;
        std::list<std::optional<TDonorQueueItem>> DonorQueue;
        std::deque<std::pair<TVDiskID, TActorId>> Donors;
        std::set<TVDiskID> ConnectedPeerDisks, ConnectedDonorDisks;
        TEvResumeForce *ResumeForceToken = nullptr;
        TInstant ReplicationEndTime;
        bool UnrecoveredNonphantomBlobs = false;

        TWatchdogTimer<TEvReplCheckProgress> ReplProgressWatchdog;

        friend class TActorBootstrapped<TReplScheduler>;

        const char *StateToStr(EState state) {
            switch (state) {
                case Plan:        return "Plan";
                case AwaitToken:  return "AwaitToken";
                case WaitQueues:  return "WaitQueues";
                case Replication: return "Replication";
                case Relaxation:  return "Relaxation";
                case Finished:    return "Finished";
                default:          return "Unknown";
            }
        }

        void Transition(EState current, EState next) {
            Y_ABORT_UNLESS(State == current, "State# %s Expected# %s", StateToStr(State), StateToStr(current));
            State = next;
        }

        void Bootstrap() {
            QueueActorMapPtr = std::make_shared<TQueueActorMap>();
            auto replInterconnectChannel = static_cast<TInterconnectChannels::EInterconnectChannels>(
                    ReplCtx->VDiskCfg->ReplInterconnectChannel);

            const TBlobStorageGroupInfo::TTopology& topology = ReplCtx->GInfo->GetTopology();
            NBackpressure::TQueueClientId replQueueClientId(NBackpressure::EQueueClientType::ReplJob,
                        topology.GetOrderNumber(ReplCtx->VCtx->ShortSelfVDisk));
            CreateQueuesForVDisks(*QueueActorMapPtr, SelfId(), ReplCtx->GInfo, ReplCtx->VCtx,
                    ReplCtx->GInfo->GetVDisks(), ReplCtx->MonGroup.GetGroup(),
                    replQueueClientId, NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead,
                    "PeerRepl", replInterconnectChannel, false);

            for (const auto& [vdiskId, vdiskActorId] : ReplCtx->VDiskCfg->BaseInfo.DonorDiskIds) {
                TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord(new NBackpressure::TFlowRecord);
                auto info = MakeIntrusive<TBlobStorageGroupInfo>(ReplCtx->GInfo, vdiskId, vdiskActorId);
                const TActorId queueActorId = Register(CreateVDiskBackpressureClient(info, vdiskId,
                    NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead, ReplCtx->MonGroup.GetGroup(), ReplCtx->VCtx,
                    NBackpressure::TQueueClientId(NBackpressure::EQueueClientType::ReplJob, 0), "Donor",
                    ReplCtx->VDiskCfg->ReplInterconnectChannel, vdiskActorId.NodeId() == SelfId().NodeId(),
                    TDuration::Minutes(1), flowRecord, NMonitoring::TCountableBase::EVisibility::Private));
                ui32 nodeId, pdiskId, vslotId;
                std::tie(nodeId, pdiskId, vslotId) = DecomposeVDiskServiceId(vdiskActorId);
                DonorQueue.emplace_back(TDonorQueueItem{
                    .VDiskId = vdiskId,
                    .QueueActorId = queueActorId,
                    .NodeId = nodeId,
                    .PDiskId = pdiskId,
                    .VSlotId = vslotId
                });
                Donors.emplace_back(vdiskId, queueActorId);
            }
            DonorQueue.emplace_back(std::nullopt); // disks from group

            if (ReplCtx->PausedAtStart) {
                Become(&TThis::StateRelax);
                State = Relaxation;
            } else {
                StartReplication();
            }
        }

        void Handle(TEvProxyQueueState::TPtr ev) {
            const TVDiskID& vdiskId = ev->Get()->VDiskId;
            auto& set = vdiskId.GroupGeneration == ReplCtx->GInfo->GroupGeneration
                ? ConnectedPeerDisks
                : ConnectedDonorDisks;
            if (ev->Get()->IsConnected) {
                set.insert(vdiskId);
            } else {
                set.erase(vdiskId);
            }
            if (ResumeForceToken) { // if the process is blocked by number of available queues, try to kick it
                ResumeIfReady();
            }
        }

        void Handle(TEvMinHugeBlobSizeUpdate::TPtr ev) {
            NextMinREALHugeBlobInBytes = ev->Get()->MinREALHugeBlobInBytes;
        }

        void StartReplication() {
            STLOG(PRI_DEBUG, BS_REPL, BSVR14, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "REPL START"));
            STLOG(PRI_DEBUG, BS_REPL, BSVR15, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "QUANTUM START"));

            LastReplStart = TAppData::TimeProvider->Now();
            ReplCtx->MonGroup.ReplUnreplicatedVDisks() = 1;
            ReplCtx->MonGroup.ReplWorkUnitsRemaining() = 0;
            ReplCtx->MonGroup.ReplWorkUnitsDone() = 0;
            ReplCtx->MonGroup.ReplItemsRemaining() = 0;
            ReplCtx->MonGroup.ReplItemsDone() = 0;
            Y_ABORT_UNLESS(NextMinREALHugeBlobInBytes);
            ReplCtx->MinREALHugeBlobInBytes = NextMinREALHugeBlobInBytes;
            UnrecoveredNonphantomBlobs = false;

            Become(&TThis::StateRepl);
            ResetReplProgressTimer(false);

            // switch to planning state
            Transition(Relaxation, Plan);

            RunRepl(TLogoBlobID());
        }

        void Handle(TEvReplStarted::TPtr& ev) {
            Y_ABORT_UNLESS(ReplJobActorId == ev->Sender);

            switch (State) {
                case Plan:
                    // this is a first quantum of replication, so we have to register it in the broker
                    State = AwaitToken;
                    if (!Send(MakeBlobStorageReplBrokerID(), new TEvQueryReplToken(ReplCtx->VDiskCfg->BaseInfo.PDiskId))) {
                        HandleReplToken();
                    }
                    break;

                case Replication:
                    // this is successive quantum, so we just continue as we have the token
                    Send(ev->Sender, new TEvReplResume);
                    break;

                default:
                    Y_ABORT("unexpected State# %s", StateToStr(State));
            }
        }

        void HandleReplToken() {
            // switch to replication state
            Transition(AwaitToken, Replication);
            if (!ResumeIfReady()) {
                Transition(Replication, WaitQueues);
                Schedule(TDuration::Seconds(5), ResumeForceToken = new TEvResumeForce);
            }
        }

        bool ResumeIfReady() {
            const auto& donor = DonorQueue.front();
            const bool ready = donor
                ? ConnectedDonorDisks.count(donor->VDiskId)
                : ConnectedPeerDisks.size() >= ReplCtx->GInfo->Type.DataParts();
            if (ready) {
                if (State != Replication) {
                    Transition(WaitQueues, Replication);
                }
                Send(ReplJobActorId, new TEvReplResume);
                ResumeForceToken = nullptr;
            }
            return ready;
        }

        void Handle(TEvResumeForce::TPtr ev) {
            if (ev->Get() == ResumeForceToken) {
                Transition(WaitQueues, Replication);
                Send(ReplJobActorId, new TEvReplResume);
                ResumeForceToken = nullptr;
            }
        }

        void DropDonor(const TDonorQueueItem& donor) {
            Donors.erase(std::find(Donors.begin(), Donors.end(), std::make_pair(donor.VDiskId, donor.QueueActorId)));
            Send(donor.QueueActorId, new TEvents::TEvPoison); // kill the queue actor
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvBlobStorage::TEvDropDonor(donor.NodeId,
                donor.PDiskId, donor.VSlotId, donor.VDiskId));
        }

        void Handle(TEvReplFinished::TPtr &ev) {
            Y_ABORT_UNLESS(ev->Sender == ReplJobActorId);
            ReplJobActorId = {};

            // replication can be finished only from the following states
            Y_ABORT_UNLESS(State == Plan || State == Replication, "State# %s", StateToStr(State));

            TEvReplFinished *msg = ev->Get();
            TEvReplFinished::TInfoPtr info = msg->Info;
            TInstant now = TAppData::TimeProvider->Now();
            STLOG(PRI_DEBUG, BS_REPL, BSVR16, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "QUANTUM COMPLETED"), (Info, *info));
            LastReplQuantumEnd = now;

            UnrecoveredNonphantomBlobs |= info->UnrecoveredNonphantomBlobs;
            UnreplicatedBlobRecords = std::move(info->UnreplicatedBlobRecords);
            MilestoneQueue = std::move(info->MilestoneQueue);

            if (info->ItemsRecovered > 0) {
                ResetReplProgressTimer(false);
            }

            bool finished = false;

            if (info->Eof) { // when it is the last quantum for some donor, rotate the blob sets
                BlobsToReplicatePtr = std::exchange(UnreplicatedBlobsPtr, std::make_shared<TBlobIdQueue>());

#ifndef NDEBUG
                Y_VERIFY_DEBUG_S(BlobsToReplicatePtr->GetNumItems() == UnreplicatedBlobRecords.size(),
                    "BlobsToReplicatePtr->size# " << BlobsToReplicatePtr->GetNumItems()
                    << " UnreplicatedBlobRecords.size# " << UnreplicatedBlobRecords.size());
                for (const TLogoBlobID& id : BlobsToReplicatePtr->Queue) {
                    Y_DEBUG_ABORT_UNLESS(UnreplicatedBlobRecords.contains(id));
                }
#endif

                if (BlobsToReplicatePtr->IsEmpty()) {
                    // no more blobs to replicate -- consider replication finished
                    finished = true;
                    for (const auto& donor : std::exchange(DonorQueue, {})) {
                        if (donor) {
                            DropDonor(*donor);
                        }
                    }
                }
                if (!finished) {
                    if (info->DropDonor) {
                        Y_ABORT_UNLESS(!DonorQueue.empty() && DonorQueue.front());
                        DropDonor(*DonorQueue.front());
                        DonorQueue.pop_front();
                    } else {
                        finished = !DonorQueue.front();
                        DonorQueue.splice(DonorQueue.end(), DonorQueue, DonorQueue.begin()); // move first item to the end
                    }
                }
            }

            History.Push(info);

            TDuration timeRemaining;

            if (finished) {
                STLOG(PRI_DEBUG, BS_REPL, BSVR17, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "REPL COMPLETED"),
                    (BlobsToReplicate, BlobsToReplicatePtr->GetNumItems()));
                LastReplEnd = now;

                if (State == WaitQueues || State == Replication) {
                    // release token as we have finished replicating
                    Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);
                }
                ResetReplProgressTimer(true);

                Become(&TThis::StateRelax);
                if (!BlobsToReplicatePtr->IsEmpty()) {
                    // try again for unreplicated blobs in some future
                    State = Relaxation;
                    Schedule(ReplCtx->VDiskCfg->ReplTimeInterval, new TEvents::TEvWakeup);
                    if (!UnrecoveredNonphantomBlobs) {
                        // semi-finished replication -- we have only phantom-like unreplicated blobs
                        TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvReplDone, 0, ReplCtx->SkeletonId,
                            SelfId(), nullptr, 1));
                    }
                } else {
                    // no more blobs to replicate; replication will not resume
                    State = Finished;
                    ReplCtx->MonGroup.ReplUnreplicatedVDisks() = 0;
                    ReplCtx->MonGroup.ReplUnreplicatedPhantoms() = 0;
                    ReplCtx->MonGroup.ReplUnreplicatedNonPhantoms() = 0;
                    ReplCtx->MonGroup.ReplWorkUnitsRemaining() = 0;
                    ReplCtx->MonGroup.ReplWorkUnitsDone() = 0;
                    ReplCtx->MonGroup.ReplItemsRemaining() = 0;
                    ReplCtx->MonGroup.ReplItemsDone() = 0;
                    TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvReplDone, 0, ReplCtx->SkeletonId,
                        SelfId(), nullptr, 0));
                }
            } else {
                STLOG(PRI_DEBUG, BS_REPL, BSVR18, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "QUANTUM START"));
                RunRepl(info->KeyPos);
                timeRemaining = EstimateTimeOfArrival();
            }

            ReplCtx->MonGroup.ReplSecondsRemaining() = timeRemaining.Seconds();
            ReplicationEndTime = finished ? TInstant::Zero() : TInstant::Now() + timeRemaining;
        }

        void RunRepl(const TLogoBlobID& from) {
            LastReplQuantumStart = TAppData::TimeProvider->Now();
            Y_ABORT_UNLESS(!ReplJobActorId);
            const auto& donor = DonorQueue.front();
            STLOG(PRI_DEBUG, BS_REPL, BSVR32, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "TReplScheduler::RunRepl"),
                (From, from), (Donor, donor ? TString(TStringBuilder() << "{VDiskId# " << donor->VDiskId << " VSlotId# " <<
                donor->NodeId << ":" << donor->PDiskId << ":" << donor->VSlotId << "}") : "generic"));
            ReplJobActorId = Register(CreateReplJobActor(ReplCtx, SelfId(), from, QueueActorMapPtr,
                BlobsToReplicatePtr, UnreplicatedBlobsPtr, donor ? std::make_optional(std::make_pair(
                donor->VDiskId, donor->QueueActorId)) : std::nullopt, std::move(UnreplicatedBlobRecords),
                std::move(MilestoneQueue)));
        }

        template<typename Iter>
        void OutputRow(IOutputStream &str, Iter &it, Iter &e) {
            HTML(str) {
                TABLER() {
                    for (unsigned i = 0; i < 3 && it != e; i++, ++it) {
                        TABLED() { (*it)->OutputHtml(str); }
                    }
                }
            }
        }

        template<typename Iter>
        void OutputQuantums(IOutputStream& str, Iter it, Iter e) {
            if (it != e) {
                OutputRow(str, it, e);
                if (it != e) {
                    HTML(str) {
                        TABLER() {
                            str << "<td colspan=3>";
                            TString id = CreateGuidAsString();
                            COLLAPSED_BUTTON_CONTENT(id, "More") {
                                TABLE() {
                                    OutputQuantums(str, it, e);
                                }
                            }
                            str << "</td>";
                        }
                    }
                }
            }
        }

        TDuration EstimateTimeOfArrival() {
            if (!History) {
                return {};
            }

            TEvReplFinished::TInfoPtr first = History.First();
            TEvReplFinished::TInfoPtr last = History.Last();

            const ui64 workAtBegin = first->WorkUnitsTotal;
            const ui64 workAtEnd = last->WorkUnitsTotal - last->WorkUnitsPerformed;
            const TInstant timeAtBegin = first->Start;
            const TInstant timeAtEnd = last->End;

            if (workAtBegin < workAtEnd || timeAtEnd < timeAtBegin) {
                return {}; // can't evaluate
            }

            const double workPerSecond = (workAtBegin - workAtEnd) / (timeAtEnd - timeAtBegin).SecondsFloat();

            return TDuration::Seconds(workAtEnd / workPerSecond);
        }

        void ResetReplProgressTimer(bool finish) {
            if (finish) {
                ReplProgressWatchdog.Disarm();
            } else {
                ReplProgressWatchdog.Rearm(SelfId());
            }
            ReplCtx->MonGroup.ReplMadeNoProgress() = 0;
        }

        void ReplStuck() {
            ReplCtx->MonGroup.ReplMadeNoProgress() = 1;
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev) {
            Y_DEBUG_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::ReplId);

            TStringStream str;
            unsigned historySize = HistorySize;
            str << "\n";
            HTML(str) {
                DIV_CLASS("panel panel-success") {
                    DIV_CLASS("panel-heading") {str << "Repl";}
                    DIV_CLASS("panel-body") {
                        auto makeConnectedDonorDisks = [&]() -> TString {
                            TStringBuilder s;
                            s << "[";
                            for (auto it = ConnectedDonorDisks.begin(); it != ConnectedDonorDisks.end(); ++it) {
                                if (it != ConnectedDonorDisks.begin()) {
                                    s << " ";
                                }
                                s << *it;
                            }
                            return s << "]";
                        };
                        str << "State: " << StateToStr(State) << "<br>"
                            << "LastReplStart: " << ToStringLocalTimeUpToSeconds(LastReplStart) << "<br>"
                            << "LastReplEnd: " << ToStringLocalTimeUpToSeconds(LastReplEnd) << "<br>"
                            << "LastReplQuantumStart: " << ToStringLocalTimeUpToSeconds(LastReplQuantumStart) << "<br>"
                            << "LastReplQuantumEnd: " << ToStringLocalTimeUpToSeconds(LastReplQuantumEnd) << "<br>"
                            << "NumConnectedPeerDisks: " << ConnectedPeerDisks.size() << "<br>"
                            << "ConnectedDonorDisks: " << makeConnectedDonorDisks() << "<br>"
                            << "ReplicationEndTime: " << ReplicationEndTime << "<br>";

                        TABLE_CLASS ("table table-condensed") {
                            CAPTION() STRONG() {str << "Last " << historySize << " replication quantums"; }
                            TABLEBODY() {
                                TVector<TEvReplFinished::TInfoPtr> quantums;
                                for (auto it = History.Begin(); it != History.End(); ++it) {
                                    quantums.push_back(*it);
                                }
                                OutputQuantums(str, quantums.rbegin(), quantums.rend());
                            }
                        }
                    }
                }
            }
            str << "\n";

            Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::ReplId));
        }

        void HandleGenerationChange(TEvVGenerationChange::TPtr& ev) {
            // forward message to queue actors
            TEvVGenerationChange *msg = ev->Get();
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, msg->Clone());
            }
            ReplCtx->GInfo = msg->NewInfo;
        }

        std::set<TActorId> DonorQueryActors;

        void Handle(TEvBlobStorage::TEvEnrichNotYet::TPtr ev) {
            DonorQueryActors.insert(Register(new TDonorQueryActor(*ev->Get(), Donors, ReplCtx->VCtx)));
        }

        void Handle(TEvents::TEvActorDied::TPtr ev) {
            const size_t num = DonorQueryActors.erase(ev->Sender);
            Y_ABORT_UNLESS(num);
        }

        void Ignore()
        {}

        void Handle(TEvReplInvoke::TPtr ev) {
            if (ReplJobActorId) {
                const TActorId selfId = SelfId();
                TActivationContext::Send(new IEventHandle(ReplJobActorId, ev->Sender, ev->Release().Release(), 0, ev->Cookie,
                    &selfId));
            } else {
                ev->Get()->Callback(UnreplicatedBlobRecords, TString());
            }
        }

        STRICT_STFUNC(StateRelax,
            cFunc(TEvents::TSystem::Wakeup, StartReplication)
            hFunc(NMon::TEvHttpInfo, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
            hFunc(TEvProxyQueueState, Handle)
            hFunc(TEvVGenerationChange, HandleGenerationChange)
            hFunc(TEvResumeForce, Handle)
            hFunc(TEvBlobStorage::TEvEnrichNotYet, Handle)
            hFunc(TEvents::TEvActorDied, Handle)
            cFunc(TEvBlobStorage::EvCommenceRepl, StartReplication)
            hFunc(TEvReplInvoke, Handle)
            hFunc(TEvReplCheckProgress, ReplProgressWatchdog)
            hFunc(TEvMinHugeBlobSizeUpdate, Handle)
        )

        void PassAway() override {
            // forward poison pills to queue actors
            for (const auto& kv : *QueueActorMapPtr) {
                Send(kv.second, new TEvents::TEvPoison);
            }
            for (const auto& donor : DonorQueue) {
                if (donor) {
                    Send(donor->QueueActorId, new TEvents::TEvPoison);
                }
            }
            for (const TActorId& actorId : DonorQueryActors) {
                Send(actorId, new TEvents::TEvPoison);
            }

            // return replication token if we have one
            if (State == AwaitToken || State == WaitQueues || State == Replication) {
                Send(MakeBlobStorageReplBrokerID(), new TEvReleaseReplToken);
            }

            if (ReplJobActorId) {
                Send(ReplJobActorId, new TEvents::TEvPoison);
            }

            ResetReplProgressTimer(true);
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateRepl,
            cFunc(TEvReplToken::EventType, HandleReplToken)
            hFunc(TEvReplStarted, Handle)
            hFunc(TEvReplFinished, Handle)
            hFunc(NMon::TEvHttpInfo, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
            hFunc(TEvProxyQueueState, Handle)
            hFunc(TEvVGenerationChange, HandleGenerationChange)
            hFunc(TEvResumeForce, Handle)
            hFunc(TEvBlobStorage::TEvEnrichNotYet, Handle)
            hFunc(TEvents::TEvActorDied, Handle)
            cFunc(TEvBlobStorage::EvCommenceRepl, Ignore)
            hFunc(TEvReplInvoke, Handle)
            hFunc(TEvReplCheckProgress, ReplProgressWatchdog)
            hFunc(TEvMinHugeBlobSizeUpdate, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_REPL_SCHEDULER;
        }

        TReplScheduler(std::shared_ptr<TReplCtx> &replCtx)
            : TActorBootstrapped<TReplScheduler>()
            , ReplCtx(replCtx)
            , NextMinREALHugeBlobInBytes(ReplCtx->MinREALHugeBlobInBytes)
            , History(HistorySize)
            , State(Relaxation)
            , ReplProgressWatchdog(
                ReplCtx->VDiskCfg->ReplMaxTimeToMakeProgress,
                std::bind(&TThis::ReplStuck, this)
            )
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // REPL ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateReplActor(std::shared_ptr<TReplCtx> &replCtx) {
        return new TReplScheduler(replCtx);
    }

} // NKikimr
