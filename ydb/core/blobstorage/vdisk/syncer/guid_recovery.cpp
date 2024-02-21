#include "guid_recovery.h"
#include "guid_proxyobtain.h"
#include "blobstorage_syncer_committer.h"
#include "blobstorage_syncquorum.h"
#include <ydb/core/blobstorage/vdisk/common/sublog.h>

#include <ydb/library/actors/core/interconnect.h>

using namespace NKikimrServices;
using namespace NKikimr::NSync;

/*

 VDisk Guid Recovery Algorithm
 =============================
 This algorithm is being run as a first step of VDisk syncronization process.
 The purpose of this algorithm is to understand if VDisk has lost it's state
 since previous run, i.e. can we trust the local state of this VDisk. Please
 note that if local state of VDisk can be correct but outdated. This algorithm
 doesn't answer the question if the local state is outdated, only if it is
 correct.
 The idea behind the algorithm is the following. The VDisk stores its guid
 (TVDiskEternalGuid) locally in persistent storage and remotely on other VDisks
 of the BlobStorage group. During sync phase VDisk compare it's local guid
 and remote VDisks guids to reach a verdict if we have lost local data.
 Additional complexity stems from the fact that we must distinguish between
 the situation of data loss from the first start of VDisk.

 How does algorithm works:
 1. Get local state of the VDisk from recovery log (TLocalSyncerState structure)
 2. Ask all other VDisks of the BlobStorage group what they know about us
 3. Receive quorum of answers and reach a verdict (EDecision enumeration). Note
    that we even can discover 'inconsistency', which means something goes
    really wrong and the given BlobStorage group can not exist anymore.
    The decision is made according Decision Table described below.

 If a verdict is FirstRun phase (i.e. we are running this group for first time),
 additional activities must take place, i.e. we must select Guid for the given
 VDisk and make other VDisks in the group agree on this decision. This is done
 in some form of two phase commit.


 First Run Algorithm
 ===================
 This algorithm is a part of VDisk Guid Recovery algorithm and it's purpose
 to generate Guid for this VDisk once and forever until this BlobStorage
 group lives.
 1. Generate Guid randomly
 2. Save Guid to all VDisks in group with status InProgress
 3. When quorum replies, save Guid locally with status Selected
 4. Save Guid to all VDisks in group with status Final
 5. When quorum replies, save Guid locally with status Final
 If we fail at any step, we replays the algorithm with some step. Step is
 determined by collaborative state of other VDisks in group.


 Decision Table
 ==============
 We need to make a decision about local VDisk guid.
 SELFVDISK represents our local state after local recovery
 VDISKQUORUM represents aggregated knowledge from other VDisks

 SELFVDISK  |           VDISKQUORUM
            |----------------------------
            | Empty | InProgress | Final
 ========================================
 Empty      |   F   |      F     |   L
 ----------------------------------------
 Selected   |   I   |      F     |   F
 ----------------------------------------
 Lost       |   I   |      I     |   L
 ----------------------------------------
 Final      |   I   |      I     |   G


 Decision:
 F -- FirstRun or continue FirstRun
 G -- Good, local database is consistent and trustable
 L -- LostData, we have lost the data and need to recover it;
 usually it means that we have detected loss of data and
 then process restarted
 I -- Inconsistency, inconsistent state, should never happen
 with allowed number of failures

 VDISKQUORUM aggregation rules:
 1. Final has priority over InProgress, InProgress has priority
 over Empty
 2. No conflicts can be for Final state
 3. Select majority in InProgress state to speed up the process

*/


namespace NKikimr {
    namespace NSyncer {

        ////////////////////////////////////////////////////////////////////////////
        // EDecision
        ////////////////////////////////////////////////////////////////////////////
        const char *EDecisionToStr(EDecision d) {
            switch (d) {
                case EDecision::FirstRun:       return "FirstRun";
                case EDecision::Good:           return "Good";
                case EDecision::LostData:       return "LostData";
                case EDecision::Inconsistency:  return "Inconsistency";
                default:                        return "Unknown";
            }
        }

        ////////////////////////////////////////////////////////////////////////////
        // TDecision
        // Decision we got after guid recovery algorithm work
        ////////////////////////////////////////////////////////////////////////////
        TDecision::TDecision(EDecision d,
                             EFirstRunStep f,
                             TVDiskEternalGuid guid,
                             const TString &e,
                             bool a)
            : Decision(d)
            , FirstRunStep(f)
            , Guid(guid)
            , Explanation(e)
            , AddInfo(a)
        {}

        TDecision TDecision::FirstRun(EFirstRunStep step, TVDiskEternalGuid guid) {
            return TDecision(EDecision::FirstRun, step, guid, TString(), false);
        }

        TDecision TDecision::Inconsistency(const TString &expl) {
            return TDecision(EDecision::Inconsistency, EFirstRunStep::STATE__Terminated,
                             0, expl, false);
        }

        TDecision TDecision::LostData(TVDiskEternalGuid guid, bool subsequentFailure) {
            return TDecision(EDecision::LostData, EFirstRunStep::STATE__Terminated,
                             guid, TString(), subsequentFailure);
        }

        TDecision TDecision::LostData(EFirstRunStep step,
                                      TVDiskEternalGuid guid,
                                      bool subsequentFailure) {
            return TDecision(EDecision::LostData, step, guid, TString(), subsequentFailure);
        }

        TDecision TDecision::Good(TVDiskEternalGuid guid) {
            return TDecision(EDecision::Good, EFirstRunStep::STATE__Terminated,
                             guid, TString(), false);
        }

        // We got Good decision, but need to pass additional step of FirstRun phase
        TDecision TDecision::Good(EFirstRunStep step, TVDiskEternalGuid guid) {
            return TDecision(EDecision::Good, step, guid, TString(), false);
        }

        void TDecision::Output(IOutputStream &str) const {
            str << "[Decision# " << Decision;
            switch (Decision) {
                case EDecision::FirstRun:
                    str << " Step# " << FirstRunStep;
                    str << " LostData# " << AddInfo;
                    break;
                case EDecision::Good:
                    break;
                case EDecision::LostData:
                    str << " SubsequentFailure# " << AddInfo;
                    break;
                case EDecision::Inconsistency:
                    str << " Explanation# \"" << Explanation << "\"";
                    break;
                default: Y_ABORT("Unexpected case");
            }
            str << "]";
        }

        TString TDecision::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        EFirstRunStep TDecision::GetFirstRunStep() const {
            return FirstRunStep;
        }

        ////////////////////////////////////////////////////////////////////////////
        // TOutcome
        // Outcome of the final decision that we pass outside
        ////////////////////////////////////////////////////////////////////////////
        void TOutcome::Output(IOutputStream &str) const {
            str << "[Decision# " << Decision;
            str << " Guid# " << Guid;
            if (!Explanation.empty())
                str << " Explanation# " << Explanation;
            str << "]";
        }

        TString TOutcome::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }


        namespace {
            ////////////////////////////////////////////////////////////////////////
            // TNeighborVDiskState represents state of a neighbor VDisk during Guid
            // recovery
            ////////////////////////////////////////////////////////////////////////
            struct TNeighborVDiskState {
                // from protobuf
                using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
                using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;


                // ActorID of a proxy that communicates with the given VDisk
                TActorId ProxyId;
                // guid
                TVDiskEternalGuid Guid = 0;
                // confidence
                ESyncState State = TSyncVal::Empty;
                // if we got response from a vdisk
                bool Obtained = false;

                TNeighborVDiskState() = default;

                void SetActorID(const TActorId &proxyId) {
                    ProxyId = proxyId;
                }

                void Setup(TVDiskEternalGuid guid, ESyncState state) {
                    Y_ABORT_UNLESS(!Obtained);
                    Obtained = true;
                    Guid = guid;
                    State = state;
                }
            };

            ////////////////////////////////////////////////////////////////////////
            // Collaborative decision by quorum of VDisks (w/o respect to local state)
            ////////////////////////////////////////////////////////////////////////
            struct TVDiskQuorumDecision {
                // from protobuf
                using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
                using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;

                const bool IsInconsistent = true;
                const ESyncState State = TSyncVal::Empty;
                const TVDiskEternalGuid Guid = 0;
                const EFirstRunStep FirstRunStep = EFirstRunStep::STATE__Uninitialized;
                const TString Explanation;

            private:
                TVDiskQuorumDecision(bool i,
                                     ESyncState s,
                                     TVDiskEternalGuid guid,
                                     EFirstRunStep f,
                                     const TString &e)
                    : IsInconsistent(i)
                    , State(s)
                    , Guid(guid)
                    , FirstRunStep(f)
                    , Explanation(e)
                {}

            public:
                static TVDiskQuorumDecision Inconsistent(const TString &exp) {
                    return TVDiskQuorumDecision(true, TSyncVal::Empty, 0,
                        EFirstRunStep::STATE__Uninitialized, exp);
                }

                static TVDiskQuorumDecision Empty() {
                    return TVDiskQuorumDecision(false, TSyncVal::Empty, 0,
                        EFirstRunStep::STATE__Uninitialized, "");
                }

                static TVDiskQuorumDecision InProgress(TVDiskEternalGuid guid, EFirstRunStep step) {
                    return TVDiskQuorumDecision(false, TSyncVal::InProgress, guid, step, "");
                }

                static TVDiskQuorumDecision Final(TVDiskEternalGuid guid) {
                    return TVDiskQuorumDecision(false, TSyncVal::Final, guid,
                        EFirstRunStep::STATE__Uninitialized, "");
                }

                void Output(IOutputStream &str) const {
                    if (IsInconsistent) {
                        str << "[Inconsistent Explanation: \"" << Explanation << "\"]";
                    } else {
                        str << "[State# " << State << " Guid# " << Guid;
                        if (State == TSyncVal::InProgress) {
                            str << " FirstRunStep#" << FirstRunStep;
                        }
                        str << "]";
                    }
                }
            };

        } // anonymous namespace

        ////////////////////////////////////////////////////////////////////////////
        // Decision we make after communication with other VDisks
        ////////////////////////////////////////////////////////////////////////////
        class TDecisionMaker {
        public:
            // from protobuf
            using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
            using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;
            using ELocalState = NKikimrBlobStorage::TLocalGuidInfo::EState;
            using TLocalVal = NKikimrBlobStorage::TLocalGuidInfo;

            TDecisionMaker(const TVDiskIdShort &self,
                           std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                           const TLocalSyncerState &locallyRecoveredState)
                : Self(self)
                , Top(top)
                , LocallyRecoveredState(locallyRecoveredState)
                , Neighbors(self, Top)
                , QuorumTracker(self, Top, true) // include my faildomain
                , Sublog(false, self.ToString() + ": ")
            {}

            void SetResponse(const TVDiskID &vdisk, TVDiskEternalGuid guid, ESyncState state) {
                Y_ABORT_UNLESS(Neighbors[vdisk].VDiskIdShort == vdisk);

                Sublog.Log() << "RESPONSE: vdisk# " << vdisk.ToString()
                    << " state# " << state << " guid# " << guid << "\n";

                Neighbors[vdisk].Get().Setup(guid, state);
                QuorumTracker.Update(vdisk);
            }

            void RunSurveyOfAllVDisks(std::function<void(TVDiskInfo<TNeighborVDiskState>&)> func) {
                for (auto &x : Neighbors) {
                    func(x);
                }
            }

            void ReconfigureAllWorkingProxies(
                        std::function<void(TVDiskInfo<TNeighborVDiskState>&)> func)
            {
                for (auto &x : Neighbors) {
                    const auto &v = x.Get();
                    if (!v.Obtained) {
                        func(x);
                    }
                }
            }

            unsigned AbandomOngoingRequests(const TActorContext &ctx) {
                unsigned counter = 0;
                for (const auto &x : Neighbors) {
                    if (!x.Get().Obtained) {
                        // cancel proxy
                        ++counter;
                        ctx.Send(x.Get().ProxyId, new NActors::TEvents::TEvPoisonPill());
                    }
                }
                return counter;
            }

            bool GotQuorum() const {
                return QuorumTracker.HasQuorum();
            }


            // calculates final decision
            TDecision ReachAVerdict() const {
                Y_ABORT_UNLESS(QuorumTracker.HasQuorum());

                TVDiskQuorumDecision quorumDecision = VDiskQuorumDecision();
                if (quorumDecision.IsInconsistent) {
                    return TDecision::Inconsistency(quorumDecision.Explanation);
                }

                switch (quorumDecision.State) {
                    case TSyncVal::Empty:       return HandleQuorumEmpty(quorumDecision);
                    case TSyncVal::InProgress:  return HandleQuorumInProgress(quorumDecision);
                    case TSyncVal::Final:       return HandleQuorumFinal(quorumDecision);
                    default:
                        Y_ABORT("Unexpected case");
                }
            }

        private:
            const TVDiskIdShort Self;
            const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
            const TLocalSyncerState LocallyRecoveredState;
            NSync::TVDiskNeighbors<TNeighborVDiskState> Neighbors;
            NSync::TQuorumTracker QuorumTracker;
            TSublog<> Sublog;

            TVDiskQuorumDecision VDiskQuorumDecision() const {
                // analyze obtained results
                // via finalQuorum we calculate if we got quorum of fail domains of Final State
                TQuorumTracker finalQuorum(Self, Top, true); // include my faildomain
                TMap<TVDiskEternalGuid, ui32> finalGuidMap;      // Guid -> HowManyVDisksHasThisGuid
                TMap<TVDiskEternalGuid, ui32> inProgressGuidMap; // Guid -> HowManyVDisksHasThisGuid
                for (const auto &x : Neighbors) {
                    const auto &v = x.Get();
                    if (v.Obtained) {
                        switch (v.State) {
                            case TSyncVal::Empty: {
                                break;
                            }
                            case TSyncVal::InProgress: {
                                inProgressGuidMap[v.Guid]++;
                                break;
                            }
                            case TSyncVal::Final: {
                                finalGuidMap[v.Guid]++;
                                finalQuorum.Update(x.VDiskIdShort);
                                break;
                            }
                            default: Y_ABORT("Unexpected case");
                        }
                    }
                }

                // analyze Final
                if (!finalGuidMap.empty()) {
                    size_t size = finalGuidMap.size();
                    if (size > 1) {
                        // we can't have multiple final guids
                        TStringStream str;
                        str << "Can't have multiple final guids; got:";
                        for (const auto &x : finalGuidMap) {
                            str << " [guid# " << x.first << " count# " << x.second << "]";
                        }
                        return TVDiskQuorumDecision::Inconsistent(str.Str());
                    }
                    Y_ABORT_UNLESS(size == 1);
                    const TVDiskEternalGuid guid = finalGuidMap.begin()->first;
                    if (finalQuorum.HasQuorum()) {
                        return TVDiskQuorumDecision::Final(guid);
                    } else {
                        auto step = EFirstRunStep::ACTION_WriteFinalToQuorum;
                        return TVDiskQuorumDecision::InProgress(guid, step);
                    }
                }

                // analyze InProgress
                if (!inProgressGuidMap.empty()) {
                    // select mostly spreaded over group guid, i.e.
                    // guid with max count
                    TVDiskEternalGuid guid = inProgressGuidMap.begin()->first;
                    ui32 count = inProgressGuidMap.begin()->second;
                    for (const auto &x: inProgressGuidMap) {
                        if (x.second > count) {
                            guid = x.first;
                            count = x.second;
                        }
                    }
                    auto step = EFirstRunStep::ACTION_WriteInProgressToQuorum;
                    return TVDiskQuorumDecision::InProgress(guid, step);
                }

                // no info from other VDisks
                return TVDiskQuorumDecision::Empty();
            }

            // calculates final decision when quorum decision is Empty
            TDecision HandleQuorumEmpty(const TVDiskQuorumDecision &quorumDecision) const {
                Y_UNUSED(quorumDecision);
                switch (LocallyRecoveredState.State) {
                    case TLocalVal::Empty: {
                        return TDecision::FirstRun(EFirstRunStep::ACTION_GenerateGuid, 0);
                    }
                    case TLocalVal::Selected:
                    case TLocalVal::Lost:
                    case TLocalVal::Final: {
                        TStringStream str;
                        str << "VDisk Quorum Decision is Empty, but local state is "
                            << LocallyRecoveredState;
                        return TDecision::Inconsistency(str.Str());
                    }
                    default: Y_ABORT("Unexpected case");
                }
            }

            // calculates final decision when quorum decision is InProgress
            TDecision HandleQuorumInProgress(const TVDiskQuorumDecision &quorumDecision) const {
                switch (LocallyRecoveredState.State) {
                    case TLocalVal::Empty: {
                        switch (quorumDecision.FirstRunStep) {
                            case EFirstRunStep::ACTION_WriteInProgressToQuorum: {
                                auto step = EFirstRunStep::ACTION_WriteInProgressToQuorum;
                                auto guid = quorumDecision.Guid;
                                return TDecision::FirstRun(step, guid);
                            }
                            case EFirstRunStep::ACTION_WriteFinalToQuorum: {
                                // we definitely lost data, but we don't know was it on FirstRun
                                // or not, i.e. if we had empty db or not
                                auto guid = quorumDecision.Guid;
                                auto step = EFirstRunStep::ACTION_WriteFinalToQuorum;
                                return TDecision::LostData(step, guid, false);
                            }
                            default:
                                Y_ABORT("Unexpected case");
                        }
                        break;
                    }
                    case TLocalVal::Selected: {
                        switch (quorumDecision.FirstRunStep) {
                            case EFirstRunStep::ACTION_WriteInProgressToQuorum: {
                                auto step = EFirstRunStep::ACTION_WriteInProgressToQuorum;
                                auto guid = quorumDecision.Guid;
                                auto d = TDecision::FirstRun(step, guid);
                                return CheckLocalAndRemoteGuid(quorumDecision, d);
                            }
                            case EFirstRunStep::ACTION_WriteFinalToQuorum: {
                                auto step = EFirstRunStep::ACTION_WriteFinalToQuorum;
                                auto guid = quorumDecision.Guid;
                                auto d = TDecision::FirstRun(step, guid);
                                return CheckLocalAndRemoteGuid(quorumDecision, d);
                            }
                            default:
                                Y_ABORT("Unexpected case");
                        }
                        break;
                    }
                    case TLocalVal::Lost: {
                        switch (quorumDecision.FirstRunStep) {
                            case EFirstRunStep::ACTION_WriteInProgressToQuorum: {
                                Sublog.Log() << "VDisk Quorum Decision is InProgress, "
                                    << "but local state is " << LocallyRecoveredState;
                                return TDecision::Inconsistency(Sublog.Get());
                            }
                            case EFirstRunStep::ACTION_WriteFinalToQuorum: {
                                auto step = EFirstRunStep::ACTION_WriteFinalToQuorum;
                                auto guid = quorumDecision.Guid;
                                auto d = TDecision::LostData(step, guid, true);
                                return CheckLocalAndRemoteGuid(quorumDecision, d);
                            }
                            default:
                                Y_ABORT("Unexpected case");
                        }
                        break;
                    }
                    case TLocalVal::Final: {
                        switch (quorumDecision.FirstRunStep) {
                            case EFirstRunStep::ACTION_WriteInProgressToQuorum: {
                                Sublog.Log() << "VDisk Quorum Decision is InProgress, "
                                    << "but local state is " << LocallyRecoveredState;
                                return TDecision::Inconsistency(Sublog.Get());
                            }
                            case EFirstRunStep::ACTION_WriteFinalToQuorum: {
                                auto step = EFirstRunStep::ACTION_WriteFinalToQuorum;
                                auto guid = quorumDecision.Guid;
                                auto d = TDecision::Good(step, guid);
                                return CheckLocalAndRemoteGuid(quorumDecision, d);
                            }
                            default:
                                Y_ABORT("Unexpected case");
                        }
                        break;
                    }
                    default: Y_ABORT("Unexpected case");
                }
            }

            // calculates final decision when quorum decision is Final
            TDecision HandleQuorumFinal(const TVDiskQuorumDecision &quorumDecision) const {
                switch (LocallyRecoveredState.State) {
                    case TLocalVal::Empty: {
                        // DATA LOSS
                        auto guid = quorumDecision.Guid;
                        return TDecision::LostData(guid, false);
                    }
                    case TLocalVal::Selected: {
                        auto step = EFirstRunStep::ACTION_WriteFinalLocally;
                        auto guid = quorumDecision.Guid;
                        auto d = TDecision::FirstRun(step, guid);
                        return CheckLocalAndRemoteGuid(quorumDecision, d);
                    }
                    case TLocalVal::Lost: {
                        // DATA LOSS again (didn't recover from last time)
                        auto guid = quorumDecision.Guid;
                        return TDecision::LostData(guid, true);
                    }
                    case TLocalVal::Final: {
                        auto guid = quorumDecision.Guid;
                        auto d = TDecision::Good(guid);
                        return CheckLocalAndRemoteGuid(quorumDecision, d);
                    }
                    default: Y_ABORT("Unexpected case");
                }
            }

            TDecision CheckLocalAndRemoteGuid(const TVDiskQuorumDecision &quorumDecision,
                                              const TDecision &des) const {
                auto remoteGuid = quorumDecision.Guid;
                auto localGuid = LocallyRecoveredState.Guid;
                if (localGuid != remoteGuid) {
                    TStringStream str;
                    str << "Guid mismatch; quorumDecision# "
                        << quorumDecision
                        << " locallyRecoveredState# "
                        << LocallyRecoveredState;
                    return TDecision::Inconsistency(str.Str());
                } else {
                    return des;
                }
            }
        };


        ////////////////////////////////////////////////////////////////////////////
        // TVDiskGuidRecoveryActor
        ////////////////////////////////////////////////////////////////////////////
        class TVDiskGuidRecoveryActor : public TActorBootstrapped<TVDiskGuidRecoveryActor>
        {
            // from protobuf
            using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
            using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;
            using ELocalState = NKikimrBlobStorage::TLocalGuidInfo::EState;
            using TLocalVal = NKikimrBlobStorage::TLocalGuidInfo;

            friend class TActorBootstrapped<TVDiskGuidRecoveryActor>;

            enum EPhase {
                PhaseNotSet,
                PhaseObtainGuid,
                PhaseFirstRun,
                PhaseSettleDataLoss
            };

            TIntrusivePtr<TVDiskContext> VCtx;
            TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
            const TActorId CommitterId;
            const TActorId NotifyId;
            TDecisionMaker DecisionMaker;
            std::unique_ptr<TDecision> Decision;
            EPhase Phase = PhaseNotSet;
            TActorId FirstRunActorId;
            const bool ReadOnly;


            ////////////////////////////////////////////////////////////////////////
            // Ask Guid from other VDisks
            ////////////////////////////////////////////////////////////////////////
            void Bootstrap(const TActorContext &ctx) {
                LOG_INFO(ctx, BS_SYNCER,
                         VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidRecoveryActor: START"));
                SUBLOGLINE(NotifyId, ctx, { stream << "GuidRecovery: START"; });

                // run Obtain VDisk Guid proxy for every VDisk in the group
                auto runProxyForVDisk = [this, &ctx] (TVDiskInfo<TNeighborVDiskState>& x) {
                    auto selfVDiskId = GInfo->GetVDiskId(VCtx->ShortSelfVDisk);
                    auto vd = GInfo->GetVDiskId(x.OrderNumber);
                    auto aid = GInfo->GetActorId(x.OrderNumber);
                    auto proxyActor = CreateProxyForObtainingVDiskGuid(VCtx, selfVDiskId, vd, aid, ctx.SelfID);
                    auto actorId = ctx.Register(proxyActor);
                    x.Get().SetActorID(actorId);
                };

                DecisionMaker.RunSurveyOfAllVDisks(runProxyForVDisk);
                Become(&TThis::ObtainGuidQuorumFunc);
                Phase = PhaseObtainGuid;
            }

            void HandleObtainGuidQuorumMode(TEvVGenerationChange::TPtr &ev,
                                            const TActorContext &ctx) {
                // save new Group Info
                GInfo = ev->Get()->NewInfo;

                // reconfigure every proxy that hasn't returned a value yet
                auto reconfigureProxy = [&ctx] (std::unique_ptr<TEvVGenerationChange> &&msg,
                                                TVDiskInfo<TNeighborVDiskState>& x) {
                    Y_ABORT_UNLESS(!x.Get().Obtained);
                    ctx.Send(x.Get().ProxyId, msg.release());
                };

                using namespace std::placeholders;
                auto call = [&reconfigureProxy, msg = ev->Get()] (TVDiskInfo<TNeighborVDiskState>& x) {
                    reconfigureProxy(std::unique_ptr<TEvVGenerationChange>(msg->Clone()), x);
                };
                DecisionMaker.ReconfigureAllWorkingProxies(call);
            }

            STRICT_STFUNC(ObtainGuidQuorumFunc,
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
                HFunc(TEvVDiskGuidObtained, Handle)
                HFunc(TEvSublogLine, Handle)
                HFunc(TEvVGenerationChange, HandleObtainGuidQuorumMode)
            )

            ////////////////////////////////////////////////////////////////////////
            // Gather Quorum
            ////////////////////////////////////////////////////////////////////////
            void Handle(TEvVDiskGuidObtained::TPtr& ev, const TActorContext &ctx) {
                const TEvVDiskGuidObtained *msg = ev->Get();

                SUBLOGLINE(NotifyId, ctx, {
                    stream << "GuidRecovery: ObtainedFromPeer: " << msg->ToString();
                });
                DecisionMaker.SetResponse(msg->VDiskId, msg->Guid, msg->State);
                if (DecisionMaker.GotQuorum()) {
                    // kill actors that didn't respond
                    DecisionMaker.AbandomOngoingRequests(ctx);
                    // reach a verdict and save it
                    Decision = std::make_unique<TDecision>(DecisionMaker.ReachAVerdict());

                    // log result of guid recovery
                    auto pri = NActors::NLog::PRI_INFO;
                    if (Decision->BadDecision())
                        pri = NActors::NLog::PRI_ERROR;
                    LOG_LOG(ctx, pri, BS_SYNCER,
                            VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidRecoveryActor: DECISION: %s",
                                  Decision->ToString().data()));
                    SUBLOGLINE(NotifyId, ctx, {
                        stream << "GuidRecovery: DECISION: " << Decision->ToString();
                    });

                    // perform some actions depending on result we got
                    auto firstRunStep = Decision->GetFirstRunStep();
                    switch (Decision->GetDecision()) {
                        case EDecision::FirstRun: {
                            // run or continue first run phase, create an actor
                            // that performs all that stuff
                            FirstRunPhase(ctx, firstRunStep);
                            break;
                        }
                        case EDecision::Good: {
                            // all good, report decision and die
                            if (firstRunStep == EFirstRunStep::STATE__Terminated) {
                                Finish(ctx, *Decision);
                            } else {
                                FirstRunPhase(ctx, firstRunStep);
                            }
                            break;
                        }
                        case EDecision::LostData: {
                            // save locally that we lost the data and keep
                            // this status until database is recovered (next stages,
                            // outside of guid recovery)
                            if (firstRunStep == EFirstRunStep::STATE__Terminated) {
                                SettleDataLossPhase(ctx);
                            } else {
                                FirstRunPhase(ctx, firstRunStep);
                            }
                            break;
                        }
                        case EDecision::Inconsistency: {
                            // all bad, report decision and die
                            Finish(ctx, *Decision);
                            break;
                        }
                        default: Y_ABORT("Unexpected case");
                    }
                }
            }

            ////////////////////////////////////////////////////////////////////////
            // Finish
            ////////////////////////////////////////////////////////////////////////
            void Finish(const TActorContext &ctx, TOutcome &&outcome) {
                // log result of guid recovery
                auto pri = NActors::NLog::PRI_INFO;
                if (outcome.BadDecision())
                    pri = NActors::NLog::PRI_ERROR;
                LOG_LOG(ctx, pri, BS_SYNCER,
                        VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidRecoveryActor: FINISH: %s", outcome.ToString().data()));
                SUBLOGLINE(NotifyId, ctx, {
                    stream << "GuidRecovery: FINISH: " << outcome.ToString();
                });

                ctx.Send(NotifyId, new TEvVDiskGuidRecovered(std::move(outcome)));
                Die(ctx);
            }

            ////////////////////////////////////////////////////////////////////////
            // FirstRun Phase
            ////////////////////////////////////////////////////////////////////////
            void FirstRunPhase(const TActorContext &ctx, EFirstRunStep f) {
                if (ReadOnly) {
                    const TString explanation = "unable to establish new GUID while in read-only";
                    LOG_WARN(ctx, BS_SYNCER,
                        VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidRecoveryActor: %s", explanation.data()));
                    *Decision = TDecision::Inconsistency(explanation);
                    Finish(ctx, *Decision);
                    return;
                }
                auto guid = Decision->GetGuid();
                Become(&TThis::WaitForFirstRunStateFunc);
                FirstRunActorId = ctx.Register(CreateVDiskGuidFirstRunActor(VCtx, GInfo, CommitterId, ctx.SelfID, f, guid));
                Phase = PhaseFirstRun;
            }

            void HandleFirstRunCompleted(TEvSyncerGuidFirstRunDone::TPtr& ev,
                                         const TActorContext &ctx)
            {
                switch (Decision->GetDecision()) {
                    case EDecision::FirstRun: {
                        auto guid = ev->Get()->Guid;
                        Finish(ctx, TOutcome(EDecision::FirstRun, guid));
                        break;
                    }
                    case EDecision::Good: {
                        Finish(ctx, *Decision);
                        break;
                    }
                    case EDecision::LostData: {
                        SettleDataLossPhase(ctx);
                        break;
                    }
                    default: Y_ABORT("Unexpected case");
                }
            }

            void HandleFirstRunMode(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
                // save new Group Info
                GInfo = ev->Get()->NewInfo;

                // notify FirstRunActorId
                ctx.Send(FirstRunActorId, ev->Get()->Clone());
            }

            STRICT_STFUNC(WaitForFirstRunStateFunc,
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
                HFunc(TEvSyncerGuidFirstRunDone, HandleFirstRunCompleted)
                // NOTE: In PhaseFirstRun phase we can still receive TEvVDiskGuidObtained
                // messages (i.e. replies from the previous phase). Ignore them.
                IgnoreFunc(TEvVDiskGuidObtained)
                HFunc(TEvSublogLine, Handle)
                HFunc(TEvVGenerationChange, HandleFirstRunMode)
            )

            ////////////////////////////////////////////////////////////////////////
            // Data Loss Phase
            ////////////////////////////////////////////////////////////////////////
            void SettleDataLossPhase(const TActorContext &ctx) {
                auto guid = Decision->GetGuid();
                Become(&TThis::WaitForSettlingDataLossStateFunc);
                auto msg = TEvSyncerCommit::Local(TLocalVal::Lost, guid);
                ctx.Send(CommitterId, msg.release());
                Phase = PhaseSettleDataLoss;
            }

            void HandleDataLossCompleted(TEvSyncerCommitDone::TPtr& ev,
                                         const TActorContext &ctx) {
                Y_UNUSED(ev);
                Finish(ctx, TOutcome(*Decision));
            }

            STRICT_STFUNC(WaitForSettlingDataLossStateFunc,
                HFunc(TEvents::TEvPoisonPill, HandlePoison)
                HFunc(TEvSyncerCommitDone, HandleDataLossCompleted)
                // NOTE: In PhaseFirstRun phase we can still receive TEvVDiskGuidObtained
                // messages (i.e. replies from the previous phase). Ignore them.
                IgnoreFunc(TEvVDiskGuidObtained)
                HFunc(TEvSublogLine, Handle)
                // no more communication with other VDisks, can ignore generation change
                IgnoreFunc(TEvVGenerationChange)
            )

            ////////////////////////////////////////////////////////////////////////
            // Handle Poison
            ////////////////////////////////////////////////////////////////////////
            void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
                Y_UNUSED(ev);
                switch (Phase) {
                    case PhaseNotSet:
                        break;
                    case PhaseObtainGuid:
                        DecisionMaker.AbandomOngoingRequests(ctx);
                        break;
                    case PhaseFirstRun:
                        ctx.Send(FirstRunActorId, new TEvents::TEvPoisonPill());
                        break;
                    case PhaseSettleDataLoss:
                        break;
                    default: Y_ABORT("Unexpected case");
                }
            }

            ////////////////////////////////////////////////////////////////////////
            // Handle TEvSublogLine
            ////////////////////////////////////////////////////////////////////////
            void Handle(TEvSublogLine::TPtr &ev, const TActorContext &ctx) {
                ctx.Send(ev->Forward(NotifyId));
            }

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_SYNC_VDISK_GUID_RECOVERY;
            }

            TVDiskGuidRecoveryActor(TIntrusivePtr<TVDiskContext> vctx,
                                    TIntrusivePtr<TBlobStorageGroupInfo> info,
                                    const TActorId &committerId,
                                    const TActorId &notifyId,
                                    const TLocalSyncerState &locallyRecoveredState,
                                    bool readOnly)
                : TActorBootstrapped<TVDiskGuidRecoveryActor>()
                , VCtx(std::move(vctx))
                , GInfo(std::move(info))
                , CommitterId(committerId)
                , NotifyId(notifyId)
                , DecisionMaker(VCtx->ShortSelfVDisk,
                                GInfo->PickTopology(),
                                locallyRecoveredState)
                , Decision()
                , ReadOnly(readOnly)
            {}
        };

    } // NSyncer

    IActor *CreateVDiskGuidRecoveryActor(TIntrusivePtr<TVDiskContext> vctx,
                                         TIntrusivePtr<TBlobStorageGroupInfo> info,
                                         const TActorId &committerId,
                                         const TActorId &notifyId,
                                         const NSyncer::TLocalSyncerState &localState,
                                         bool readOnly) {
        return new NSyncer::TVDiskGuidRecoveryActor(std::move(vctx), std::move(info), committerId,
            notifyId, localState, readOnly);
    }

} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NSyncer::TLocalSyncerState, stream, value) {
    value.Output(stream);
}

Y_DECLARE_OUT_SPEC(, NKikimr::NSyncer::TDecision, stream, value) {
    value.Output(stream);
}

Y_DECLARE_OUT_SPEC(, NKikimr::NSyncer::TVDiskQuorumDecision, stream, value) {
    value.Output(stream);
}

Y_DECLARE_OUT_SPEC(, NKikimr::NSyncer::EDecision, stream, value) {
    stream << NKikimr::NSyncer::EDecisionToStr(value);
}



