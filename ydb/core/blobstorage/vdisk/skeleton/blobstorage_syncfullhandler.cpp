#include "blobstorage_syncfullhandler.h"
#include "blobstorage_db.h"
#include "blobstorage_syncfull.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TVSyncFullHandler -- this actor starts on handling TEvVSyncFull requst.
    // It receives last lsn from sync log to make a snapshot of hull database.
    // This actor must be run on the same mailbox as TSkeleton
    // This actor creates THullSyncFull actor to be run on a different mailbox
    ////////////////////////////////////////////////////////////////////////////
    class TVSyncFullHandler : public TActorBootstrapped<TVSyncFullHandler> {
        TIntrusivePtr<TDb> Db;
        TIntrusivePtr<THullCtx> HullCtx;
        TVDiskID SelfVDiskId;
        const TActorId ParentId;
        std::shared_ptr<THull> Hull;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        std::shared_ptr<NMonGroup::TFullSyncGroup> FullSyncGroup;
        const ui64 DbBirthLsn;

        using TSessionKey = std::pair<TVDiskID, TVDiskID>; // { SourceVDisk, TargetVDisk }
        THashMap<TSessionKey, TActorId> UnorderedDataFullSyncSessions;
        THashMap<TActorId, TSessionKey> UnorderedDataFullSyncSessionLookup;

        friend class TActorBootstrapped<TVSyncFullHandler>;

        struct TEventInfo {
            TEventInfo(TEvBlobStorage::TEvVSyncFull::TPtr& ev)
                : Event(ev)
                , Record(ev->Get()->Record)
                , SourceVDisk(VDiskIDFromVDiskID(Record.GetSourceVDiskID()))
                , TargetVDisk(VDiskIDFromVDiskID(Record.GetTargetVDiskID()))
                , SessionKey(SourceVDisk, TargetVDisk)
                , ClientSyncState(SyncStateFromSyncState(Record.GetSyncState()))
                , Protocol(ev->Get()->GetProtocol())
                , Now(TActivationContext::Now())
            {}

            TEvBlobStorage::TEvVSyncFull::TPtr& Event;
            const NKikimrBlobStorage::TEvVSyncFull& Record;
            const TVDiskID SourceVDisk;
            const TVDiskID TargetVDisk;
            TSessionKey SessionKey;
            TSyncState ClientSyncState;

            NKikimrBlobStorage::EFullSyncProtocol Protocol;

            TInstant Now;
        };

        void RespondWithErroneousStatus(const TEventInfo& evInfo, TSyncState syncState,
                    NKikimrProto::EReplyStatus status) {
            auto result = std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(status, SelfVDiskId,
                    syncState, evInfo.Record.GetCookie(), evInfo.Now,
                    IFaceMonGroup->SyncFullResMsgsPtr(), nullptr, evInfo.Event->GetChannel());
            SendVDiskResponse(TActivationContext::AsActorContext(), evInfo.Event->Sender, result.release(),
                    evInfo.Event->Cookie, HullCtx->VCtx, {});
        }

        bool CheckEvent(const TEventInfo& evInfo) {
            // check that the disk is from this group
            if (!SelfVDiskId.SameGroupAndGeneration(evInfo.SourceVDisk) ||
                    !SelfVDiskId.SameDisk(evInfo.TargetVDisk)) {
                LOG_DEBUG_S(TActivationContext::AsActorContext(), BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                        << "TVSyncFullHandler: Invalid VDisk ids: "
                        << " SelfVDiskId# " << SelfVDiskId.ToString()
                        << " SorceVDiskId# " << evInfo.SourceVDisk.ToString()
                        << " TargetVDiskId# " << evInfo.TargetVDisk.ToString()
                        << " Marker# BSVSFH07");
                RespondWithErroneousStatus(evInfo, {}, NKikimrProto::ERROR);
                return false;
            }

            // check disk guid and start from the beginning if it has changed
            TVDiskIncarnationGuid incarnationGuid = Db->GetVDiskIncarnationGuid();
            if (incarnationGuid != evInfo.ClientSyncState.Guid) {
                LOG_DEBUG_S(TActivationContext::AsActorContext(), BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                        << "TVSyncFullHandler: GUID CHANGED;"
                        << " SourceVDisk# " << evInfo.SourceVDisk
                        << " DbBirthLsn# " << DbBirthLsn
                        << " VDiskIncarnationGuid# " << incarnationGuid
                        << " ClientGuid# " << evInfo.ClientSyncState.Guid
                        << " Marker# BSVSFH02");
                RespondWithErroneousStatus(evInfo, TSyncState(incarnationGuid, DbBirthLsn), NKikimrProto::NODATA);
                return false;
            }

            Y_VERIFY_DEBUG_S(evInfo.SourceVDisk != SelfVDiskId, HullCtx->VCtx->VDiskLogPrefix);

            LOG_DEBUG_S(TActivationContext::AsActorContext(), BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                    << "TVSyncFullHandler: syncedLsn# " << evInfo.ClientSyncState.SyncedLsn
                    << " SourceVDisk# " << evInfo.SourceVDisk
                    << " TargetVDisk# " << evInfo.TargetVDisk
                    << " Marker# BSVSFH90");
            return true;
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);

            LOG_DEBUG_S(TActivationContext::AsActorContext(), BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                    << "TVSyncFullHandler: Bootstrap: SelfVDiskId# " << SelfVDiskId
                    << " Marker# BSVSFH01");
        }

        void CreateActorForLegacyProtocol(const TEventInfo& evInfo, TSyncState syncState) {
            // parse stage and keys
            const NKikimrBlobStorage::ESyncFullStage stage = evInfo.Record.GetStage();
            const TLogoBlobID logoBlobFrom = LogoBlobIDFromLogoBlobID(evInfo.Record.GetLogoBlobFrom());
            const ui64 blockTabletFrom = evInfo.Record.GetBlockTabletFrom();
            const TKeyBarrier barrierFrom(evInfo.Record.GetBarrierFrom());

            Register(CreateHullSyncFullActorLegacyProtocol(
                    Db->Config,
                    HullCtx,
                    SelfId(),
                    Hull->GetIndexSnapshot(),
                    syncState,
                    SelfVDiskId,
                    IFaceMonGroup,
                    FullSyncGroup,
                    evInfo.Event,
                    TKeyLogoBlob(logoBlobFrom),
                    TKeyBlock(blockTabletFrom),
                    barrierFrom,
                    stage));
        }

        void HandleInitialEventLegacyProtocol(const TEventInfo& evInfo) {
            // this is the first message, so after full sync source node
            // will be synced by lsn obtained from SyncLog
            TSyncState newSyncState(Db->GetVDiskIncarnationGuid(),
                    Db->LsnMngr->GetConfirmedLsnForSyncLog());
            CreateActorForLegacyProtocol(evInfo, newSyncState);
        }

        void HandleConsequentEventLegacyProtocol(const TEventInfo& evInfo) {
            TSyncState clientSyncState(SyncStateFromSyncState(evInfo.Record.GetSyncState()));
            CreateActorForLegacyProtocol(evInfo, clientSyncState);
        }

        void HandleInitialEventUnorderedDataProtocol(const TEventInfo& evInfo) {
            TSyncState newSyncState(Db->GetVDiskIncarnationGuid(),
                    Db->LsnMngr->GetConfirmedLsnForSyncLog());
            
            std::optional<TActorId> oldActorId = DeleteUnorderedDataSession(evInfo.SessionKey);
            if (oldActorId) {
                Send(*oldActorId, new TEvents::TEvPoisonPill);
            }

            TActorId actorId = Register(CreateHullSyncFullActorUnorderedDataProtocol(
                    Db->Config,
                    HullCtx,
                    SelfId(),
                    Hull->GetIndexSnapshot(),
                    newSyncState,
                    SelfVDiskId,
                    IFaceMonGroup,
                    FullSyncGroup,
                    evInfo.Event));

            UnorderedDataFullSyncSessions[evInfo.SessionKey] = actorId;
            UnorderedDataFullSyncSessionLookup[actorId] = evInfo.SessionKey;
        }

        void HandleConsequentEventUnorderedDataProtocol(const TEventInfo& evInfo) {
            auto it = UnorderedDataFullSyncSessions.find(evInfo.SessionKey);
            if (it == UnorderedDataFullSyncSessions.end()) {
                // no session found
                // either protocol violation, or race on generation change
                RespondWithErroneousStatus(evInfo, {}, NKikimrProto::ERROR);
            } else {
                Send(evInfo.Event->Forward(it->second));
            }
        }

        std::optional<TActorId> DeleteUnorderedDataSession(const TSessionKey& sessionKey) {
            auto it1 = UnorderedDataFullSyncSessions.find(sessionKey);
            if (it1 != UnorderedDataFullSyncSessions.end()) {
                // Recipient demanded fullsync restart, kill existing actor
                TActorId actorId = it1->second;
                Send(actorId, new TEvents::TEvPoisonPill);
                UnorderedDataFullSyncSessions.erase(it1);
                bool erased = UnorderedDataFullSyncSessionLookup.erase(actorId);
                Y_VERIFY(erased);
                return actorId;
            }
            return std::nullopt;
        }

        void Handle(TEvents::TEvGone::TPtr& ev) {
            auto it1 = UnorderedDataFullSyncSessionLookup.find(ev->Sender);
            if (it1 != UnorderedDataFullSyncSessionLookup.end()) {
                TSessionKey sessionKey = it1->second;
                bool erased = UnorderedDataFullSyncSessions.erase(sessionKey);
                Y_VERIFY(erased);
                UnorderedDataFullSyncSessionLookup.erase(it1);
            }
        }

        void TerminateAllUnorderedDataSessions() {
            for (const auto& [_, actorId] : UnorderedDataFullSyncSessions) {
                Send(actorId, new TEvents::TEvPoisonPill);
            }
            UnorderedDataFullSyncSessionLookup.clear();
            UnorderedDataFullSyncSessions.clear();
        }

        void HandlePoison() {
            TerminateAllUnorderedDataSessions();
            PassAway();
        }

        void Handle(TEvBlobStorage::TEvVSyncFull::TPtr& ev) {
            TEventInfo evInfo(ev);
            LOG_DEBUG_S(TActivationContext::AsActorContext(), BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                    << "TVSyncFullHandler: Handle TEvVSyncFull, ev# " << ev->ToString()
                    << " From SyncState# " << evInfo.ClientSyncState.ToString()
                    << " Marker# BSVSFH04");

            IFaceMonGroup->SyncFullMsgs()++;

            if (!CheckEvent(evInfo)) {
                LOG_DEBUG_S(TActivationContext::AsActorContext(), BS_SYNCJOB, Db->VCtx->VDiskLogPrefix
                        << "TVSyncFullHandler: TEvVSyncFull event discarded, Marker# BSVSFH05");
                return;
            }

            switch (evInfo.Protocol) {
                case NKikimrBlobStorage::EFullSyncProtocol::Legacy: {
                    if (ev->Get()->IsInitial()) {
                        HandleInitialEventLegacyProtocol(ev);
                    } else {
                        HandleConsequentEventLegacyProtocol(ev);
                    }
                    break;
                }
                case NKikimrBlobStorage::EFullSyncProtocol::UnorderedData:
                    if (ev->Get()->IsInitial()) {
                        HandleInitialEventUnorderedDataProtocol(ev);
                    } else {
                        HandleConsequentEventUnorderedDataProtocol(ev);
                    }
                    break;
                default:
                    // unknown protocol, respond with erroneous status
                    RespondWithErroneousStatus(ev, {}, NKikimrProto::ERROR);
                    
            }
        }


        void Handle(const TEvVGenerationChange::TPtr& ev) {
            SelfVDiskId = ev->Get()->NewVDiskId;
            // We must restart all fullsync sessions when VDisk generation changes
            TerminateAllUnorderedDataSessions();
        }


        STRICT_STFUNC(StateFunc,
            hFunc(TEvents::TEvGone, Handle)
            cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison)
            hFunc(TEvVGenerationChange, Handle)
            hFunc(TEvBlobStorage::TEvVSyncFull, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNC_FULL_HANDLER;
        }

        TVSyncFullHandler(const TIntrusivePtr<TDb> &db,
                          const TIntrusivePtr<THullCtx> &hullCtx,
                          const TVDiskID &selfVDiskId,
                          const TActorId &parentId,
                          const std::shared_ptr<THull> &hull,
                          const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                          const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                          ui64 dbBirthLsn)
            : TActorBootstrapped<TVSyncFullHandler>()
            , Db(db)
            , HullCtx(hullCtx)
            , SelfVDiskId(selfVDiskId)
            , ParentId(parentId)
            , Hull(hull)
            , IFaceMonGroup(ifaceMonGroup)
            , FullSyncGroup(fullSyncGroup)
            , DbBirthLsn(dbBirthLsn)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateHullSyncFullHandler
    // VDisk Skeleton Handler for TEvVSyncFull event
    // MUST work on the same mailbox as Skeleton
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateHullSyncFullHandler(const TIntrusivePtr<TDb> &db,
                                      const TIntrusivePtr<THullCtx> &hullCtx,
                                      const TVDiskID &selfVDiskId,
                                      const TActorId &parentId,
                                      const std::shared_ptr<THull> &hull,
                                      const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                                      const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                                      ui64 dbBirthLsn) {
        return new TVSyncFullHandler(db, hullCtx, selfVDiskId, parentId, hull,
                    ifaceMonGroup, fullSyncGroup, dbBirthLsn);
    }



} // NKikimr
