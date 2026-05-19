#include "blobstorage_syncfullhandler.h"
#include "blobstorage_db.h"
#include "blobstorage_syncfull.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_SYNCJOB

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
        const TActorId SyncLogActorId;
        std::shared_ptr<THull> Hull;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        std::shared_ptr<NMonGroup::TFullSyncGroup> FullSyncGroup;
        const ui64 DbBirthLsn;

        using TSessionKey = std::pair<TVDiskID, TVDiskID>; // { SourceVDisk, TargetVDisk }
        THashMap<TSessionKey, TActorId> UnorderedDataFullSyncSessions;
        THashMap<TActorId, TSessionKey> UnorderedDataFullSyncSessionLookup;

        friend class TActorBootstrapped<TVSyncFullHandler>;

        struct TEventInfo {
            TEventInfo(TEvBlobStorage::TEvVSyncFull::TPtr& ev, bool enablePhantomFlagStorage)
                : Event(ev)
                , Record(ev->Get()->Record)
                , SourceVDisk(VDiskIDFromVDiskID(Record.GetSourceVDiskID()))
                , TargetVDisk(VDiskIDFromVDiskID(Record.GetTargetVDiskID()))
                , SessionKey(SourceVDisk, TargetVDisk)
                , ClientSyncState(SyncStateFromSyncState(Record.GetSyncState()))
                , Protocol(enablePhantomFlagStorage
                        ? ev->Get()->GetProtocol()
                        : NKikimrBlobStorage::EFullSyncProtocol::Legacy)
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
                    IFaceMonGroup->SyncFullResMsgsPtr(), nullptr, evInfo.Event->GetChannel(),
                    evInfo.Protocol);
            SendVDiskResponse(TActivationContext::AsActorContext(), evInfo.Event->Sender, result.release(),
                    evInfo.Event->Cookie, HullCtx->VCtx, {});
        }

        bool CheckEvent(const TEventInfo& evInfo) {
            // check that the disk is from this group
            if (!SelfVDiskId.SameGroupAndGeneration(evInfo.SourceVDisk) ||
                    !SelfVDiskId.SameDisk(evInfo.TargetVDisk)) {
                YDB_LOG_CTX_DEBUG(TActivationContext::AsActorContext(), "TVSyncFullHandler: Invalid VDisk ids: Marker# BSVSFH07",
                    {"#_Db->VCtx->VDiskLogPrefix", Db->VCtx->VDiskLogPrefix},
                    {"SelfVDiskId", SelfVDiskId.ToString()},
                    {"SorceVDiskId", evInfo.SourceVDisk.ToString()},
                    {"TargetVDiskId", evInfo.TargetVDisk.ToString()});
                RespondWithErroneousStatus(evInfo, {}, NKikimrProto::ERROR);
                return false;
            }

            // check disk guid and start from the beginning if it has changed
            TVDiskIncarnationGuid incarnationGuid = Db->GetVDiskIncarnationGuid();
            if (incarnationGuid != evInfo.ClientSyncState.Guid) {
                YDB_LOG_CTX_DEBUG(TActivationContext::AsActorContext(), "TVSyncFullHandler: GUID CHANGED; Marker# BSVSFH02",
                    {"#_Db->VCtx->VDiskLogPrefix", Db->VCtx->VDiskLogPrefix},
                    {"SourceVDisk", evInfo.SourceVDisk},
                    {"DbBirthLsn", DbBirthLsn},
                    {"VDiskIncarnationGuid", incarnationGuid},
                    {"ClientGuid", evInfo.ClientSyncState.Guid});
                RespondWithErroneousStatus(evInfo, TSyncState(incarnationGuid, DbBirthLsn), NKikimrProto::NODATA);
                return false;
            }

            Y_VERIFY_DEBUG_S(evInfo.SourceVDisk != SelfVDiskId, HullCtx->VCtx->VDiskLogPrefix);

            YDB_LOG_CTX_DEBUG(TActivationContext::AsActorContext(), "TVSyncFullHandler: Marker# BSVSFH90",
                {"#_Db->VCtx->VDiskLogPrefix", Db->VCtx->VDiskLogPrefix},
                {"syncedLsn", evInfo.ClientSyncState.SyncedLsn},
                {"SourceVDisk", evInfo.SourceVDisk},
                {"TargetVDisk", evInfo.TargetVDisk});
            return true;
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);

            YDB_LOG_CTX_DEBUG(TActivationContext::AsActorContext(), "TVSyncFullHandler: Bootstrap: Marker# BSVSFH01",
                {"#_Db->VCtx->VDiskLogPrefix", Db->VCtx->VDiskLogPrefix},
                {"SelfVDiskId", SelfVDiskId});
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

            DeleteUnorderedDataSession(evInfo.SessionKey);

            TActorId actorId = Register(CreateHullSyncFullActorUnorderedDataProtocol(
                    Db->Config,
                    HullCtx,
                    SelfId(),
                    SyncLogActorId,
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

        void DeleteUnorderedDataSession(const TSessionKey& sessionKey) {
            auto it1 = UnorderedDataFullSyncSessions.find(sessionKey);
            if (it1 != UnorderedDataFullSyncSessions.end()) {
                // Recipient demanded fullsync restart, kill existing actor
                TActorId actorId = it1->second;
                Send(actorId, new TEvents::TEvPoisonPill);
                UnorderedDataFullSyncSessions.erase(it1);
                bool erased = UnorderedDataFullSyncSessionLookup.erase(actorId);
                Y_VERIFY(erased);
            }
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
            TEventInfo evInfo(ev, HullCtx->VCfg->EnablePhantomFlagStorage);
            YDB_LOG_CTX_DEBUG(TActivationContext::AsActorContext(), "TVSyncFullHandler: Handle TEvVSyncFull, From Marker# BSVSFH04",
                {"#_Db->VCtx->VDiskLogPrefix", Db->VCtx->VDiskLogPrefix},
                {"ev", ev->ToString()},
                {"SyncState", evInfo.ClientSyncState.ToString()});

            IFaceMonGroup->SyncFullMsgs()++;

            if (!CheckEvent(evInfo)) {
                YDB_LOG_CTX_DEBUG(TActivationContext::AsActorContext(), "TVSyncFullHandler: TEvVSyncFull event discarded, Marker# BSVSFH05",
                    {"#_Db->VCtx->VDiskLogPrefix", Db->VCtx->VDiskLogPrefix});
                return;
            }

            switch (evInfo.Protocol) {
                case NKikimrBlobStorage::EFullSyncProtocol::Legacy: {
                    if (ev->Get()->IsInitial()) {
                        HandleInitialEventLegacyProtocol(evInfo);
                    } else {
                        HandleConsequentEventLegacyProtocol(evInfo);
                    }
                    break;

                }
                case NKikimrBlobStorage::EFullSyncProtocol::UnorderedData:
                    if (ev->Get()->IsInitial()) {
                        HandleInitialEventUnorderedDataProtocol(evInfo);
                    } else {
                        HandleConsequentEventUnorderedDataProtocol(evInfo);
                    }
                    break;
                default:
                    // unknown protocol, respond with erroneous status
                    RespondWithErroneousStatus(evInfo, {}, NKikimrProto::ERROR);

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
                          const TActorId& syncLogActorId,
                          const std::shared_ptr<THull> &hull,
                          const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                          const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                          ui64 dbBirthLsn)
            : TActorBootstrapped<TVSyncFullHandler>()
            , Db(db)
            , HullCtx(hullCtx)
            , SelfVDiskId(selfVDiskId)
            , ParentId(parentId)
            , SyncLogActorId(syncLogActorId)
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
                                      const TActorId& syncLogActorId,
                                      const std::shared_ptr<THull> &hull,
                                      const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
                                      const std::shared_ptr<NMonGroup::TFullSyncGroup>& fullSyncGroup,
                                      ui64 dbBirthLsn) {
        return new TVSyncFullHandler(db, hullCtx, selfVDiskId, parentId, syncLogActorId, hull,
                    ifaceMonGroup, fullSyncGroup, dbBirthLsn);
    }



} // NKikimr
