#pragma once

#include "defs.h"
#include "blobstorage_syncer_data.h"
#include <ydb/core/blobstorage/vdisk/common/sublog.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_syncneighbors.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include "blobstorage_syncer_localwriter.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {

    class TSyncerContext;

    namespace NSyncer {

        ////////////////////////////////////////////////////////////////////////////
        // TSjOutcome -- Syncer Job Outcome
        // Methods of Syncer Job FSM that change its state return this structure
        ////////////////////////////////////////////////////////////////////////////
        struct TSjOutcome {
            // we can send a message as a result ...
            std::unique_ptr<IEventBase> Ev;
            TActorId To;
            // ... or run a actor
            std::unique_ptr<IActor> ActorActivity;
            bool RunInBatchPool = false;
            // ... or/and finish working
            bool Die = false;

            static TSjOutcome Event(const TActorId &to, std::unique_ptr<IEventBase> &&ev) {
                TSjOutcome outcome;
                outcome.To = to;
                outcome.Ev = std::move(ev);
                return outcome;
            }

            static TSjOutcome Actor(std::unique_ptr<IActor> actor, bool runInBatchPool = false) {
                TSjOutcome outcome;
                outcome.ActorActivity = std::move(actor);
                outcome.RunInBatchPool = runInBatchPool;
                return outcome;
            }

            static TSjOutcome Death() {
                TSjOutcome outcome;
                outcome.Die = true;
                return outcome;
            }

            static TSjOutcome Nothing() {
                return TSjOutcome();
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSjCtx - Syncer Job Context
        // We pass this context to every syncer job task, it isolates TSyncerJobTask
        // from complex environment (configuration, etc)
        ////////////////////////////////////////////////////////////////////////////
        struct TSjCtx {
           // TSjCtx is reconstructed during BlobStorage Group reconfiguration,
            // so we can keep SelfVDiskId const
            const TVDiskID SelfVDiskId;
            const TIntrusivePtr<TSyncerContext> SyncerCtx;

            // create TSjCtx using actual Db and GInfo
            static std::shared_ptr<TSjCtx> Create(
                                const TIntrusivePtr<TSyncerContext> &sc,
                                const TIntrusivePtr<TBlobStorageGroupInfo> &info);

        protected:
            TSjCtx(const TVDiskID &self, const TIntrusivePtr<TSyncerContext> &sc);
        };

        ////////////////////////////////////////////////////////////////////////////
        // TSyncerJobTask
        ////////////////////////////////////////////////////////////////////////////
        class TSyncerJobTask {
        public:
            // from protobuf
            using ESyncStatus = NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus;
            using TSyncStatusVal = NKikimrVDiskData::TSyncerVDiskEntry;

            ////////////////////////////////////////////////////////////////////////
            // EJobType
            // Syncer Job Task can be ordinary sync (EJustSync) or full recovery (EFullRecover)
            ////////////////////////////////////////////////////////////////////////
            enum EJobType {
                EFullRecover,
                EJustSync,
            };

            ////////////////////////////////////////////////////////////////////////
            // EPhase
            // Syncer Job Task has the following phases
            ////////////////////////////////////////////////////////////////////////
            enum EPhase {
                EStart,
                EWaitRemote,
                EWaitLocal,
                EFinished,
                ETerminated
            };

            ////////////////////////////////////////////////////////////////////////
            // TFullRecoverInfo
            // When Type=EFullRecover we store state here
            ////////////////////////////////////////////////////////////////////////
            struct TFullRecoverInfo {
                NKikimrBlobStorage::ESyncFullStage Stage = NKikimrBlobStorage::LogoBlobs;
                TKeyLogoBlob LogoBlobFrom = TKeyLogoBlob::First();
                TKeyBlock BlockTabletFrom = TKeyBlock::First();
                TKeyBarrier BarrierFrom = TKeyBarrier::First();
                ui64 VSyncFullMsgsReceived = 0;
            };

        public:
            ////////////////////////////////////////////////////////////////////////
            // Interface
            ////////////////////////////////////////////////////////////////////////
            static const char *EPhaseToStr(EPhase t);
            static const char *EJobTypeToStr(EJobType t);

            TSyncerJobTask(
                    EJobType type,
                    const TVDiskID &vdisk,
                    const TActorId &service,
                    const NSyncer::TPeerSyncState &peerState,
                    const std::shared_ptr<TSjCtx> &ctx);
            void Output(IOutputStream &str) const;
            TString ToString() const;
            TSjOutcome NextRequest();
            TSjOutcome Handle(TEvBlobStorage::TEvVSyncResult::TPtr &ev, const TActorId &parentId);
            TSjOutcome Handle(TEvBlobStorage::TEvVSyncFullResult::TPtr &ev, const TActorId &parentId);
            TSjOutcome Handle(TEvLocalSyncDataResult::TPtr &ev);
            TSjOutcome Terminate(ESyncStatus status);

            bool NeedCommit() const {
                bool need = OldSyncState != GetCurrent().SyncState;
                return need;
            }

            const NSyncer::TPeerSyncState &GetCurrent() const {
                return Current;
            }

            bool IsFullRecoveryTask() const {
                return Type == EFullRecover;
            }

        private:
            ////////////////////////////////////////////////////////////////////////
            // Private functions
            ////////////////////////////////////////////////////////////////////////
            void HandleStatusFlags(const NKikimrBlobStorage::TEvVSyncResult &record);
            bool CheckFragmentFormat(const TString &data);
            void PrepareToFullRecovery(const TSyncState &syncState);
            TSjOutcome ContinueInFullRecoveryMode();
            TSjOutcome ReplyAndDie(ESyncStatus status);
            TSjOutcome HandleOK(
                    TEvBlobStorage::TEvVSyncResult::TPtr &ev,
                    const TSyncState &newSyncState,
                    const TActorId &parentId);
            TSjOutcome HandleOK(
                    TEvBlobStorage::TEvVSyncFullResult::TPtr &ev,
                    const TSyncState &syncState,
                    const TActorId &parentId);
            TSjOutcome HandleRestart(const TSyncState &newSyncState);

            void SetSyncState(const TSyncState &syncState) {
                Current.SyncState = syncState;
            }

        public:
            const TVDiskID VDiskId;
            const TActorId ServiceId;
        private:
            // job type
            EJobType Type = EJustSync;
            // phase
            EPhase Phase = EStart;
            // previous sync state
            const TSyncState OldSyncState;
            NSyncer::TPeerSyncState Current;
            // stat counter
            ui64 BytesReceived = 0;
            // stat counter
            ui64 SentMsg = 0;
            // number of redirects, i.e. number of FullRecovery attempts
            ui64 RedirCounter = 0;
            // when server returned 'finished' (i.e. 'end of stream')
            bool EndOfStream = false;
            // if performing FullRecovery, we save state here
            TMaybe<TFullRecoverInfo> FullRecoverInfo;
            // context passed to us from outside
            std::shared_ptr<TSjCtx> Ctx;
            // Sublog
            TSublog<> Sublog;
        };

    } // NSyncer
} // NKikimr
