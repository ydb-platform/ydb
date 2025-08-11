#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/bridge.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h> // for TEvConfigureProxy

namespace NKikimr::NBridge {

    class TSyncerActor : public TActorBootstrapped<TSyncerActor> {
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TBridgePileId TargetPileId;
        TBridgePileId SourcePileId;
        const TGroupId GroupId;
        TGroupId SourceGroupId;
        TGroupId TargetGroupId;
        std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> StorageConfig;
        TBridgeInfo::TPtr BridgeInfo;
        TString LogId;

    public:
        TSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TBridgePileId targetPileId, TGroupId groupId);

        void Bootstrap();
        void PassAway() override;

        void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev);

        void Terminate(std::optional<TString> errorReason);

        STFUNC(StateFunc);

        void Handle(TEvNodeWardenStorageConfig::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Main sync logic

        struct TQueryPayload {
            bool ToTargetGroup;
        };

        const ui32 MaxQueriesInFlight = 16;
        ui32 QueriesInFlight = 0;
        THashMap<ui64, TQueryPayload> Payloads;
        std::deque<std::unique_ptr<IEventHandle>> PendingQueries;
        ui64 NextCookie = 1;

        bool Errors = false;

        void InitiateSync();
        void DoMergeLoop();

        template<typename T, typename TCallback>
        bool DoMergeEntities(std::deque<T>& source, std::deque<T>& target, bool sourceFinished, bool targetFinished,
            TCallback&& merge);

        void IssueQuery(bool toTargetGroup, std::unique_ptr<IEventBase> ev, TQueryPayload queryPayload = {});
        void Handle(TEvBlobStorage::TEvBlockResult::TPtr ev);
        void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);
        void Handle(TEvBlobStorage::TEvPutResult::TPtr ev);
        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);
        TQueryPayload OnQueryFinished(ui64 cookie, bool success);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Per-group assimilation status

        struct TAssimilateState {
            std::optional<ui64> SkipBlocksUpTo;
            std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
            std::optional<TLogoBlobID> SkipBlobsUpTo;
            std::deque<TEvBlobStorage::TEvAssimilateResult::TBlock> Blocks;
            std::deque<TEvBlobStorage::TEvAssimilateResult::TBarrier> Barriers;
            std::deque<TEvBlobStorage::TEvAssimilateResult::TBlob> Blobs;
            bool RequestInFlight = false;
            bool BlocksFinished = false;
            bool BarriersFinished = false;
            bool BlobsFinished = false;
        };
        std::array<TAssimilateState, 2> GroupAssimilateState; // indexed by toTargetGroup
        TAssimilateState *TargetState = nullptr;
        TAssimilateState *SourceState = nullptr;

        void IssueAssimilateRequest(bool toTargetGroup);
        void Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev);
    };

} // NKikimr::NBridge
