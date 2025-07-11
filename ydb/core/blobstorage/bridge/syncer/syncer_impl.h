#pragma once

#include "defs.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/bridge.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h> // for TEvConfigureProxy

namespace NKikimr::NStorage::NBridge {

    class TSyncerActor : public TActorBootstrapped<TSyncerActor> {
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TBridgePileId TargetPileId;
        TBridgePileId SourcePileId;
        TGroupId SourceGroupId;
        TGroupId TargetGroupId;
        TBridgeInfo::TPtr BridgeInfo;
        TString LogId;

    public:
        TSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TBridgePileId targetPileId);

        void Bootstrap();
        void PassAway() override;

        void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev);

        void Terminate(std::optional<TString> errorReason);

        STFUNC(StateFunc);

        void Handle(TEvNodeWardenStorageConfig::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Main sync logic

        void InitiateSync();
        void DoMergeLoop();

        template<typename T, typename TCallback>
        bool DoMergeEntities(std::deque<T>& source, std::deque<T>& target, bool sourceFinished, bool targetFinished,
            TCallback&& merge);

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
        THashMap<TGroupId, TAssimilateState> GroupAssimilateState;
        TAssimilateState *TargetState = nullptr;
        TAssimilateState *SourceState = nullptr;

        void IssueAssimilateRequest(TGroupId groupId);
        void Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev);
    };

} // NKikimr::NStorage::NBridge
