#include "datashard_repl_offsets_client.h"
#include "datashard_impl.h"

#include <ydb/core/base/tablet_pipecache.h>

namespace NKikimr::NDataShard {

    class TReplicationSourceOffsetsClient
        : public TActorBootstrapped<TReplicationSourceOffsetsClient>
    {
        using TEvPrivate = TDataShard::TEvPrivate;

    public:
        TReplicationSourceOffsetsClient(TActorId owner, ui64 srcTabletId, const TPathId& pathId)
            : Owner(owner)
            , SrcTabletId(srcTabletId)
            , PathId(pathId)
            , PipeCache(MakePipePerNodeCacheID(false))
            , Result(new TEvPrivate::TEvReplicationSourceOffsets(srcTabletId, pathId))
        { }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TX_DATASHARD_SOURCE_OFFSETS_CLIENT;
        }

        void Bootstrap() {
            Become(&TThis::StateWork);
            StartNewRead();
        }

        void PassAway() override {
            if (CurrentReadId != 0) {
                CancelRead(CurrentReadId);
                CurrentReadId = 0;
            }
            Send(PipeCache, new TEvPipeCache::TEvUnlink(0));
            TActorBootstrapped::PassAway();
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                sFunc(TEvents::TEvPoison, PassAway);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvDataShard::TEvReplicationSourceOffsets, Handle);
            }
        }

        void StartNewRead() {
            auto req = MakeHolder<TEvDataShard::TEvGetReplicationSourceOffsets>(++CurrentReadId, PathId);
            req->Record.SetFromSourceId(NextSourceId);
            req->Record.SetFromSplitKeyId(NextSplitKeyId);
            Send(PipeCache, new TEvPipeCache::TEvForward(req.Release(), SrcTabletId, /* subscribe */ true));
            ExpectedSeqNo = 1;
        }

        void CancelRead(ui64 readId) {
            auto req = MakeHolder<TEvDataShard::TEvReplicationSourceOffsetsCancel>(readId);
            Send(PipeCache, new TEvPipeCache::TEvForward(req.Release(), SrcTabletId, /* subscribe */ false));
        }

        void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            auto* msg = ev->Get();
            if (!msg->NotDelivered) {
                CancelRead(CurrentReadId);
            }

            // TODO: add some exponential randomized delay between retries
            StartNewRead();
        }

        void Handle(TEvDataShard::TEvReplicationSourceOffsets::TPtr& ev) {
            auto* msg = ev->Get();

            if (msg->Record.GetReadId() != CurrentReadId) {
                // Ignore replies from unexpected reads
                CancelRead(msg->Record.GetReadId());
                return;
            }

            if (msg->Record.GetSeqNo() != ExpectedSeqNo) {
                // Ignore out-of-order messages
                // It should not be possible anyway
                return;
            }

            for (const auto& result : msg->Record.GetResult()) {
                const auto& source = result.GetSource();
                const auto& splitKey = result.GetSplitKey();
                auto& entry = Result->SourceOffsets[source].emplace_back();
                entry.SplitKey.Parse(splitKey);
                if (result.HasMaxOffset()) {
                    entry.MaxOffset = result.GetMaxOffset();
                }
                NextSourceId = result.GetSourceId();
                NextSplitKeyId = result.GetSplitKeyId();
                if (++NextSplitKeyId == 0) {
                    ++NextSourceId;
                }
            }

            Send(ev->Sender, new TEvDataShard::TEvReplicationSourceOffsetsAck(CurrentReadId, ExpectedSeqNo));
            ++ExpectedSeqNo;

            if (msg->Record.GetEndOfStream()) {
                Send(Owner, Result.Release());
                CurrentReadId = 0;
                PassAway();
            }
        }

    private:
        const TActorId Owner;
        const ui64 SrcTabletId;
        const TPathId PathId;
        const TActorId PipeCache;
        THolder<TEvPrivate::TEvReplicationSourceOffsets> Result;
        ui64 NextSourceId = 0;
        ui64 NextSplitKeyId = 0;
        ui64 CurrentReadId = 0;
        ui64 ExpectedSeqNo = 0;
    };

    IActor* CreateReplicationSourceOffsetsClient(TActorId owner, ui64 srcTabletId, const TPathId& pathId) {
        return new TReplicationSourceOffsetsClient(owner, srcTabletId, pathId);
    }

} // namespace NKikimr::NDataShard
