#pragma once
#include "defs.h"
#include "schema.h"

#include <ydb/core/tx/sequenceshard/public/events.h>

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/core/protos/counters_sequenceshard.pb.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#define SLOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::SEQUENCESHARD, LogPrefix << stream)
#define SLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::SEQUENCESHARD, LogPrefix << stream)
#define SLOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::SEQUENCESHARD, LogPrefix << stream)

namespace NKikimr {
namespace NSequenceShard {

    class TSequenceShard
        : public TActor<TSequenceShard>
        , public NTabletFlatExecutor::TTabletExecutedFlat
    {
    private:
        using Schema = TSequenceShardSchema;

    public:
        struct TLogPrefix {
            TLogPrefix(TSequenceShard* self)
                : Self(self)
            { }

            TSequenceShard* Self;
        };

        class TTxBase : public NTabletFlatExecutor::TTransactionBase<TSequenceShard> {
        public:
            TTxBase(TSequenceShard* self)
                : TTransactionBase(self)
                , LogPrefix(self)
            { }

        protected:
            const TLogPrefix LogPrefix;
        };

    public:
        TSequenceShard(const TActorId& tablet, TTabletStorageInfo* info);
        ~TSequenceShard();

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::SEQUENCESHARD_ACTOR;
        }

    private:
        void OnDetach(const TActorContext& ctx) override;
        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
        void OnActivateExecutor(const TActorContext& ctx) override;
        void DefaultSignalTabletActive(const TActorContext& ctx) override;
        // TODO: bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;

    private:
        struct TTxInitSchema;
        struct TTxInit;
        struct TTxMarkSchemeShardPipe;
        struct TTxCreateSequence;
        struct TTxAllocateSequence;
        struct TTxDropSequence;
        struct TTxUpdateSequence;
        struct TTxFreezeSequence;
        struct TTxRestoreSequence;
        struct TTxRedirectSequence;
        struct TTxGetSequence;

        void RunTxInitSchema(const TActorContext& ctx);
        void RunTxInit(const TActorContext& ctx);

    private:
        void ResetState();
        void ResetCounters();

        bool CheckPipeRequest(const TActorId& serverId) const noexcept;

    private:
        STFUNC(StateInit);
        STFUNC(StateWork);

        void SwitchToWork(const TActorContext& ctx);

        void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev);
        void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev);
        void Handle(TEvSequenceShard::TEvMarkSchemeShardPipe::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvCreateSequence::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvAllocateSequence::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvDropSequence::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvUpdateSequence::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvFreezeSequence::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvRestoreSequence::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvRedirectSequence::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvSequenceShard::TEvGetSequence::TPtr& ev, const TActorContext& ctx);

    private:
        struct TPipeInfo {
            TActorId ServerId;
            TActorId ClientId;
            ui64 SchemeShardId = 0;
            ui64 Generation = 0;
            ui64 Round = 0;
        };

        struct TSequence {
            TPathId PathId;
            i64 MinValue = 1;
            i64 MaxValue = Max<i64>();
            i64 StartValue = 1;
            i64 NextValue = 1;
            bool NextUsed = false;
            ui64 Cache = 1;
            i64 Increment = 1;
            bool Cycle = false;

            Schema::ESequenceState State = Schema::ESequenceState::Active;
            ui64 MovedTo = 0;
        };

    private:
        // Logging support
        const TLogPrefix LogPrefix;
        // Counters support
        THolder<TTabletCountersBase> TabletCountersPtr;
        TTabletCountersBase* TabletCounters;
        // System params
        ui64 CurrentSchemeShardId = 0;
        std::optional<NKikimrSubDomains::TProcessingParams> ProcessingParams;
        // Current pipes and schemeshard marks
        THashMap<TActorId, TPipeInfo> PipeInfos;
        THashMap<ui64, std::pair<ui64, ui64>> SchemeShardRounds;
        // Current sequences
        THashMap<TPathId, TSequence> Sequences;
    };

} // namespace NSequenceShard
} // namespace NKikimr
