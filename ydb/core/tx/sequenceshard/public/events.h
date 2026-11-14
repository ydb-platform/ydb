#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/tx_sequenceshard.pb.h>

namespace NKikimr {
namespace NSequenceShard {

    namespace TEvSequenceShard {
        enum EEv {
            EvMarkSchemeShardPipe = EventSpaceBegin(TKikimrEvents::ES_SEQUENCESHARD),
            EvCreateSequence,
            EvCreateSequenceResult,
            EvAllocateSequence,
            EvAllocateSequenceResult,
            EvDropSequence,
            EvDropSequenceResult,
            EvUpdateSequence,
            EvUpdateSequenceResult,
            EvFreezeSequence,
            EvFreezeSequenceResult,
            EvRestoreSequence,
            EvRestoreSequenceResult,
            EvRedirectSequence,
            EvRedirectSequenceResult,
            EvGetSequence,
            EvGetSequenceResult,
            EvAdvanceSequence,
            EvAdvanceSequenceResult,
            EvEnd,
        };

        static_assert(TKikimrEvents::ES_SEQUENCESHARD == 4216,
            "Expected TKikimrEvents::ES_SEQUENCESHARD == 4216");
        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SEQUENCESHARD),
            "Expected EvEnd < EventSpaceEnd(TKikimrEvents::ES_SEQUENCESHARD)");

        struct TEvMarkSchemeShardPipe
            : public TEventPB<TEvMarkSchemeShardPipe, NKikimrTxSequenceShard::TEvMarkSchemeShardPipe, EvMarkSchemeShardPipe>
        {
            TEvMarkSchemeShardPipe() = default;

            TEvMarkSchemeShardPipe(ui64 schemeShardId, ui64 generation, ui64 round);
        };

        struct TEvCreateSequence
            : public TEventPB<TEvCreateSequence, NKikimrTxSequenceShard::TEvCreateSequence, EvCreateSequence>
        {
            TEvCreateSequence() = default;

            explicit TEvCreateSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;

            struct TBuilder {
                THolder<TEvCreateSequence> Msg;

                TBuilder&& SetMinValue(i64 minValue) &&;

                TBuilder&& SetMaxValue(i64 maxValue) &&;

                TBuilder&& SetStartValue(i64 startValue) &&;

                TBuilder&& SetCache(ui64 cache) &&;

                TBuilder&& SetIncrement(i64 increment) &&;

                TBuilder&& SetCycle(bool cycle) &&;

                TBuilder&& SetFrozen(bool frozen) &&;

                THolder<TEvCreateSequence> Done() && {
                    return std::move(Msg);
                }
            };

            static TBuilder Build(const TPathId& pathId) {
                return TBuilder{ MakeHolder<TEvCreateSequence>(pathId) };
            }
        };

        struct TEvCreateSequenceResult
            : public TEventPB<TEvCreateSequenceResult, NKikimrTxSequenceShard::TEvCreateSequenceResult, EvCreateSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvCreateSequenceResult::EStatus;

            TEvCreateSequenceResult() = default;

            TEvCreateSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvAllocateSequence
            : public TEventPB<TEvAllocateSequence, NKikimrTxSequenceShard::TEvAllocateSequence, EvAllocateSequence>
        {
            TEvAllocateSequence() = default;

            explicit TEvAllocateSequence(const TPathId& pathId, ui64 cache = 0);

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;
        };

        struct TEvAllocateSequenceResult
            : public TEventPB<TEvAllocateSequenceResult, NKikimrTxSequenceShard::TEvAllocateSequenceResult, EvAllocateSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvAllocateSequenceResult::EStatus;

            TEvAllocateSequenceResult() = default;

            TEvAllocateSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvDropSequence
            : public TEventPB<TEvDropSequence, NKikimrTxSequenceShard::TEvDropSequence, EvDropSequence>
        {
            TEvDropSequence() = default;

            explicit TEvDropSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;
        };

        struct TEvDropSequenceResult
            : public TEventPB<TEvDropSequenceResult, NKikimrTxSequenceShard::TEvDropSequenceResult, EvDropSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvDropSequenceResult::EStatus;

            TEvDropSequenceResult() = default;

            TEvDropSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvUpdateSequence
            : public TEventPB<TEvUpdateSequence, NKikimrTxSequenceShard::TEvUpdateSequence, EvUpdateSequence>
        {
            TEvUpdateSequence() = default;

            explicit TEvUpdateSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;

            struct TBuilder {
                THolder<TEvUpdateSequence> Msg;

                TBuilder&& SetMinValue(i64 minValue) &&;

                TBuilder&& SetMaxValue(i64 maxValue) &&;

                TBuilder&& SetStartValue(i64 startValue) &&;

                TBuilder&& SetNextValue(i64 nextValue) &&;

                TBuilder&& SetNextUsed(bool nextUsed) &&;

                TBuilder&& SetCache(ui64 cache) &&;

                TBuilder&& SetIncrement(i64 increment) &&;

                TBuilder&& SetCycle(bool cycle) &&;

                THolder<TEvUpdateSequence> Done() && {
                    return std::move(Msg);
                }
            };

            static TBuilder Build(const TPathId& pathId) {
                return TBuilder{ MakeHolder<TEvUpdateSequence>(pathId) };
            }
        };

        struct TEvUpdateSequenceResult
            : public TEventPB<TEvUpdateSequenceResult, NKikimrTxSequenceShard::TEvUpdateSequenceResult, EvUpdateSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvUpdateSequenceResult::EStatus;

            TEvUpdateSequenceResult() = default;

            TEvUpdateSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvFreezeSequence
            : public TEventPB<TEvFreezeSequence, NKikimrTxSequenceShard::TEvFreezeSequence, EvFreezeSequence>
        {
            TEvFreezeSequence() = default;

            explicit TEvFreezeSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;
        };

        struct TEvFreezeSequenceResult
            : public TEventPB<TEvFreezeSequenceResult, NKikimrTxSequenceShard::TEvFreezeSequenceResult, EvFreezeSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvFreezeSequenceResult::EStatus;

            TEvFreezeSequenceResult() = default;

            TEvFreezeSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvRestoreSequence
            : public TEventPB<TEvRestoreSequence, NKikimrTxSequenceShard::TEvRestoreSequence, EvRestoreSequence>
        {
            TEvRestoreSequence() = default;

            explicit TEvRestoreSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            TEvRestoreSequence(const TPathId& pathId, const NKikimrTxSequenceShard::TEvFreezeSequenceResult& record) {
                SetPathId(pathId);
                InitFrom(record);
            }

            TEvRestoreSequence(const TPathId& pathId, const NKikimrTxSequenceShard::TEvGetSequenceResult& record) {
                SetPathId(pathId);
                InitFrom(record);
            }

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;

            void InitFrom(const NKikimrTxSequenceShard::TEvFreezeSequenceResult& record);

            void InitFrom(const NKikimrTxSequenceShard::TEvGetSequenceResult& record);
        };

        struct TEvRestoreSequenceResult
            : public TEventPB<TEvRestoreSequenceResult, NKikimrTxSequenceShard::TEvRestoreSequenceResult, EvRestoreSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvRestoreSequenceResult::EStatus;

            TEvRestoreSequenceResult() = default;

            TEvRestoreSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvRedirectSequence
            : public TEventPB<TEvRedirectSequence, NKikimrTxSequenceShard::TEvRedirectSequence, EvRedirectSequence>
        {
            TEvRedirectSequence() = default;

            TEvRedirectSequence(const TPathId& pathId, ui64 redirectTo);

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;
        };

        struct TEvRedirectSequenceResult
            : public TEventPB<TEvRedirectSequenceResult, NKikimrTxSequenceShard::TEvRedirectSequenceResult, EvRedirectSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvRedirectSequenceResult::EStatus;

            TEvRedirectSequenceResult() = default;

            TEvRedirectSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvGetSequence
            : public TEventPB<TEvGetSequence, NKikimrTxSequenceShard::TEvGetSequence, EvGetSequence>
        {
            TEvGetSequence() = default;

            explicit TEvGetSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;
        };

        struct TEvGetSequenceResult
            : public TEventPB<TEvGetSequenceResult, NKikimrTxSequenceShard::TEvGetSequenceResult, EvGetSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvGetSequenceResult::EStatus;

            TEvGetSequenceResult() = default;

            TEvGetSequenceResult(EStatus status, ui64 origin);
        };

        struct TEvAdvanceSequence
            : public TEventPB<TEvAdvanceSequence, NKikimrTxSequenceShard::TEvAdvanceSequence, EvAdvanceSequence>
        {
            TEvAdvanceSequence() = default;

            explicit TEvAdvanceSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId);

            TPathId GetPathId() const;

            struct TBuilder {
                THolder<TEvAdvanceSequence> Msg;

                TBuilder&& SetObserved(i64 observedMin, i64 observedMax) &&;

                THolder<TEvAdvanceSequence> Done() && {
                    return std::move(Msg);
                }
            };

            static TBuilder Build(const TPathId& pathId) {
                return TBuilder{ MakeHolder<TEvAdvanceSequence>(pathId) };
            }
        };

        struct TEvAdvanceSequenceResult
            : public TEventPB<TEvAdvanceSequenceResult, NKikimrTxSequenceShard::TEvAdvanceSequenceResult, EvAdvanceSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvAdvanceSequenceResult::EStatus;

            TEvAdvanceSequenceResult() = default;

            TEvAdvanceSequenceResult(EStatus status, ui64 origin);
        };

    };

} // namespace NSequenceShard
} // namespace NKikimr
