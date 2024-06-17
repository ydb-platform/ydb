#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/tx_sequenceshard.pb.h>

namespace NKikimr {
namespace NSequenceShard {

    struct TEvSequenceShard {
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

            TEvMarkSchemeShardPipe(ui64 schemeShardId, ui64 generation, ui64 round) {
                Record.SetSchemeShardId(schemeShardId);
                Record.SetGeneration(generation);
                Record.SetRound(round);
            }
        };

        struct TEvCreateSequence
            : public TEventPB<TEvCreateSequence, NKikimrTxSequenceShard::TEvCreateSequence, EvCreateSequence>
        {
            TEvCreateSequence() = default;

            explicit TEvCreateSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }

            struct TBuilder {
                THolder<TEvCreateSequence> Msg;

                TBuilder&& SetMinValue(i64 minValue) && {
                    Msg->Record.SetMinValue(minValue);
                    return std::move(*this);
                }

                TBuilder&& SetMaxValue(i64 maxValue) && {
                    Msg->Record.SetMaxValue(maxValue);
                    return std::move(*this);
                }

                TBuilder&& SetStartValue(i64 startValue) && {
                    Msg->Record.SetStartValue(startValue);
                    return std::move(*this);
                }

                TBuilder&& SetCache(ui64 cache) && {
                    Msg->Record.SetCache(cache);
                    return std::move(*this);
                }

                TBuilder&& SetIncrement(i64 increment) && {
                    Msg->Record.SetIncrement(increment);
                    return std::move(*this);
                }

                TBuilder&& SetCycle(bool cycle) && {
                    Msg->Record.SetCycle(cycle);
                    return std::move(*this);
                }

                TBuilder&& SetFrozen(bool frozen) && {
                    Msg->Record.SetFrozen(frozen);
                    return std::move(*this);
                }

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

            TEvCreateSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
        };

        struct TEvAllocateSequence
            : public TEventPB<TEvAllocateSequence, NKikimrTxSequenceShard::TEvAllocateSequence, EvAllocateSequence>
        {
            TEvAllocateSequence() = default;

            explicit TEvAllocateSequence(const TPathId& pathId, ui64 cache = 0) {
                SetPathId(pathId);
                Record.SetCache(cache);
            }

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }
        };

        struct TEvAllocateSequenceResult
            : public TEventPB<TEvAllocateSequenceResult, NKikimrTxSequenceShard::TEvAllocateSequenceResult, EvAllocateSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvAllocateSequenceResult::EStatus;

            TEvAllocateSequenceResult() = default;

            TEvAllocateSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
        };

        struct TEvDropSequence
            : public TEventPB<TEvDropSequence, NKikimrTxSequenceShard::TEvDropSequence, EvDropSequence>
        {
            TEvDropSequence() = default;

            explicit TEvDropSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }
        };

        struct TEvDropSequenceResult
            : public TEventPB<TEvDropSequenceResult, NKikimrTxSequenceShard::TEvDropSequenceResult, EvDropSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvDropSequenceResult::EStatus;

            TEvDropSequenceResult() = default;

            TEvDropSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
        };

        struct TEvUpdateSequence
            : public TEventPB<TEvUpdateSequence, NKikimrTxSequenceShard::TEvUpdateSequence, EvUpdateSequence>
        {
            TEvUpdateSequence() = default;

            explicit TEvUpdateSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }

            struct TBuilder {
                THolder<TEvUpdateSequence> Msg;

                TBuilder&& SetMinValue(i64 minValue) && {
                    Msg->Record.SetMinValue(minValue);
                    return std::move(*this);
                }

                TBuilder&& SetMaxValue(i64 maxValue) && {
                    Msg->Record.SetMaxValue(maxValue);
                    return std::move(*this);
                }

                TBuilder&& SetStartValue(i64 startValue) && {
                    Msg->Record.SetStartValue(startValue);
                    return std::move(*this);
                }

                TBuilder&& SetNextValue(i64 nextValue) && {
                    Msg->Record.SetNextValue(nextValue);
                    return std::move(*this);
                }

                TBuilder&& SetNextUsed(bool nextUsed) && {
                    Msg->Record.SetNextUsed(nextUsed);
                    return std::move(*this);
                }

                TBuilder&& SetCache(ui64 cache) && {
                    Msg->Record.SetCache(cache);
                    return std::move(*this);
                }

                TBuilder&& SetIncrement(i64 increment) && {
                    Msg->Record.SetIncrement(increment);
                    return std::move(*this);
                }

                TBuilder&& SetCycle(bool cycle) && {
                    Msg->Record.SetCycle(cycle);
                    return std::move(*this);
                }

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

            TEvUpdateSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
        };

        struct TEvFreezeSequence
            : public TEventPB<TEvFreezeSequence, NKikimrTxSequenceShard::TEvFreezeSequence, EvFreezeSequence>
        {
            TEvFreezeSequence() = default;

            explicit TEvFreezeSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }
        };

        struct TEvFreezeSequenceResult
            : public TEventPB<TEvFreezeSequenceResult, NKikimrTxSequenceShard::TEvFreezeSequenceResult, EvFreezeSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvFreezeSequenceResult::EStatus;

            TEvFreezeSequenceResult() = default;

            TEvFreezeSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
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

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }

            void InitFrom(const NKikimrTxSequenceShard::TEvFreezeSequenceResult& record) {
                Record.SetMinValue(record.GetMinValue());
                Record.SetMaxValue(record.GetMaxValue());
                Record.SetStartValue(record.GetStartValue());
                Record.SetNextValue(record.GetNextValue());
                Record.SetNextUsed(record.GetNextUsed());
                Record.SetCache(record.GetCache());
                Record.SetIncrement(record.GetIncrement());
                Record.SetCycle(record.GetCycle());
            }

            void InitFrom(const NKikimrTxSequenceShard::TEvGetSequenceResult& record) {
                Record.SetMinValue(record.GetMinValue());
                Record.SetMaxValue(record.GetMaxValue());
                Record.SetStartValue(record.GetStartValue());
                Record.SetNextValue(record.GetNextValue());
                Record.SetNextUsed(record.GetNextUsed());
                Record.SetCache(record.GetCache());
                Record.SetIncrement(record.GetIncrement());
                Record.SetCycle(record.GetCycle());
            }
        };

        struct TEvRestoreSequenceResult
            : public TEventPB<TEvRestoreSequenceResult, NKikimrTxSequenceShard::TEvRestoreSequenceResult, EvRestoreSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvRestoreSequenceResult::EStatus;

            TEvRestoreSequenceResult() = default;

            TEvRestoreSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
        };

        struct TEvRedirectSequence
            : public TEventPB<TEvRedirectSequence, NKikimrTxSequenceShard::TEvRedirectSequence, EvRedirectSequence>
        {
            TEvRedirectSequence() = default;

            TEvRedirectSequence(const TPathId& pathId, ui64 redirectTo) {
                SetPathId(pathId);
                Record.SetRedirectTo(redirectTo);
            }

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }
        };

        struct TEvRedirectSequenceResult
            : public TEventPB<TEvRedirectSequenceResult, NKikimrTxSequenceShard::TEvRedirectSequenceResult, EvRedirectSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvRedirectSequenceResult::EStatus;

            TEvRedirectSequenceResult() = default;

            TEvRedirectSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
        };

        struct TEvGetSequence
            : public TEventPB<TEvGetSequence, NKikimrTxSequenceShard::TEvGetSequence, EvGetSequence>
        {
            TEvGetSequence() = default;

            explicit TEvGetSequence(const TPathId& pathId) {
                SetPathId(pathId);
            }

            void SetPathId(const TPathId& pathId) {
                auto* p = Record.MutablePathId();
                p->SetOwnerId(pathId.OwnerId);
                p->SetLocalId(pathId.LocalPathId);
            }

            TPathId GetPathId() const {
                const auto& p = Record.GetPathId();
                return TPathId(p.GetOwnerId(), p.GetLocalId());
            }
        };

        struct TEvGetSequenceResult
            : public TEventPB<TEvGetSequenceResult, NKikimrTxSequenceShard::TEvGetSequenceResult, EvGetSequenceResult>
        {
            using EStatus = NKikimrTxSequenceShard::TEvGetSequenceResult::EStatus;

            TEvGetSequenceResult() = default;

            TEvGetSequenceResult(EStatus status, ui64 origin) {
                Record.SetStatus(status);
                Record.SetOrigin(origin);
            }
        };

    };

} // namespace NSequenceShard
} // namespace NKikimr
