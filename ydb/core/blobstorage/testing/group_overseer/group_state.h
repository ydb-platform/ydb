#pragma once

#include "defs.h"

namespace NKikimr::NTesting {

    using TQueryId = std::tuple<ui32, ui32, TActorId, ui64>; // nodeId, result type, sender, cookie
    using TBarrierId = std::tuple<ui64, ui8>;

} // NKikimr::NTesting

template<>
struct std::hash<NKikimr::NTesting::TQueryId> {
    size_t operator ()(const NKikimr::NTesting::TQueryId& x) const {
        return MultiHash(std::get<0>(x), std::get<1>(x), std::get<2>(x), std::get<3>(x));
    }
};

template<>
struct std::hash<NKikimr::NTesting::TBarrierId> {
    size_t operator ()(const NKikimr::NTesting::TBarrierId& x) const {
        return MultiHash(std::get<0>(x), std::get<1>(x));
    }
};

namespace NKikimr::NTesting {

    class TGroupState {

        enum class EConfidence {
            SURELY_NOT,
            POSSIBLE,
            CONFIRMED,
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Blocks

        struct TBlockedGeneration {
            ui32 Generation;
            ui64 IssuerGuid;

            TBlockedGeneration(const TEvBlobStorage::TEvBlock& msg)
                : Generation(msg.Generation)
                , IssuerGuid(msg.IssuerGuid)
            {}

            TBlockedGeneration(const TBlockedGeneration&) = default;

            friend bool operator <(const TBlockedGeneration& x, const TBlockedGeneration& y) {
                return x.Generation < y.Generation;
            }

            bool Same(const TBlockedGeneration& x) const {
                return Generation == x.Generation && IssuerGuid == x.IssuerGuid && IssuerGuid;
            }
        };

        struct TBlockInfo {
            std::optional<TBlockedGeneration> Confirmed;
            std::multiset<TBlockedGeneration> InFlight;
            std::unordered_map<TQueryId, std::multiset<TBlockedGeneration>::iterator> QueryToInFlight;

            EConfidence IsBlocked(ui32 generation) const;
        };

        std::unordered_map<ui64, TBlockInfo> Blocks;
        std::unordered_map<TQueryId, ui64> BlockQueryToTabletId;

        EConfidence IsBlocked(ui64 tabletId, ui32 generation) const;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TBarrierInfo {
            struct TValue {
                ui32 RecordGeneration;
                ui32 PerGenerationCounter;
                ui32 CollectGeneration;
                ui32 CollectStep;

                std::tuple<ui32, ui32> GetCollectGenStep() const { return {CollectGeneration, CollectStep}; }
                friend bool operator <(const TValue& x, const TValue& y) { return x.GetCollectGenStep() < y.GetCollectGenStep(); }

                void Supersede(const TValue& with) {
                    Y_ABORT_UNLESS(std::tie(RecordGeneration, PerGenerationCounter) <= std::tie(with.RecordGeneration, with.PerGenerationCounter));
                    if (RecordGeneration == with.RecordGeneration && PerGenerationCounter == with.PerGenerationCounter) {
                        Y_ABORT_UNLESS(GetCollectGenStep() == with.GetCollectGenStep());
                    } else {
                        Y_ABORT_UNLESS(GetCollectGenStep() <= with.GetCollectGenStep());
                    }
                    *this = with;
                }
            };
            std::optional<TValue> Confirmed[2]; // soft, hard

            struct TInFlightCollect {
                bool Hard;
                TValue Value;
                std::set<TLogoBlobID> Flags[2]; // doNotKeep, keep

                friend bool operator <(const TInFlightCollect& x, const TInFlightCollect& y) { return x.Value < y.Value; }
            };

            std::multiset<TInFlightCollect> InFlight[2]; // soft, hard
            std::unordered_map<TQueryId, std::multiset<TInFlightCollect>::iterator> CollectsInFlight;
        };

        std::unordered_map<TBarrierId, TBarrierInfo> Barriers;
        std::unordered_multimap<TQueryId, std::tuple<bool, TLogoBlobID>> FlagsInFlight;

        struct TBlobInfo;
        EConfidence IsCollected(TLogoBlobID id, EConfidence keep, EConfidence doNotKeep) const;
        EConfidence IsCollected(TLogoBlobID id, const TBlobInfo *blob) const;
        void ApplyBarrier(TBarrierId barrierId, std::optional<std::tuple<ui32, ui32>> prevGenStep,
            std::tuple<ui32, ui32> collectGenStep);

        struct TBlobValueHash {
            ui64 Low;
            ui64 High;

            TBlobValueHash(const TEvBlobStorage::TEvPut& msg) {
                uint64_t high;
                TRope buffer(msg.Buffer);
                const auto span = buffer.GetContiguousSpan();
                Low = t1ha2_atonce128(&high, span.data(), span.size(), 1);
                High = high;
            }

            TBlobValueHash(const TBlobValueHash&) = default;

            friend bool operator ==(const TBlobValueHash& x, const TBlobValueHash& y) { return x.Low == y.Low && x.High == y.High; }
            friend bool operator !=(const TBlobValueHash& x, const TBlobValueHash& y) { return !(x == y); }
        };

        struct TBlobInfo {
            std::optional<TBlobValueHash> ValueHash; // nullopt if not written
            bool ConfirmedValue = false;
            bool ConfirmedKeep = false;
            bool ConfirmedDoNotKeep = false;
            ui32 NumKeepsInFlight = 0; // number of CollectGarbage requests in flight with Keep flag for this blob
            ui32 NumDoNotKeepsInFlight = 0; // the same, but for DoNotKeep flag

            struct TQueryContext {
                bool IsBlocked = false; // was the request already blocked when the Put got issued?
                bool IsCollected = false; // was the blob id under garbage barrier when the Put got issued?
            };
            std::unordered_map<TQueryId, TQueryContext> PutsInFlight;
        };

        std::map<TLogoBlobID, TBlobInfo> Blobs;

        TBlobInfo *LookupBlob(TLogoBlobID id, bool create) const;

        void Log(TString message) const;

    public:
        TGroupState(ui32 groupId);

        template<typename T>
        void ExamineQueryEvent(const TQueryId& queryId, const T& msg);

        template<typename T>
        void ExamineResultEvent(const TQueryId& queryId, const T& msg);

        EBlobState GetBlobState(TLogoBlobID id, const TBlobInfo *blob = nullptr) const;
        void EnumerateBlobs(const std::function<void(TLogoBlobID, EBlobState)>& callback) const;

    private:
        TString LogPrefix;
    };

} // NKikimr::NTesting
