#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_defs.h>

#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <utility>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TFullCompactionState
    ////////////////////////////////////////////////////////////////////////////
    struct TFullCompactionState {
        class TRateLimitter {
        public:
            explicit TRateLimitter(TIntrusivePtr<TVDiskConfig> config)
                : Config(std::move(config))
            {}

            bool IsEnable() const {
                if (!(ui32) Config->HullCompFullCompPeriodSec) {
                    return true;
                }
                return (TActivationContext::Now() - LastUpdateTime).Seconds() > (ui32) Config->HullCompFullCompPeriodSec;
            }

            void Update() {
                LastUpdateTime = TActivationContext::Now();
            }

        private:
            TInstant LastUpdateTime = TInstant::Zero();
            TIntrusivePtr<TVDiskConfig> Config;
        };

        struct TCompactionRequest {
            EHullDbType Type = EHullDbType::Max;
            ui64 RequestId = 0;
            TActorId Recipient;
        };

        explicit TFullCompactionState(TIntrusivePtr<TVDiskConfig> config)
            : RateLimitter(std::move(config))
        {}

        bool Enabled() const {
            return bool(FullCompactionAttrs) && (Force || RateLimitter.IsEnable());
        }

        void FullCompactionTask(ui64 fullCompactionLsn, TInstant now, EHullDbType type, ui64 requestId,
                const TActorId &recipient, THashSet<ui64> tablesToCompact, bool force)
        {
            if (!FullCompactionAttrs) {
                FullCompactionAttrs.emplace(fullCompactionLsn, now, std::move(tablesToCompact));
                Requests.push_back({type, requestId, recipient});
                if (force) {
                    Force = true;
                }
                return;
            }

            // Defer new requests; keep the current FullCompactionLsn unchanged.
            PendingRequests.push_back({type, requestId, recipient});
            if (!PendingFullCompactionLsn || fullCompactionLsn > *PendingFullCompactionLsn) {
                PendingFullCompactionLsn = fullCompactionLsn;
            }
            PendingTablesToCompact.insert(tablesToCompact.begin(), tablesToCompact.end());
            if (force) {
                PendingForce = true;
            }
        }

        TVector<TCompactionRequest> Compacted(const std::pair<std::optional<NHullComp::TFullCompactionAttrs>, bool>& info,
                TInstant now)
        {
            TVector<TCompactionRequest> completed;
            if (Enabled() && FullCompactionAttrs == info.first && info.second) {
                // full compaction finished
                completed.assign(Requests.begin(), Requests.end());
                Requests.clear();
                FullCompactionAttrs.reset();
                RateLimitter.Update();
                Force = false;

                if (PendingFullCompactionLsn) {
                    FullCompactionAttrs.emplace(*PendingFullCompactionLsn, now, std::move(PendingTablesToCompact));
                    Requests = std::move(PendingRequests);
                    PendingRequests.clear();
                    PendingFullCompactionLsn.reset();
                    PendingTablesToCompact.clear();
                    Force = PendingForce;
                    PendingForce = false;
                }
            }
            return completed;
        }

        // returns FullCompactionAttrs for Level Compaction Selector
        // if Fresh segment before FullCompactionAttrs->FullCompationLsn has not been written to sst yet,
        // there is no profit in starting LevelCompaction, so we return nullopt
        template <class TRTCtx>
        std::optional<NHullComp::TFullCompactionAttrs> GetFullCompactionAttrsForLevelCompactionSelector(const TRTCtx &rtCtx) {
            return Enabled() && rtCtx->LevelIndex->IsWrittenToSstBeforeLsn(FullCompactionAttrs->FullCompactionLsn)
                ? FullCompactionAttrs
                : std::nullopt;
        }

    private:
        TRateLimitter RateLimitter;
        bool Force = false;

        std::deque<TCompactionRequest> Requests;
        std::optional<NHullComp::TFullCompactionAttrs> FullCompactionAttrs;

        std::optional<ui64> PendingFullCompactionLsn;
        THashSet<ui64> PendingTablesToCompact;
        std::deque<TCompactionRequest> PendingRequests;
        bool PendingForce = false;
    };

} // NKikimr
