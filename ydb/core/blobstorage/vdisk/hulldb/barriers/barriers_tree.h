#pragma once

#include "defs.h"
#include "barriers_chain.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
    namespace NBarriers {

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TTreeEntry -- entry of the index tree for key [TabletId, Channel]
        ////////////////////////////////////////////////////////////////////////////////////////////
        class TTreeEntry {
        private:
            TBarrierChain SoftBarrier;
            TBarrierChain HardBarrier;

        public:
            void Update(
                    const TIngressCache *ingrCache,
                    bool gcOnlySynced,
                    const TKeyBarrier &key,
                    const TMemRecBarrier &memRec);
            TMaybe<TCurrentBarrier> GetSoftBarrier() const {
                return SoftBarrier.GetBarrier();
            }
            TMaybe<TCurrentBarrier> GetHardBarrier() const {
                return HardBarrier.GetBarrier();
            }
            void Output(IOutputStream &str, const TIngressCache *ingrCache) const;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TIndexKey
        // Index key for Barriers Database Tree
        ////////////////////////////////////////////////////////////////////////////////////////////
        class TIndexKey {
        private:
            ui64 TabletId = 0;
            ui8 Channel = 0;

        public:
            TIndexKey() = default;
            explicit TIndexKey(ui64 tabletId, ui8 channel);
            size_t Hash() const;
            bool operator ==(const TIndexKey &v) const;
            void Output(IOutputStream &str) const;
            TString ToString() const;
        };

    } // NBarriers
} // NKikimr

template <>
struct THash<NKikimr::NBarriers::TIndexKey> {
inline size_t operator()(const NKikimr::NBarriers::TIndexKey& k) const {
        return k.Hash();
    }
};

namespace NKikimr {
    namespace NBarriers {

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TTree
        // Barriers Database Tree
        // Index tree is built of these structures: [TabletId, Channel] -> [Entry]
        ////////////////////////////////////////////////////////////////////////////////////////////
        class TTree {
            // Main index for TabletId, Channel
            using TIndex = THashMap<TIndexKey, TTreeEntry>;
            // Deleted keys (data structure below is better in terms of memory consumption)
            using TDead = THashSet<TIndexKey>;

            TIntrusivePtr<TIngressCache> IngressCache;
            TString VDiskLogPrefix;
            TIndex Index;
            TDead Dead;

        public:
            TTree(TIntrusivePtr<TIngressCache> ingressCache, const TString &vdiskLogPrefix);
            void Update(
                    bool gcOnlySynced,
                    const TKeyBarrier &key,
                    const TMemRecBarrier &memRec);
            void GetBarrier(
                    ui64 tabletId,
                    ui8 channel,
                    TMaybe<TCurrentBarrier> &soft,
                    TMaybe<TCurrentBarrier> &hard) const;
            void Output(IOutputStream &str) const;

        private:
            mutable std::atomic_intptr_t Lock{0};

            void LockWrite() {
                if constexpr (NSan::TSanIsOn()) {
                    const intptr_t x = Lock.exchange(-1);
                    Y_ABORT_UNLESS(x == 0);
                }
            }

            void UnlockWrite() {
                if constexpr (NSan::TSanIsOn()) {
                    const intptr_t x = Lock.exchange(0);
                    Y_ABORT_UNLESS(x == -1);
                }
            }

            void LockRead() const {
                if constexpr (NSan::TSanIsOn()) {
                    const intptr_t x = Lock++;
                    Y_ABORT_UNLESS(x >= 0);
                }
            }

            void UnlockRead() const {
                if constexpr (NSan::TSanIsOn()) {
                    const intptr_t x = --Lock;
                    Y_ABORT_UNLESS(x >= 0);
                }
            }
        };

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TMemViewSnap
        // Barriers Database Memory View Snapshot
        ////////////////////////////////////////////////////////////////////////////////////////////
        class TMemViewSnap {
            std::shared_ptr<TTree> Tree;

        public:
            TMemViewSnap(std::shared_ptr<TTree> tree)
                : Tree(std::move(tree))
            {}
            TMemViewSnap(const TMemViewSnap &) = default;
            TMemViewSnap(TMemViewSnap &&) = default;
            TMemViewSnap &operator=(const TMemViewSnap &) = default;
            TMemViewSnap &operator=(TMemViewSnap &&) = default;
            void GetBarrier(ui64 tabletId, ui8 channel, TMaybe<TCurrentBarrier> &soft,
                    TMaybe<TCurrentBarrier> &hard) const
            {
                return Tree->GetBarrier(tabletId, channel, soft, hard);
            }
            void Output(IOutputStream &str) const { return Tree->Output(str); }
        };

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TMemView
        // Barriers Database Memory View
        ////////////////////////////////////////////////////////////////////////////////////////////
        class TMemView {
            struct TTreeWithLog {
                // log of enqueued updates
                using TLogRec = std::pair<TKeyBarrier, TMemRecBarrier>;
                using TLog = TDeque<TLogRec>;

                std::shared_ptr<TTree> Tree;
                TLog Log;

                TTreeWithLog(TIntrusivePtr<TIngressCache> ingressCache, const TString &vdiskLogPrefix);
                void RollUp(bool gcOnlySynced);
                void Update(
                        bool gcOnlySynced,
                        const TKeyBarrier &key,
                        const TMemRecBarrier &memRec);
                bool Shared() const;
                bool NeedRollUp() const;
                TMemViewSnap GetSnapshot() const;
            };

            const bool GCOnlySynced;
            std::unique_ptr<TTreeWithLog> Active;
            std::unique_ptr<TTreeWithLog> Passive;

        public:
            TMemView(TIntrusivePtr<TIngressCache> ingrCache, const TString &vdiskLogPrefix, bool gcOnlySynced);
            void Update(const TKeyBarrier &key, const TMemRecBarrier &memRec);
            TMemViewSnap GetSnapshot();
        };

    } // NBarriers
} // NKikimr

