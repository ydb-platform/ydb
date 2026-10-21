#include "barriers_tree.h"

#include <util/generic/algorithm.h>

namespace NKikimr {
    namespace NBarriers {

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TTreeEntry -- entry of the index tree for key [TabletId, Channel]
        ////////////////////////////////////////////////////////////////////////////////////////////
        void TTreeEntry::Update(
                const TIngressCache *ingrCache,
                bool gcOnlySynced,
                const TKeyBarrier &key,
                const TMemRecBarrier &memRec)
        {
            if (key.Hard) {
                HardBarrier.Update(ingrCache, gcOnlySynced, key, memRec);
            } else {
                SoftBarrier.Update(ingrCache, gcOnlySynced, key, memRec);
            }
        }

        void TTreeEntry::Output(IOutputStream &str, const TIngressCache *ingrCache) const {
            str << "{soft# ";
            SoftBarrier.Output(str, ingrCache);
            str << " hard# ";
            HardBarrier.Output(str, ingrCache);
            str << "}";
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TIndexKey
        ////////////////////////////////////////////////////////////////////////////////////////////
        TIndexKey::TIndexKey(ui64 tabletId, ui8 channel)
            : TabletId(tabletId)
              , Channel(channel)
        {}

        size_t TIndexKey::Hash() const {
            return CombineHashes(IntHash<size_t>(TabletId), IntHash<size_t>(Channel));
        }

        bool TIndexKey::operator ==(const TIndexKey &v) const {
            return TabletId == v.TabletId && Channel == v.Channel;
        }

        void TIndexKey::Output(IOutputStream &str) const {
            str << "[TabletId# " << TabletId << " Channel# " << ui32(Channel) << "]";
        }

        TString TIndexKey::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TTree
        ////////////////////////////////////////////////////////////////////////////////////////////
        TTree::TTree(TIntrusivePtr<TIngressCache> ingressCache, const TString &vdiskLogPrefix)
            : IngressCache(std::move(ingressCache))
            , VDiskLogPrefix(vdiskLogPrefix)
        {}

        void TTree::Update(
                bool gcOnlySynced,
                const TKeyBarrier &key,
                const TMemRecBarrier &memRec)
        {
            LockWrite();

            if (DeadTablets.contains(key.TabletId)) {
                // complete tablet deletion (Max generation block), ignore further barriers
                UnlockWrite();
                return;
            }

            TIndexKey indexKey(key.TabletId, key.Channel);
            auto deadIt = Dead.find(indexKey);
            if (deadIt != Dead.end()) {
                // already dead table, ignore
                UnlockWrite();
                return;
            }

            auto it = Index.find(indexKey);
            if (it == Index.end()) {
                // inserts are rare
                auto res = Index.insert(TIndex::value_type(indexKey, {}));
                Y_ABORT_UNLESS(res.second);
                it = res.first;
            }

            // update entry
            it->second.Update(IngressCache.Get(), gcOnlySynced, key, memRec);

            auto hardBarrierOpt = it->second.GetHardBarrier();
            if (hardBarrierOpt && hardBarrierOpt->IsDead()) {
                Dead.insert(indexKey);
                Index.erase(it);
            }

            UnlockWrite();
        }

        void TTree::MarkTabletDeleted(ui64 tabletId) {
            LockWrite();

            if (!DeadTablets.insert(tabletId).second) {
                UnlockWrite();
                return;
            }

            for (ui32 channel = 0; channel < 256; ++channel) {
                TIndexKey indexKey(tabletId, static_cast<ui8>(channel));
                Index.erase(indexKey);
                Dead.erase(indexKey);
            }

            UnlockWrite();
        }

        void TTree::MarkTabletsDeleted(const THashSet<ui64> &tabletIds) {
            LockWrite();

            THashSet<ui64> added;
            for (ui64 tabletId : tabletIds) {
                if (DeadTablets.insert(tabletId).second) {
                    added.insert(tabletId);
                }
            }

            if (!added.empty()) {
                // One pass over the index, not 256 probes per tablet: this is called once per
                // VDisk start with every tablet ever completely deleted on this group, and that
                // set only grows.
                EraseNodesIf(Index, [&added](const auto &item) {
                    return added.contains(item.first.GetTabletId());
                });
                EraseNodesIf(Dead, [&added](const TIndexKey &key) {
                    return added.contains(key.GetTabletId());
                });
            }

            UnlockWrite();
        }

        bool TTree::IsTabletDeleted(ui64 tabletId) const {
            LockRead();
            const bool deleted = DeadTablets.contains(tabletId);
            UnlockRead();
            return deleted;
        }

        void TTree::GetBarrier(ui64 tabletId,
                ui8 channel,
                TMaybe<TCurrentBarrier> &soft,
                TMaybe<TCurrentBarrier> &hard) const
        {
            // A completely deleted tablet (Max generation block) has no barriers here at all: the
            // records were dropped by MarkTabletDeleted and Update ignores any that arrive later.
            // That is a fact about the tablet, not a barrier value, so it is not made up here --
            // IsTabletDeleted() reports it and TBarriersEssence acts on it.
            LockRead();

            TIndexKey indexKey(tabletId, channel);
            auto deadIt = Dead.find(indexKey);
            if (deadIt != Dead.end()) {
                // already dead table, ignore
                soft = TCurrentBarrier(Max<ui32>(), Max<ui32>(), Max<ui32>(), Max<ui32>());
                hard = TCurrentBarrier(Max<ui32>(), Max<ui32>(), Max<ui32>(), Max<ui32>());
                UnlockRead();
                return;
            }

            auto it = Index.find(indexKey);
            if (it == Index.end()) {
                soft = TMaybe<TCurrentBarrier>();
                hard = TMaybe<TCurrentBarrier>();
            } else {
                soft = it->second.GetSoftBarrier();
                hard = it->second.GetHardBarrier();
            }

            UnlockRead();
        }

        void TTree::Output(IOutputStream &str) const {
            str << "{Index# [";
            for (const auto &x : Index) {
                str << "{key# " << x.first.ToString() << " entry# ";
                x.second.Output(str, IngressCache.Get());
                str << "} ";
            }
            str << "] Dead# [";
            for (const auto &x : Dead) {
                str << "{key#" << x.ToString() << "} ";
            }
            str << "] DeadTablets# [";
            for (const auto &x : DeadTablets) {
                str << x << " ";
            }
            str << "]}";
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TMemView::TTreeWithLog
        ////////////////////////////////////////////////////////////////////////////////////////////
        TMemView::TTreeWithLog::TTreeWithLog(TIntrusivePtr<TIngressCache> ingressCache, const TString &vdiskLogPrefix)
            : Tree(std::make_shared<TTree>(ingressCache, vdiskLogPrefix))
        {}

        void TMemView::TTreeWithLog::RollUp(bool gcOnlySynced){
            Y_DEBUG_ABORT_UNLESS(!Shared());
            for (const auto &x : Log) {
                Tree->Update(gcOnlySynced, x.first, x.second);
            }
            Log.clear();
            for (ui64 tabletId : DeletedTabletsLog) {
                Tree->MarkTabletDeleted(tabletId);
            }
            DeletedTabletsLog.clear();
        }

        void TMemView::TTreeWithLog::Update(
                bool gcOnlySynced,
                const TKeyBarrier &key,
                const TMemRecBarrier &memRec)
        {
            if (Shared()) {
                Log.push_back(TLogRec(key, memRec));
            } else {
                RollUp(gcOnlySynced);
                Tree->Update(gcOnlySynced, key, memRec);
            }
        }

        void TMemView::TTreeWithLog::MarkTabletDeleted(bool gcOnlySynced, ui64 tabletId) {
            if (Shared()) {
                DeletedTabletsLog.push_back(tabletId);
            } else {
                RollUp(gcOnlySynced);
                Tree->MarkTabletDeleted(tabletId);
            }
        }

        void TMemView::TTreeWithLog::MarkTabletsDeleted(bool gcOnlySynced, const THashSet<ui64> &tabletIds) {
            if (Shared()) {
                DeletedTabletsLog.insert(DeletedTabletsLog.end(), tabletIds.begin(), tabletIds.end());
            } else {
                RollUp(gcOnlySynced);
                Tree->MarkTabletsDeleted(tabletIds);
            }
        }

        bool TMemView::TTreeWithLog::Shared() const {
            return Tree.use_count() > 1;
        }

        bool TMemView::TTreeWithLog::NeedRollUp() const {
            return !Log.empty() || !DeletedTabletsLog.empty();
        }

        TMemViewSnap TMemView::TTreeWithLog::GetSnapshot() const {
            return TMemViewSnap(Tree);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TMemView
        ////////////////////////////////////////////////////////////////////////////////////////////
        TMemView::TMemView(TIntrusivePtr<TIngressCache> ingrCache, const TString &vdiskLogPrefix, bool gcOnlySynced)
            : GCOnlySynced(gcOnlySynced)
            , Active(std::make_unique<TTreeWithLog>(ingrCache, vdiskLogPrefix))
            , Passive(std::make_unique<TTreeWithLog>(ingrCache, vdiskLogPrefix))
        {}

        void TMemView::Update(const TKeyBarrier &key, const TMemRecBarrier &memRec) {
            Active->Update(GCOnlySynced, key, memRec);
            Passive->Update(GCOnlySynced, key, memRec);
            if (Active->Shared() && !Passive->Shared()) {
                Active.swap(Passive);
            }
        }

        void TMemView::MarkTabletDeleted(ui64 tabletId) {
            Active->MarkTabletDeleted(GCOnlySynced, tabletId);
            Passive->MarkTabletDeleted(GCOnlySynced, tabletId);
            if (Active->Shared() && !Passive->Shared()) {
                Active.swap(Passive);
            }
        }

        void TMemView::MarkTabletsDeleted(const THashSet<ui64> &tabletIds) {
            Active->MarkTabletsDeleted(GCOnlySynced, tabletIds);
            Passive->MarkTabletsDeleted(GCOnlySynced, tabletIds);
            if (Active->Shared() && !Passive->Shared()) {
                Active.swap(Passive);
            }
        }

        TMemViewSnap TMemView::GetSnapshot() {
            if (Active->NeedRollUp()) {
                if (Active->Shared() && !Passive->Shared()) {
                    Active.swap(Passive);
                }
                if (!Active->Shared()) {
                    Active->RollUp(GCOnlySynced);
                }
            }
            return Active->GetSnapshot();
        }

    } // NBarriers
} // NKikimr
