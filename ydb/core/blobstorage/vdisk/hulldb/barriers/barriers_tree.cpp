#include "barriers_tree.h"

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

        void TTree::GetBarrier(ui64 tabletId,
                ui8 channel,
                TMaybe<TCurrentBarrier> &soft,
                TMaybe<TCurrentBarrier> &hard) const
        {
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

        bool TMemView::TTreeWithLog::Shared() const {
            return Tree.use_count() > 1;
        }

        bool TMemView::TTreeWithLog::NeedRollUp() const {
            return !Log.empty();
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
