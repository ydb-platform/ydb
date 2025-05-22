#include "cache_block.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <util/stream/output.h>

template<>
void Out<NKikimr::TBlocksCache::EStatus>(IOutputStream &str, NKikimr::TBlocksCache::EStatus status) {
    switch (status) {
        case NKikimr::TBlocksCache::EStatus::OK:
            str << "OK";
            return;
        case NKikimr::TBlocksCache::EStatus::BLOCKED_PERS:
            str << "BLOCKED_PERS";
            return;
        case NKikimr::TBlocksCache::EStatus::BLOCKED_INFLIGH:
            str << "BLOCKED_INFLIGH";
            return;
    }
}

namespace NKikimr {

    void TBlocksCache::TBlockRes::Output(IOutputStream &str) const {
        str << "{Status# " << Status << " Lsn# " << Lsn << "}";
    }

    TString TBlocksCache::TBlockRes::ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    TBlocksCache::TBlockRes TBlocksCache::IsBlocked(ui64 tabletId, TBlockedGen gen, ui32 *actualGen) const {
        Y_ABORT_UNLESS(Initialized);
        if (const auto& st = IsBlockedByPersistent(tabletId, gen, actualGen); st.Status != EStatus::OK) {
            return st;
        } else if (const auto& st = IsBlockedByInFlight(tabletId, gen, actualGen); st.Status != EStatus::OK) {
            return st;
        } else {
            return {EStatus::OK, 0};
        }
    }

    bool TBlocksCache::IsBlockedLegacy(ui64 tabletId, TBlockedGen gen, ui32 *actualGen) const {
        Y_ABORT_UNLESS(Initialized);
        auto persRes = IsBlockedByPersistent(tabletId, gen, actualGen);
        return persRes.Status != EStatus::OK;
    }

    bool TBlocksCache::HasRecord(ui64 tabletId) const {
        Y_ABORT_UNLESS(Initialized);
        return PersistentBlocks.find(tabletId) != PersistentBlocks.end();
    }

    bool TBlocksCache::Find(ui64 tabletId, ui32 *outGen) const {
        Y_ABORT_UNLESS(Initialized);
        if (const auto it = PersistentBlocks.find(tabletId); it != PersistentBlocks.end()) {
            *outGen = it->second.Generation;
            return true;
        } else {
            return false;
        }
    }


    void TBlocksCache::Build(const THullDs *hullDs) {
        Y_ABORT_UNLESS(!Initialized);
        Initialized = true;

        if (!hullDs) {
            // for debug purpose
            return;
        }

        TIndexRecordMerger<TKeyBlock, TMemRecBlock> merger(hullDs->HullCtx->VCtx->Top->GType);
        // take a snapshot of all blocks; we don't care about LSN's here,
        // because there should be no data in fresh segment at this point of time
        TBlocksSnapshot snapshot(hullDs->Blocks->GetIndexSnapshot());
        TBlocksSnapshot::TForwardIterator it(hullDs->HullCtx, &snapshot);
        THeapIterator<TKeyBlock, TMemRecBlock, true> heapIt(&it);
        auto callback = [&] (TKeyBlock key, auto* merger) -> bool {
            ui32 blockedGen = merger->GetMemRec().BlockedGeneration;
            bool inserted = PersistentBlocks.emplace(key.TabletId, TBlockedGen(blockedGen, 0)).second;
            Y_ABORT_UNLESS(inserted);
            return true;
        };
        heapIt.Walk(TKeyBlock::First(), &merger, callback);
    }

    TBlocksCache::TBlockRes TBlocksCache::IsBlockedByInFlight(ui64 tabletId, TBlockedGen gen, ui32 *actualGen) const {
        if (const auto it = InFlightBlocks.find(tabletId); it != InFlightBlocks.end() && it->second.MaxBlockedGen.IsBlocked(gen)) {
            if (actualGen) {
                *actualGen = it->second.MaxBlockedGen.Generation;
            }
            return {EStatus::BLOCKED_INFLIGH, it->second.LsnForMaxBlockedGen};
        }
        return {EStatus::OK, 0};
    }

    TBlocksCache::TBlockRes TBlocksCache::IsBlockedByPersistent(ui64 tabletId, TBlockedGen gen, ui32 *actualGen) const {
        if (const auto it = PersistentBlocks.find(tabletId); it != PersistentBlocks.end() && it->second.IsBlocked(gen)) {
            if (actualGen) {
                *actualGen = it->second.Generation;
            }
            return {EStatus::BLOCKED_PERS, 0};
        }
        return {EStatus::OK, 0};
    }

    void TBlocksCache::UpdatePersistent(ui64 tabletId, TBlockedGen gen) {
        Y_ABORT_UNLESS(Initialized);
        auto& value = PersistentBlocks[tabletId];
        if (value.Generation < gen.Generation) {
            value = gen;
        }
    }

    void TBlocksCache::UpdateInFlight(ui64 tabletId, TBlockedGen gen, ui64 lsn) {
        Y_ABORT_UNLESS(Initialized);
        if (IsBlockedLegacy(tabletId, gen)) {
            // already blocked and saved
            return;
        }

        auto& state = InFlightBlocks[tabletId];
        if (state.MaxBlockedGen.Generation < gen.Generation) {
            state.MaxBlockedGen = gen;
            state.LsnForMaxBlockedGen = lsn;
        }

        // check that lsns increment in every queue
        Y_ABORT_UNLESS(state.InFlightQueue.empty() || state.InFlightQueue.back().Lsn < lsn);
        Y_ABORT_UNLESS(InFlightBlocksQueue.empty() || InFlightBlocksQueue.back().Lsn < lsn);

        state.InFlightQueue.push_back({lsn, gen});
        InFlightBlocksQueue.push_back({lsn, tabletId});
    }

    void TBlocksCache::CommitInFlight(ui64 tabletId, TBlockedGen gen, ui64 lsn) {
        Y_ABORT_UNLESS(Initialized);
        if (!InFlightBlocksQueue.empty()) {
            Y_ABORT_UNLESS(lsn <= InFlightBlocksQueue.front().Lsn);
            if (InFlightBlocksQueue.front().Lsn == lsn) {
                Y_ABORT_UNLESS(InFlightBlocksQueue.front().TabletId == tabletId);
                InFlightBlocksQueue.pop_front();

                const auto it = InFlightBlocks.find(tabletId);
                Y_ABORT_UNLESS(it != InFlightBlocks.end());
                auto& state = it->second;
                Y_ABORT_UNLESS(!state.InFlightQueue.empty() && state.InFlightQueue.front().Lsn == lsn &&
                    state.InFlightQueue.front().BlockedGen == gen);

                UpdatePersistent(tabletId, gen);
                state.InFlightQueue.pop_front();
                if (state.InFlightQueue.empty()) {
                    InFlightBlocks.erase(it);
                }
            }
        }
    }

} // NKikimr

