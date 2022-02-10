#pragma once

#include "flat_mem_warm.h"

namespace NKikimr {
namespace NTable {
namespace NMem {

    class TTreeIterator : public TTree::TIterator {
        using TBase = TTree::TIterator;

    public:
        explicit TTreeIterator(const TBase& base)
            : TBase(base)
        { }

        const TCell* GetKey() const {
            return TBase::GetKey().KeyCells;
        }

        const TUpdate* GetValue() const {
            return TBase::GetValue().GetFirst();
        }
    };

    class TTreeSnapshot {
    public:
        explicit TTreeSnapshot(TTree::TSnapshot snapshot)
            : Snapshot(std::move(snapshot))
        { }

        TTreeIterator Iterator() const {
            return TTreeIterator(Snapshot.Iterator());
        }

    private:
        TTree::TSnapshot Snapshot;
    };

} // namespace NMem

    struct TMemTableSnapshot {
        TIntrusiveConstPtr<TMemTable> MemTable;
        NMem::TTreeSnapshot Snapshot;

        TMemTableSnapshot(TIntrusiveConstPtr<TMemTable> memTable, NMem::TTreeSnapshot snapshot)
            : MemTable(std::move(memTable))
            , Snapshot(std::move(snapshot))
        { }

        const TMemTable& operator*() const {
            return *MemTable;
        }

        const TMemTable* operator->() const {
            return MemTable.Get();
        }
    };

}
}
