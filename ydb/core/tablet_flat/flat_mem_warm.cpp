#include "flat_mem_warm.h"
#include "flat_mem_snapshot.h"
#include "flat_page_other.h"

namespace NKikimr {
namespace NTable {

TString PrintRow(const TDbTupleRef& row, const NScheme::TTypeRegistry& typeRegistry) {
    return DbgPrintTuple(row, typeRegistry);
}

TIntrusiveConstPtr<NPage::TExtBlobs> TMemTable::MakeBlobsPage(TArrayRef<const TMemTableSnapshot> list)
{
    NPage::TExtBlobsWriter writer;

    for (auto &one: list) {
        for (auto it = one->GetBlobs()->Iterator(); it.IsValid(); it.Next()) {
            writer.Put(it->GId);
        }
    }

    return new NPage::TExtBlobs(writer.Make(true), { });
}

NMem::TTreeSnapshot TMemTable::Snapshot() {
    return NMem::TTreeSnapshot(Tree.Snapshot());
}

NMem::TTreeSnapshot TMemTable::Immediate() const {
    // Immediate snapshots are used in two distinct cases:
    // 1) When taking a snapshot of a frozen mem table, in that case we know
    //    the tree is frozen and will not be modified, so using an unsafe
    //    snapshot is ok.
    // 2) When taking a snapshot of a mutable mem table, but used in table
    //    iterators. In all those cases we know mem table is not modified
    //    until transaction is committed, so using unsafe snapshot is ok.
    return NMem::TTreeSnapshot(Tree.UnsafeSnapshot());
}

void TMemTable::DebugDump(IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const {
    auto it = Immediate().Iterator();
    auto types = Scheme->Keys->BasicTypes();
    for (it.SeekFirst(); it.IsValid(); it.Next()) {
        TDbTupleRef key(types.data(), it.GetKey(), types.size());

        TString keyStr = PrintRow(key, typeRegistry) + " -> ";
        const auto *row = it.GetValue();
        while (row) {
            str << keyStr
                << "ERowOp " << int(row->Rop)
                << " {";
            for (ui32 i = 0; i < row->Items; ++i) {
                TTag colId = row->Ops()[i].Tag;
                if (Scheme->ColInfo(colId)) {
                    NScheme::TTypeId typeId = Scheme->ColInfo(colId)->TypeId;
                    auto &op = row->Ops()[i];

                    str << EOpToStr(ECellOp(op.Op)) << " " << op.Tag << " " << DbgPrintCell(op.Value, typeId, typeRegistry);
                } else {
                    str << "unknown column " << colId;
                }
                if (i+1 < row->Items)
                    str << ", ";
            }
            str << "}" << Endl;
            row = row->Next;
        }
    }
}

}}
