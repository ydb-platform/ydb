#include "flat_test_db.h"
#include "flat_database_ut_common.h"

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/scheme_types/scheme_types.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

struct TFakeKey {
    TVector<TFakeTableCell> Columns;
};

struct TFakeKeyComparator {
    bool operator () (const TFakeKey& a, const TFakeKey& b) const {
        Y_ASSERT(a.Columns.size() == b.Columns.size());
        for (ui32 i = 0; i < a.Columns.size(); ++i) {
            const TRawTypeValue& av = a.Columns[i].Get();
            const TRawTypeValue& bv = b.Columns[i].Get();
            if (av.IsEmpty() && !bv.IsEmpty())
                return true;
            if (bv.IsEmpty())
                return false;
            Y_ASSERT(av.Type() == bv.Type());
            // pg types are not supported
            int res = CompareTypedCells(TCell(&av), TCell(&bv), NScheme::TTypeInfo(av.Type()));
            if (res)
                return res < 0;
        }
        return false;
    }
};

struct TFakeVal {
    bool IsDeleted;
    TMap<TTag, TFakeTableCell> Columns;

    void ApplyUpdate(const TFakeVal& other)
    {
        if (IsDeleted = other.IsDeleted) {
            Columns.clear();
        } else {
            for (auto &val: other.Columns)
                Columns[val.first] = val.second;
        }
    }

    TCell Read(
            TArrayRef<const TFakeTableCell> key, const TColumn &col)
                const noexcept
    {
        if (col.KeyOrder != Max<ui32>()) {
            return *key[col.KeyOrder]; /* has no default values */
        } else {
            auto *val = Columns.FindPtr(col.Id);

            return val ? **val : col.Null;
        }
    }

    void Swap(TFakeVal& other) {
        std::swap(IsDeleted, other.IsDeleted);
        Columns.swap(other.Columns);
    }
};

typedef TMap<TFakeKey, TFakeVal, TFakeKeyComparator> TFakeTable;


class TFakeDbIterator : public ITestIterator {
    TVector<ui32> ValueTags;
    TVector<const TColumn*> Cols;
    TFakeTable::const_iterator RowIt;
    TFakeTable::const_iterator RowEnd;

    TVector<NScheme::TTypeInfo> KeyTypes;
    TVector<TCell> KeyCells;

    TVector<NScheme::TTypeInfo> ValueTypes;
    TVector<TCell> ValueCells;
    bool First = true;

public:
    explicit TFakeDbIterator(const TScheme& scheme, ui32 root, TTagsRef tags, TFakeTable::const_iterator rowIt, TFakeTable::const_iterator rowEnd)
        : ValueTags(tags.begin(), tags.end())
        , RowIt(rowIt)
        , RowEnd(rowEnd)
    {
        Cols.reserve(tags.size());
        ValueTypes.reserve(tags.size());
        ValueCells.reserve(tags.size());

        for (auto tag : ValueTags) {
            auto *column = scheme.GetColumnInfo(root, tag);

            Cols.push_back(column);
            ValueTypes.push_back(column->PType);
        }

        for (auto tag : scheme.GetTableInfo(root)->KeyColumns) {
            KeyTypes.push_back(scheme.GetColumnInfo(root, tag)->PType);
        }

        KeyCells.reserve(KeyTypes.size());
    }

    const TFakeKey& GetFakeKey() const {
        return RowIt->first;
    }

    EReady Next(ENext mode) override
    {
        /* Should position to the first row on first Next() to conform db
            iterator API, but iterator is already positioned to first row
            just after ctor invocation.
         */

        while (IsValid()) {
            if (!std::exchange(First, false))
                ++RowIt;

            if (!(mode == ENext::Data && IsValid() && RowIt->second.IsDeleted))
                break;
        }

        FillCells();

        return IsValid () ? EReady::Data : EReady::Gone;
    }

    bool IsValid() override {
        return RowIt != RowEnd;
    }

    bool IsRowDeleted() override {
        Y_ASSERT(IsValid());
        return RowIt->second.IsDeleted;
    }

    TDbTupleRef GetKey() override {
        if (!IsValid())
            return TDbTupleRef();

        return TDbTupleRef(&KeyTypes[0], &KeyCells[0], KeyCells.size());
    }

    TDbTupleRef GetValues() override {
        if (!IsValid())
            return TDbTupleRef();

        if (RowIt->second.IsDeleted)
            return TDbTupleRef();

        return TDbTupleRef(&ValueTypes[0], &ValueCells[0], ValueCells.size());
    }

    TCell GetValue(ui32 idx) override {
        if (!IsValid())
            return TCell();

        return ValueCells[idx];
    }

private:
    void FillCells() {
        KeyCells.clear();
        ValueCells.clear();
        if (!IsValid())
            return;

        for (const TFakeTableCell& kc : RowIt->first.Columns)
            KeyCells.push_back(*kc);

        for (auto *col: Cols)
            ValueCells.emplace_back(RowIt->second.Read(RowIt->first.Columns, *col));
    }
};

// Simple test implementation of a DB using TMap<>
class TFakeDb : public ITestDb {
private:
    TAutoPtr<TScheme> Scheme;

public:
    void Init(const TScheme& scheme) override {
        Scheme.Reset(new TScheme(scheme));
    }

    const TScheme& GetScheme() const override {
        return *Scheme;
    }

    TString FinishTransaction(bool commit) override {
        if (commit) {
            TAutoPtr<TSchemeChanges> schemeDeltaRecs = SchemeChanges.Flush();
            TSchemeModifier(*Scheme).Apply(*schemeDeltaRecs);
            // Apply scheme operations that affect existing data
            for (ui32 i = 0;  i < schemeDeltaRecs->DeltaSize(); ++i) {
                const TAlterRecord& rec = schemeDeltaRecs->GetDelta(i);
                switch (rec.GetDeltaType()) {
                case TAlterRecord::AddTable:
                    Tables[rec.GetTableId()] = TFakeTable();
                    break;
                case TAlterRecord::DropTable:
                    Tables.erase(rec.GetTableId());
                    break;
                case TAlterRecord::DropColumn:
                    EraseColumnValues(rec.GetTableId(), rec.GetColumnId());
                    break;
                default:
                    break;
                }
            }

            // Apply data modifications
            for (auto& table : TxChanges) {
                ui32 rootId = table.first;
                for (auto& row : table.second) {
                    TFakeVal& fv = Tables[rootId][row.first];
                    // Replace the whole row with the updated
                    fv.Swap(row.second);
                }
            }
        }
        TxChanges.clear();
        SchemeChanges.Flush();
        return TString();
    }

    void Update(ui32 root, ERowOp rop, TRawVals key, TArrayRef<const TUpdateOp> ops) override
    {
        if (rop == ERowOp::Upsert) {
            UpdateRow(root, key, ops);
        } else if (rop == ERowOp::Erase){
            EraseRow(root, key);
        }
    }

    void UpdateRow(ui32 root, TRawVals key, TArrayRef<const TUpdateOp> ops)
    {
        TFakeKey fk;
        FillKey(root, fk, key, true);

        // Copy previous value from the commited data
        if (!TxChanges[root].contains(fk) && Tables[root].contains(fk)) {
            TxChanges[root][fk] = Tables[root][fk];
        }

        // Apply changes
        TFakeVal& fv = TxChanges[root][fk];
        fv.IsDeleted = false;
        for (auto &one: ops) {
            if (one.Op == ECellOp::Null || !one.Value)
                fv.Columns[one.Tag].Set({ });
            else
                fv.Columns[one.Tag].Set(one.Value);
        }
    }

    void EraseRow(ui32 root, TRawVals key) {
        TFakeKey fk;
        FillKey(root, fk, key, true);
        TFakeVal fv;
        fv.IsDeleted = true;
        TxChanges[root][fk] = fv;
    }

    void Precharge(ui32 root,
                           TRawVals keyFrom, TRawVals keyTo,
                           TTagsRef tags, ui32 flags) override {
        Y_UNUSED(root);
        Y_UNUSED(keyFrom);
        Y_UNUSED(keyTo);
        Y_UNUSED(tags);
        Y_UNUSED(flags);
    }

    ITestIterator* Iterate(ui32 root, TRawVals key, TTagsRef tags, ELookup mode) noexcept override {
        if (!key) {
            return new TFakeDbIterator(GetScheme(), root, tags, Tables[root].begin(), Tables[root].end());
        }

        TFakeKey fk;
        FillKey(root, fk, key, false);
        if (mode == ELookup::ExactMatch) {
            auto begin = Tables[root].find(fk);
            decltype(begin) end = begin;

            return new TFakeDbIterator(GetScheme(), root, tags, begin, end == Tables[root].end() ? end : ++end);
        } else {
            auto begin = Tables[root].lower_bound(fk);

            if (mode == ELookup::GreaterThan && begin != Tables[root].end()) {
                TFakeKeyComparator keyLess;
                // proceed to the next row if keys are equal
                if (!keyLess(fk, begin->first) && !keyLess(begin->first, fk))
                    ++begin;
            }

            return new TFakeDbIterator(GetScheme(), root, tags, begin, Tables[root].end());
        }
    }

    void Apply(const TSchemeChanges &delta) override
    {
        SchemeChanges.Merge(delta);
    }

private:
    void FillKey(ui32, TFakeKey& fk, TRawVals key, bool) const
    {
        fk.Columns.resize(key.size());
        for (ui32 on = 0; on < fk.Columns.size(); on++) {
            fk.Columns[on].Set(key[on]);
        }
    }

    void EraseColumnValues(ui32 tableId, ui32 colId) {
        for (auto& row : Tables[tableId]) {
            row.second.Columns.erase(colId);
        }
    }

private:
    THashMap<ui32, TFakeTable> Tables;
    THashMap<ui32, TFakeTable> TxChanges;
    TAlter SchemeChanges;
};


TAutoPtr<ITestDb> CreateFakeDb() {
    return new TFakeDb();
}

} // namspace NTable
} // namespace NKikimr
