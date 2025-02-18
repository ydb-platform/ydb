#pragma once

#include "flat_test_db.h"

#include <ydb/core/tablet_flat/flat_dbase_apply.h>
#include <ydb/core/tablet_flat/flat_row_scheme.h>
#include <ydb/core/tablet_flat/flat_mem_warm.h>
#include <ydb/core/scheme/scheme_type_id.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

struct TFakeTableCell {
private:
    ECellOp Op = ECellOp::Set;
    TRawTypeValue Val;
    TString Buf;
public:
    TFakeTableCell() {}

    TFakeTableCell(const TFakeTableCell& other) {
        Set(other.Get());
    }

    const TFakeTableCell& operator = (const TFakeTableCell& other) {
        if (this != &other)
            Set(other.Get());
        return *this;
    }

    void Set(const TRawTypeValue& v) {
        if (!v.IsEmpty()) {
            Buf = TString((const char*)v.Data(), v.Size());
            Val = TRawTypeValue(Buf.data(), Buf.size(), v.Type());
        } else {
            Val = TRawTypeValue();
        }
    }

    void SetOp(ECellOp op) {
        Op = op;
    }

    TCell operator*() const
    {
        return TCell((const char*)Val.Data(), Val.Size());
    }

    const TRawTypeValue& Get() const {
        return Val;
    }

    ECellOp GetCellOp() const {
        return Op;
    }
};

// TODO: properly support all types
inline TFakeTableCell FromVal(NScheme::TTypeId t, i64 val) {
    TFakeTableCell c;
    ui32 sz = sizeof(val);
    switch (t) {
    case NScheme::NTypeIds::Byte:
    case NScheme::NTypeIds::Bool:
        sz = 1;
        break;
    case NScheme::NTypeIds::Int32:
    case NScheme::NTypeIds::Uint32:
        sz = 4;
        break;
    }

    c.Set(TRawTypeValue(&val, sz, t));
    return c;
}

inline TFakeTableCell MakeNull(ECellOp op) {
    TFakeTableCell c;
    c.SetOp(op);
    return c;
}

inline TFakeTableCell FromVal(NScheme::TTypeId, std::nullptr_t) {
    return MakeNull(ECellOp::Set);
}

inline TFakeTableCell FromVal(NScheme::TTypeId t, TString val) {
    TFakeTableCell c;
    c.Set(TRawTypeValue(val.data(), val.size(), t));
    return c;
}

inline TFakeTableCell FromVal(NScheme::TTypeId t, const char* v) {
    return FromVal(t, TString(v));
}

// Store table id and row key for an update operation
class TDbRowOpBase {
protected:
    const TScheme& Scheme;
private:
    ui32 Root;
    TVector<TFakeTableCell> KeyCells;
public:
    TDbRowOpBase(const TScheme& scheme, ui32 root)
        : Scheme(scheme)
        , Root(root)
    {}

    template <typename... Tt>
    TDbRowOpBase& Key(Tt... tt) {
        AppendKeyColumn(Root, Scheme, KeyCells, tt...);
        return *this;
    }

    ui32 GetRoot() const {
        return Root;
    }

    const TVector<TFakeTableCell>& GetKey() const {
        return KeyCells;
    }
};

// Accumulates row key and and a set of tag updates operations
class TDbRowUpdate : public TDbRowOpBase {
    TMap<ui32, TFakeTableCell> TagOps;
public:
    TDbRowUpdate(const TScheme& scheme, ui32 root)
        : TDbRowOpBase(scheme, root)
    {}

    template <typename... Tt>
    TDbRowUpdate& Key(Tt... tt) {
        TDbRowOpBase::Key(tt...);
        return *this;
    }

    template<typename T>
    TDbRowUpdate& Set(TString tagName, const T& val) {
        const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(GetRoot());
        Y_ABORT_UNLESS(tableInfo, "Unknown table id %u", GetRoot());
        const ui32* tagId = tableInfo->ColumnNames.FindPtr(tagName);
        Y_ABORT_UNLESS(tagId, "Unknown column \"%s\" in table %u", tagName.data(), GetRoot());
        const auto *colInfo = Scheme.GetColumnInfo(GetRoot(), *tagId);
        Y_ABORT_UNLESS(colInfo, "Column info not found for table id %u, column id %u", GetRoot(), *tagId);
        NScheme::TTypeId type = colInfo->PType.GetTypeId();
        TagOps[*tagId] = FromVal(type, val);
        return *this;
    }

    TDbRowUpdate& Erase(TString tagName) {
        const TScheme::TTableInfo* tableInfo = Scheme.GetTableInfo(GetRoot());
        Y_ABORT_UNLESS(tableInfo, "Unknown table id %u", GetRoot());
        const ui32* tagId = tableInfo->ColumnNames.FindPtr(tagName);
        Y_ABORT_UNLESS(tagId, "Unknown column \"%s\" in table %u", tagName.data(), GetRoot());
        const auto * colInfo = Scheme.GetColumnInfo(GetRoot(), *tagId);
        Y_ABORT_UNLESS(colInfo, "Column info not found for table id %u, column id %u", GetRoot(), *tagId);
        TagOps[*tagId] = MakeNull(ECellOp::Null);
        return *this;
    }

    const TMap<ui32, TFakeTableCell>& GetTagOps() const {
        return TagOps;
    }
};

typedef TDbRowOpBase TDbRowErase;


template <typename... Tt>
void AppendKeyColumn(ui32 root, const TScheme& scheme, TVector<TFakeTableCell>& tuple) {
    // Extend the key according to scheme
    tuple.resize(scheme.GetTableInfo(root)->KeyColumns.size());
}

template <typename T, typename... Tt>
void AppendKeyColumn(ui32 root, const TScheme& scheme, TVector<TFakeTableCell>& tuple, T t, Tt... tt) {
    ui32 pos = tuple.size();
    ui32 tag = scheme.GetTableInfo(root)->KeyColumns[pos];
    NScheme::TTypeId type = scheme.GetColumnInfo(root, tag)->PType.GetTypeId();
    tuple.push_back(FromVal(type, t));
    AppendKeyColumn(root, scheme, tuple, tt...);
}

template <typename... Tt>
void AppendKeyColumn(ui32 root, const TScheme& scheme, TVector<TFakeTableCell>& tuple, nullptr_t, Tt... tt) {
    tuple.push_back(MakeNull(ECellOp::Set));
    AppendKeyColumn(root, scheme, tuple, tt...);
}

// Helps to simplify test code that deals with ITestDb
class TDbWrapper {
    ITestDb& Db;
    TScheme Scheme;

public:
    using ECodec = NPage::ECodec;
    using ECache = NPage::ECache;

    explicit TDbWrapper(ITestDb& db)
        : Db(db)
        , Scheme(Db.GetScheme())
    {}

    ITestDb* operator->() const { return &Db; }

    TDbRowUpdate Update(ui32 root) {
        return TDbRowUpdate(Scheme, root);
    }

    TDbRowErase Erase(ui32 root) {
        return TDbRowErase(Scheme, root);
    }

    void Apply(const TDbRowUpdate& update) {
        TVector<TRawTypeValue> key;
        Y_ABORT_UNLESS(!update.GetKey().empty());
        for (const auto& col : update.GetKey()) {
            key.push_back(col.Get());
        }

        TVector<TUpdateOp> ops;
        for (const auto& op : update.GetTagOps()) {
            ops.push_back(TUpdateOp(op.first, op.second.GetCellOp(), op.second.Get()));
        }

        Db.Update(update.GetRoot(), ERowOp::Upsert, key, ops);
    }

    void Apply(const TDbRowErase& erase) {
        TVector<TRawTypeValue> key;
        Y_ABORT_UNLESS(!erase.GetKey().empty());
        for (const auto& col : erase.GetKey()) {
            key.push_back(col.Get());
        }

        Db.Update(erase.GetRoot(), ERowOp::Erase, key, { });
    }

    void Apply(const TSchemeChanges &delta)
    {
        Db.Apply(delta);
        TSchemeModifier(Scheme).Apply(delta);
    }

    void FinishTransaction(bool commit) {
        Db.FinishTransaction(commit);
        Scheme = Db.GetScheme();
    }

};

} // namspace NTabletFlatExecutor
} // namespace NKikimr

