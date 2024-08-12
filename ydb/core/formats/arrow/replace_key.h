#pragma once
#include "arrow_helpers.h"
#include "permutations.h"

#include "common/validation.h"
#include "switch/compare.h"

#include <ydb/core/base/defs.h>

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>

#include <util/string/builder.h>
#include <util/string/join.h>

#include <compare>

namespace NKikimr::NArrow {

using TArrayVec = std::vector<std::shared_ptr<arrow::Array>>;

template<typename TArrayVecPtr>
class TReplaceKeyTemplate {
public:
    static constexpr bool IsOwning = std::is_same_v<TArrayVecPtr, std::shared_ptr<TArrayVec>>;

    void ShrinkToFit() {
        if (Columns->front()->length() == 1) {
            Y_ABORT_UNLESS(Position == 0);
        } else {
            auto columnsNew = std::make_shared<TArrayVec>();
            for (auto&& i : *Columns) {
                columnsNew->emplace_back(NArrow::CopyRecords(i, { Position }));
            }
            Columns = columnsNew;
            Position = 0;
        }
    }

    TString DebugString() const {
        TStringBuilder sb;
        for (auto&& i : *Columns) {
            auto res = i->GetScalar(Position);
            if (!res.ok()) {
                sb << res.status().ToString() << ";";
            } else {
                sb << (*res)->ToString() << ";";
            }
        }
        return sb;
    }

    TReplaceKeyTemplate(TArrayVecPtr columns, const ui64 position)
        : Columns(columns)
        , Position(position)
    {
        Y_ABORT_UNLESS(Size() > 0 && Position < (ui64)Column(0).length());
        for (auto&& i : *Columns) {
            Types.emplace_back(i->type_id());
        }
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    TReplaceKeyTemplate(TArrayVec&& columns, const ui64 position)
        : Columns(std::make_shared<TArrayVec>(std::move(columns)))
        , Position(position)
    {
        Y_ABORT_UNLESS(Size() > 0 && Position < (ui64)Column(0).length());
        for (auto&& i : *Columns) {
            Types.emplace_back(i->type_id());
        }
    }

    template<typename T>
    bool operator == (const TReplaceKeyTemplate<T>& key) const {
        Y_ABORT_UNLESS(Size() == key.Size());

        for (ui32 i = 0; i < Size(); ++i) {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp)) {
                return false;
            }
        }
        return true;
    }

    template<typename T>
    std::partial_ordering operator <=> (const TReplaceKeyTemplate<T>& key) const {
        Y_ABORT_UNLESS(Size() == key.Size());

        for (ui32 i = 0; i < Size(); ++i) {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template<typename T>
    std::partial_ordering CompareNotNull(const TReplaceKeyTemplate<T>& key) const {
        Y_ABORT_UNLESS(Size() == key.Size());

        for (ui32 i = 0; i < Size(); ++i) {
            auto cmp = CompareColumnValueNotNull(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template<typename T>
    std::partial_ordering ComparePartNotNull(const TReplaceKeyTemplate<T>& key, const ui32 size) const {
        Y_ABORT_UNLESS(size <= key.Size());
        Y_ABORT_UNLESS(size <= Size());

        for (ui32 i = 0; i < size; ++i) {
            auto cmp = CompareColumnValueNotNull(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template<typename T>
    std::partial_ordering CompareColumnValueNotNull(int column, const TReplaceKeyTemplate<T>& key, int keyColumn) const {
        Y_DEBUG_ABORT_UNLESS(Column(column).type_id() == key.Column(keyColumn).type_id());

        return TComparator::TypedCompare<true>(Column(column), Position, key.Column(keyColumn), key.Position);
    }

    template<typename T>
    std::partial_ordering CompareColumnValue(int column, const TReplaceKeyTemplate<T>& key, int keyColumn) const {
        Y_DEBUG_ABORT_UNLESS(Column(column).type_id() == key.Column(keyColumn).type_id());
        Y_DEBUG_ABORT_UNLESS(Types[column] == Column(column).type_id());
        Y_DEBUG_ABORT_UNLESS(Types.size() == Size());

        return TComparator::ConcreteTypedCompare<false>(Types[column], Column(column), Position, key.Column(keyColumn), key.Position);
    }

    ui64 Size() const {
        Y_DEBUG_ABORT_UNLESS(Columns);
        return Columns->size();
    }

    int GetPosition() const {
        return Position;
    }

    const arrow::Array& Column(int i) const {
        Y_DEBUG_ABORT_UNLESS(Columns);
        Y_DEBUG_ABORT_UNLESS((size_t)i < Columns->size());
        Y_DEBUG_ABORT_UNLESS((*Columns)[i]);
        return *(*Columns)[i];
    }

    std::shared_ptr<arrow::Array> ColumnPtr(int i) const {
        Y_DEBUG_ABORT_UNLESS(Columns);
        Y_DEBUG_ABORT_UNLESS((size_t)i < Columns->size());
        return (*Columns)[i];
    }

    TReplaceKeyTemplate<const TArrayVec*> ToRaw() const {
        if constexpr (IsOwning) {
            return TReplaceKeyTemplate<const TArrayVec*>(Columns.get(), Position);
        } else {
            return *this;
        }
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    std::shared_ptr<arrow::RecordBatch> RestoreBatch(const std::shared_ptr<arrow::Schema>& schema) const {
        AFL_VERIFY(Size() && Size() == (ui32)schema->num_fields())("columns", DebugString())("schema", JoinSeq(",", schema->field_names()));
        const auto& columns = *Columns;
        return arrow::RecordBatch::Make(schema, columns[0]->length(), columns);
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Schema>& schema) const {
        auto batch = RestoreBatch(schema);
        Y_ABORT_UNLESS(Position < (ui64)batch->num_rows());
        return batch->Slice(Position, 1);
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    static TReplaceKeyTemplate<TArrayVecPtr> FromBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                       const std::shared_ptr<arrow::Schema>& key, int row) {
        Y_ABORT_UNLESS(key->num_fields() <= batch->num_columns());

        TArrayVec columns;
        columns.reserve(key->num_fields());
        for (int i = 0; i < key->num_fields(); ++i) {
            auto& keyField = key->field(i);
            auto array = batch->GetColumnByName(keyField->name());
            Y_ABORT_UNLESS(array);
            Y_ABORT_UNLESS(keyField->type()->Equals(array->type()));
            columns.push_back(array);
        }

        return TReplaceKeyTemplate<TArrayVecPtr>(std::move(columns), row);
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    static TReplaceKeyTemplate<TArrayVecPtr> FromBatch(const std::shared_ptr<arrow::RecordBatch>& batch, int row) {
        auto columns = std::make_shared<TArrayVec>(batch->columns());
        return TReplaceKeyTemplate<TArrayVecPtr>(columns, row);
    }

    static TReplaceKeyTemplate<TArrayVecPtr> FromScalar(const std::shared_ptr<arrow::Scalar>& s) {
        Y_DEBUG_ABORT_UNLESS(IsGoodScalar(s));
        auto res = MakeArrayFromScalar(*s, 1);
        Y_ABORT_UNLESS(res.status().ok(), "%s", res.status().ToString().c_str());
        return TReplaceKeyTemplate<TArrayVecPtr>(std::make_shared<TArrayVec>(1, *res), 0);
    }

    static std::shared_ptr<arrow::Scalar> ToScalar(const TReplaceKeyTemplate<TArrayVecPtr>& key, int colNumber = 0) {
        Y_DEBUG_ABORT_UNLESS(colNumber < key.Size());
        auto& column = key.Column(colNumber);
        auto res = column.GetScalar(key.GetPosition());
        Y_ABORT_UNLESS(res.status().ok(), "%s", res.status().ToString().c_str());
        Y_DEBUG_ABORT_UNLESS(IsGoodScalar(*res));
        return *res;
    }

private:
    TArrayVecPtr Columns = nullptr;
    std::vector<arrow::Type::type> Types;
    ui64 Position = 0;

};

using TReplaceKey = TReplaceKeyTemplate<std::shared_ptr<TArrayVec>>;

class TReplaceKeyInterval {
private:
    NArrow::TReplaceKey Start;
    NArrow::TReplaceKey Finish;
public:
    TReplaceKeyInterval(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& finish)
        : Start(start)
        , Finish(finish)
    {

    }

    const NArrow::TReplaceKey& GetStart() const {
        return Start;
    }

    const NArrow::TReplaceKey& GetFinish() const {
        return Finish;
    }

    void SetFinish(const NArrow::TReplaceKey& key) {
        Finish = key;
    }
};

using TRawReplaceKey = TReplaceKeyTemplate<const TArrayVec*>;

class TStoreReplaceKey: public TReplaceKey {
private:
    using TBase = TReplaceKey;
public:
    TStoreReplaceKey(const TReplaceKey& baseKey)
        : TBase(baseKey)
    {
        TBase::ShrinkToFit();
    }
};

class TReplaceKeyHelper {
public:
    static size_t LowerBound(const std::vector<TRawReplaceKey>& batchKeys, const TReplaceKey& key, size_t offset);
};

}

