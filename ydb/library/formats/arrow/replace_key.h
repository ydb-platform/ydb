#pragma once
#include "arrow_helpers.h"
#include "permutations.h"

#include "switch/compare.h"
#include "switch/switch_type.h"

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/digest/fnv.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <compare>

namespace NKikimr::NArrow {

using TArrayVec = std::vector<std::shared_ptr<arrow::Array>>;

template <typename TArrayVecPtr>
class TReplaceKeyTemplate {
protected:
    TArrayVecPtr Columns = nullptr;
    ui64 Position = 0;

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

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_ARRAY;
        for (auto&& i : *Columns) {
            auto res = i->GetScalar(Position);
            if (!res.ok()) {
                result.AppendValue(res.status().ToString());
            } else {
                result.AppendValue((*res)->ToString());
            }
        }
        return result;
    }

    TReplaceKeyTemplate(TArrayVecPtr columns, const ui64 position)
        : Columns(columns)
        , Position(position) {
        Y_ABORT_UNLESS(GetColumnsCount() > 0 && Position < (ui64)Column(0).length());
    }

    template <typename T = TArrayVecPtr>
        requires IsOwning
    TReplaceKeyTemplate(TArrayVec&& columns, const ui64 position)
        : Columns(std::make_shared<TArrayVec>(std::move(columns)))
        , Position(position) {
        Y_ABORT_UNLESS(GetColumnsCount() > 0 && Position < (ui64)Column(0).length());
    }

    template <typename T>
    bool operator==(const TReplaceKeyTemplate<T>& key) const {
        Y_ABORT_UNLESS(GetColumnsCount() == key.GetColumnsCount());

        for (ui32 i = 0; i < GetColumnsCount(); ++i) {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp)) {
                return false;
            }
        }
        return true;
    }

    template <typename T>
    std::partial_ordering operator<=>(const TReplaceKeyTemplate<T>& key) const {
        Y_ABORT_UNLESS(GetColumnsCount() == key.GetColumnsCount());

        for (ui32 i = 0; i < GetColumnsCount(); ++i) {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T>
    std::partial_ordering CompareNotNull(const TReplaceKeyTemplate<T>& key) const {
        Y_ABORT_UNLESS(GetColumnsCount() == key.GetColumnsCount());

        for (ui32 i = 0; i < GetColumnsCount(); ++i) {
            auto cmp = CompareColumnValueNotNull(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T>
    std::partial_ordering ComparePartNotNull(const TReplaceKeyTemplate<T>& key, const ui32 size) const {
        Y_ABORT_UNLESS(size <= key.GetColumnsCount());
        Y_ABORT_UNLESS(size <= GetColumnsCount());

        for (ui32 i = 0; i < size; ++i) {
            auto cmp = CompareColumnValueNotNull(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T>
    std::partial_ordering CompareColumnValueNotNull(int column, const TReplaceKeyTemplate<T>& key, int keyColumn) const {
        Y_DEBUG_ABORT_UNLESS(Column(column).type_id() == key.Column(keyColumn).type_id());
        return TComparator::TypedCompare<true>(Column(column), Position, key.Column(keyColumn), key.GetPosition());
    }

    template <typename T>
    std::partial_ordering CompareColumnValue(int column, const TReplaceKeyTemplate<T>& key, int keyColumn) const {
        Y_DEBUG_ABORT_UNLESS(Column(column).type_id() == key.Column(keyColumn).type_id());
        return TComparator::TypedCompare<false>(Column(column), Position, key.Column(keyColumn), key.GetPosition());
    }

    ui64 GetColumnsCount() const {
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

    template <typename T = TArrayVecPtr>
    std::shared_ptr<arrow::RecordBatch> RestoreBatch(const std::shared_ptr<arrow::Schema>& schema) const {
        AFL_VERIFY(GetColumnsCount())("columns", DebugString())("schema", JoinSeq(",", schema->field_names()));
        AFL_VERIFY(GetColumnsCount() == (ui32)schema->num_fields())("columns", DebugString())("schema", JoinSeq(",", schema->field_names()));
        const auto& columns = *Columns;
        return arrow::RecordBatch::Make(schema, columns[0]->length(), columns);
    }

    template <typename T = TArrayVecPtr>
    std::vector<std::shared_ptr<arrow::Field>> RestoreFakeFields() const {
        AFL_VERIFY(GetColumnsCount())("columns", DebugString());
        return BuildFakeFields(*Columns);
    }

    template <typename T = TArrayVecPtr>
    std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Schema>& schema) const {
        auto batch = RestoreBatch(schema);
        Y_ABORT_UNLESS(Position < (ui64)batch->num_rows());
        return batch->Slice(Position, 1);
    }

    template <typename T = TArrayVecPtr>
        requires IsOwning
    static TReplaceKeyTemplate<TArrayVecPtr> FromBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& key, int row) {
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

    template <typename T = TArrayVecPtr>
        requires IsOwning
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
        Y_DEBUG_ABORT_UNLESS(colNumber < key.GetColumnsCount());
        auto& column = key.Column(colNumber);
        auto res = column.GetScalar(key.GetPosition());
        Y_ABORT_UNLESS(res.status().ok(), "%s", res.status().ToString().c_str());
        Y_DEBUG_ABORT_UNLESS(IsGoodScalar(*res));
        return *res;
    }

    const TArrayVecPtr& GetColumns() const {
        return Columns;
    }
};

class TReplaceKeyView;

class TReplaceKey: public TReplaceKeyTemplate<std::shared_ptr<TArrayVec>> {
private:
    using TBase = TReplaceKeyTemplate<std::shared_ptr<TArrayVec>>;

public:
    using TBase::TBase;
    TReplaceKey(TBase&& val)
        : TBase(std::move(val)) {
    }

    TReplaceKey(const TBase& val)
        : TBase(val) {
    }

    TReplaceKeyView GetView() const;
};

class TReplaceKeyView: public TReplaceKeyTemplate<const TArrayVec*> {
private:
    using TBase = TReplaceKeyTemplate<const TArrayVec*>;

public:
    using TBase::TBase;

    TReplaceKeyView(const std::vector<std::shared_ptr<arrow::Array>>& columns, const ui64 position)
        : TBase(&columns, position) {
    }

    TReplaceKey GetToStore() const {
        return TReplaceKey(std::make_shared<std::vector<std::shared_ptr<arrow::Array>>>(*GetColumns()), GetPosition());
    }
};

class TReplaceKeyHashable: public TReplaceKeyTemplate<const TArrayVec*> {
private:
    using TBase = TReplaceKeyTemplate<const TArrayVec*>;
    ui64 HashPrecalculated = 0;
    const std::vector<arrow::Type::type>* Types = nullptr;

    ui64 CalcHash() const {
        size_t result = 0;
        for (auto&& i : *Columns) {
            AFL_VERIFY(NArrow::SwitchType(i->type_id(), [&](const auto& type) {
                auto* arr = type.CastArray(i.get());
                result = CombineHashes<size_t>(result, type.CalcHash(type.GetValue(*arr, Position)));
                return true;
            }));
        }
        return result;
    }

public:
    ui64 GetHashPrecalculated() const {
        return HashPrecalculated;
    }

    bool operator==(const TReplaceKeyHashable& key) const {
        if (HashPrecalculated != key.HashPrecalculated) {
            return false;
        }
        const ui32 count = Columns->size();
        Y_ABORT_UNLESS(count == key.Columns->size());
        for (ui32 i = 0; i < count; ++i) {
            Y_DEBUG_ABORT_UNLESS(Column(i).type_id() == key.Column(i).type_id());
            if (std::is_neq(TComparator::ConcreteTypedCompare<true>((*Types)[i], Column(i), Position, key.Column(i), key.GetPosition()))) {
                return false;
            }
        }
        return true;
    }

    explicit operator size_t() const {
        return HashPrecalculated;
    }

    bool IsFinished() const {
        return (*GetColumns()).front()->length() == GetPosition();
    }

    void Next() {
        AFL_VERIFY(GetPosition() < (*GetColumns()).front()->length());
        ++Position;
        if (!IsFinished()) {
            HashPrecalculated = CalcHash();
        }
    }

    TReplaceKeyHashable(
        const std::vector<std::shared_ptr<arrow::Array>>& columns, const ui64 position, const std::vector<arrow::Type::type>& types)
        : TBase(&columns, position)
        , HashPrecalculated(CalcHash())
        , Types(&types) {
        AFL_VERIFY(Types);
    }

    TReplaceKey GetToStore() const {
        return TReplaceKey(std::make_shared<std::vector<std::shared_ptr<arrow::Array>>>(*GetColumns()), GetPosition());
    }
};

class TComparablePosition {
private:
    std::vector<std::shared_ptr<arrow::Array>> Arrays;
    std::vector<ui32> Positions;

public:
    ui32 GetMonoPosition() const {
        std::optional<ui32> result;
        for (auto&& i : Positions) {
            if (!result) {
                result = i;
            } else {
                AFL_VERIFY(*result == i);
            }
        }
        AFL_VERIFY(result);
        return *result;
    }

    const std::vector<std::shared_ptr<arrow::Array>>& GetArrays() const {
        return Arrays;
    }

    TComparablePosition(const TReplaceKey& key)
        : Arrays(*key.GetColumns())
        , Positions(Arrays.size(), key.GetPosition()) {
    }

    TComparablePosition(const TReplaceKeyView& key)
        : Arrays(*key.GetColumns())
        , Positions(Arrays.size(), key.GetPosition()) {
    }

    TComparablePosition(const std::shared_ptr<arrow::Table>& table, const ui32 position) {
        AFL_VERIFY(position < table->num_rows());
        for (auto&& col : table->columns()) {
            ui32 pos = 0;
            bool found = false;
            for (auto&& chunk : col->chunks()) {
                if (position < pos + chunk->length()) {
                    AFL_VERIFY(pos <= position);
                    Arrays.emplace_back(chunk);
                    Positions.emplace_back(position - pos);
                    found = true;
                    break;
                }
                pos += chunk->length();
            }
            AFL_VERIFY(found);
        }
    }

    TComparablePosition(const std::vector<std::shared_ptr<arrow::ChunkedArray>>& tableColumns, const ui32 position) {
        for (auto&& col : tableColumns) {
            ui32 pos = 0;
            bool found = false;
            for (auto&& chunk : col->chunks()) {
                if (position < pos + chunk->length()) {
                    AFL_VERIFY(pos <= position);
                    Arrays.emplace_back(chunk);
                    Positions.emplace_back(position - pos);
                    found = true;
                    break;
                }
                pos += chunk->length();
            }
            AFL_VERIFY(found);
        }
    }

    TComparablePosition(const std::shared_ptr<arrow::RecordBatch>& rb, const ui32 position) {
        AFL_VERIFY(position < rb->num_rows());
        for (auto&& col : rb->columns()) {
            Arrays.emplace_back(col);
            Positions.emplace_back(position);
        }
    }

    TString DebugString() const {
        TStringBuilder sb;
        for (ui32 i = 0; i < Arrays.size(); ++i) {
            auto res = Arrays[i]->GetScalar(Positions[i]);
            if (!res.ok()) {
                sb << res.status().ToString() << ";";
            } else {
                sb << (*res)->ToString() << ";";
            }
        }
        return sb;
    }

    template <bool NotNull = false>
    std::partial_ordering Compare(const TComparablePosition& pos) const {
        AFL_VERIFY(pos.Positions.size() == Positions.size());
        for (ui32 i = 0; i < Positions.size(); ++i) {
            AFL_VERIFY(Arrays[i]->type()->id() == pos.Arrays[i]->type()->id());
            const std::partial_ordering cmpResult =
                TComparator::TypedCompare<NotNull>(*Arrays[i], Positions[i], *pos.Arrays[i], pos.Positions[i]);
            if (cmpResult != std::partial_ordering::equivalent) {
                return cmpResult;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template <bool NotNull = false>
    std::partial_ordering ComparePartial(const TComparablePosition& pos) const {
        for (ui32 i = 0; i < std::min<ui32>(Positions.size(), pos.Positions.size()); ++i) {
            AFL_VERIFY(Arrays[i]->type()->id() == pos.Arrays[i]->type()->id());
            const std::partial_ordering cmpResult =
                TComparator::TypedCompare<NotNull>(*Arrays[i], Positions[i], *pos.Arrays[i], pos.Positions[i]);
            if (cmpResult != std::partial_ordering::equivalent) {
                return cmpResult;
            }
        }
        if (Positions.size() < pos.Positions.size()) {
            return std::partial_ordering::less;
        } else if (pos.Positions.size() < Positions.size()) {
            return std::partial_ordering::greater;
        }
        return std::partial_ordering::equivalent;
    }

    bool operator==(const TComparablePosition& pos) const {
        return Compare(pos) == std::partial_ordering::equivalent;
    }

    bool operator<(const TComparablePosition& pos) const {
        return Compare(pos) == std::partial_ordering::less;
    }
};

class TReplaceKeyInterval {
private:
    NArrow::TReplaceKey Start;
    NArrow::TReplaceKey Finish;

public:
    TReplaceKeyInterval(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& finish)
        : Start(start)
        , Finish(finish) {
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
        : TBase(baseKey) {
        TBase::ShrinkToFit();
    }
};

class TReplaceKeyHelper {
public:
    static size_t LowerBound(const std::vector<TRawReplaceKey>& batchKeys, const TReplaceKey& key, size_t offset);
};

template <bool desc, bool uniq>
static bool IsSelfSorted(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch->num_rows() < 2) {
        return true;
    }
    auto& columns = batch->columns();

    for (int i = 1; i < batch->num_rows(); ++i) {
        TRawReplaceKey prev(&columns, i - 1);
        TRawReplaceKey current(&columns, i);
        if constexpr (desc) {
            if (prev < current) {
                AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "prev < current")("current", current.DebugString())(
                    "prev", prev.DebugString());
                return false;
            }
        } else {
            if (current < prev) {
                AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "current < prev")("current", current.DebugString())(
                    "prev", prev.DebugString());
                return false;
            }
        }
        if constexpr (uniq) {
            if (prev == current) {
                AFL_DEBUG(NKikimrServices::ARROW_HELPER)("event", "equal")("current", current.DebugString())("prev", prev.DebugString());
                return false;
            }
        }
    }
    return true;
}

}   // namespace NKikimr::NArrow

namespace std {
template <>
struct hash<NKikimr::NArrow::TReplaceKeyHashable> {
    std::size_t operator()(const NKikimr::NArrow::TReplaceKeyHashable& obj) const {
        return obj.GetHashPrecalculated();
    }
    hash() = default;
};
}   // namespace std
