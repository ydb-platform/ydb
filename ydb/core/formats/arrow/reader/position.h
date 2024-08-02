#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/formats/arrow/switch/compare.h>
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/json/writer/json_value.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/types.h>

namespace NKikimr::NArrow::NMerger {

class TRecordBatchBuilder;
class TSortableScanData;

class TCursor {
private:
    YDB_READONLY(ui64, Position, 0);
    std::vector<NAccessor::IChunkedArray::TFullDataAddress> PositionAddress;
public:
    TCursor() = default;
    TCursor(const std::shared_ptr<arrow::Table>& table, const ui64 position, const std::vector<std::string>& columns);

    TCursor(const ui64 position, const std::vector<NAccessor::IChunkedArray::TFullDataAddress>& addresses)
        : Position(position)
        , PositionAddress(addresses)
    {

    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        for (ui32 i = 0; i < PositionAddress.size(); ++i) {
            auto& jsonColumn = result["sorting_columns"].AppendValue(NJson::JSON_MAP);
            jsonColumn["value"] = PositionAddress[i].DebugString(Position);
        }
        return result;
    }

    std::shared_ptr<arrow::RecordBatch> ExtractSortingPosition(const std::vector<std::shared_ptr<arrow::Field>>& fields) const {
        AFL_VERIFY(fields.size() == PositionAddress.size());
        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields);
        for (ui32 i = 0; i < PositionAddress.size(); ++i) {
            auto extracted = PositionAddress[i].CopyRecord(Position);
            columns.emplace_back(extracted);
        }
        return arrow::RecordBatch::Make(schema, 1, columns);
    }

    void AppendPositionTo(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, ui64* recordSize) const;

    std::partial_ordering Compare(const TSortableScanData& item, const ui64 itemPosition) const;
    std::partial_ordering Compare(const TCursor& item) const;

};

class TSortableScanData {
private:
    ui64 RecordsCount = 0;
    YDB_READONLY_DEF(std::vector<NAccessor::IChunkedArray::TFullDataAddress>, PositionAddress);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<NAccessor::IChunkedArray>>, Columns);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Field>>, Fields);
    ui64 StartPosition = 0;
    ui64 FinishPosition = 0;
    void BuildPosition(const ui64 position);
    ui64 LastInit = 0;

    bool Contains(const ui64 position) const {
        return StartPosition <= position && position < FinishPosition;
    }
public:
    TSortableScanData(const ui64 position, const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columns);
    TSortableScanData(const ui64 position, const std::shared_ptr<arrow::Table>& batch, const std::vector<std::string>& columns);
    TSortableScanData(const ui64 position, const std::shared_ptr<TGeneralContainer>& batch, const std::vector<std::string>& columns);
    TSortableScanData(const ui64 position, const ui64 recordsCount, const std::vector<std::shared_ptr<NAccessor::IChunkedArray>>& columns, const std::vector<std::shared_ptr<arrow::Field>>& fields)
        : RecordsCount(recordsCount)
        , Columns(columns)
        , Fields(fields)
    {
        BuildPosition(position);
    }

    const NAccessor::IChunkedArray::TFullDataAddress& GetPositionAddress(const ui32 colIdx) const {
        AFL_VERIFY(colIdx < PositionAddress.size());
        return PositionAddress[colIdx];
    }

    ui32 GetPositionInChunk(const ui32 colIdx, const ui32 pos) const {
        AFL_VERIFY(colIdx < PositionAddress.size());
        return PositionAddress[colIdx].GetAddress().GetLocalIndex(pos);
    }

    std::shared_ptr<TSortableScanData> BuildCopy(const ui64 /*position*/) const {
        return std::make_shared<TSortableScanData>(*this);
    }

    TCursor BuildCursor(const ui64 position) const {
        if (Contains(position)) {
            return TCursor(position, PositionAddress);
        }
        auto addresses = PositionAddress;
        ui32 idx = 0;
        for (auto&& i : addresses) {
            if (!i.GetAddress().Contains(position)) {
                i = Columns[idx]->GetChunk(i.GetAddress(), position);
            }
            ++idx;
        }
        return TCursor(position, addresses);
    }

    std::partial_ordering Compare(const ui64 position, const TSortableScanData& item, const ui64 itemPosition) const {
        AFL_VERIFY(PositionAddress.size() == item.PositionAddress.size());
        if (Contains(position) && item.Contains(itemPosition)) {
            for (ui32 idx = 0; idx < PositionAddress.size(); ++idx) {
                std::partial_ordering cmp = PositionAddress[idx].Compare(position, item.PositionAddress[idx], itemPosition);
                if (cmp != std::partial_ordering::equivalent) {
                    return cmp;
                }
            }
        } else {
            for (ui32 idx = 0; idx < PositionAddress.size(); ++idx) {
                std::partial_ordering cmp = std::partial_ordering::equivalent;
                const bool containsSelf = PositionAddress[idx].GetAddress().Contains(position);
                const bool containsItem = item.PositionAddress[idx].GetAddress().Contains(itemPosition);
                if (containsSelf && containsItem) {
                    cmp = PositionAddress[idx].Compare(position, item.PositionAddress[idx], itemPosition);
                } else if (containsSelf) {
                    auto temporaryAddress = item.Columns[idx]->GetChunk(item.PositionAddress[idx].GetAddress(), itemPosition);
                    cmp = PositionAddress[idx].Compare(position, temporaryAddress, itemPosition);
                } else if (containsItem) {
                    auto temporaryAddress = Columns[idx]->GetChunk(PositionAddress[idx].GetAddress(), position);
                    cmp = temporaryAddress.Compare(position, item.PositionAddress[idx], itemPosition);
                } else {
                    AFL_VERIFY(false);
                }
                if (cmp != std::partial_ordering::equivalent) {
                    return cmp;
                }
            }
        }

        return std::partial_ordering::equivalent;
    }

    void AppendPositionTo(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const ui64 position, ui64* recordSize) const;

    [[nodiscard]] bool InitPosition(const ui64 position);

    std::shared_ptr<arrow::Table> Slice(const ui64 offset, const ui64 count) const {
        std::vector<std::shared_ptr<arrow::ChunkedArray>> slicedArrays;
        for (auto&& i : Columns) {
            slicedArrays.emplace_back(i->Slice(offset, count));
        }
        return arrow::Table::Make(std::make_shared<arrow::Schema>(Fields), slicedArrays, count);
    }

    bool IsSameSchema(const std::shared_ptr<arrow::Schema>& schema) const {
        if (Fields.size() != (size_t)schema->num_fields()) {
            return false;
        }
        for (ui32 i = 0; i < Fields.size(); ++i) {
            if (!Fields[i]->type()->Equals(schema->field(i)->type())) {
                return false;
            }
            if (Fields[i]->name() != schema->field(i)->name()) {
                return false;
            }
        }
        return true;
    }

    NJson::TJsonValue DebugJson(const ui64 position) const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& jsonFields = result.InsertValue("fields", NJson::JSON_ARRAY);
        for (auto&& i : Fields) {
            jsonFields.AppendValue(i->ToString());
        }
        for (ui32 i = 0; i < Columns.size(); ++i) {
            auto& jsonColumn = result["sorting_columns"].AppendValue(NJson::JSON_MAP);
            jsonColumn["name"] = Fields[i]->name();
            jsonColumn["value"] = PositionAddress[i].DebugString(position);
        }
        return result;
    }

    std::vector<std::string> GetFieldNames() const {
        std::vector<std::string> result;
        for (auto&& i : Fields) {
            result.emplace_back(i->name());
        }
        return result;
    }
};

class TRWSortableBatchPosition;

class TSortableBatchPosition {
protected:
    i64 Position = 0;
    i64 RecordsCount = 0;
    bool ReverseSort = false;
    std::shared_ptr<TSortableScanData> Sorting;
    std::shared_ptr<TSortableScanData> Data;

    TSortableBatchPosition(const i64 position, const i64 recordsCount, const bool reverseSort, const std::shared_ptr<TSortableScanData>& sorting,
        const std::shared_ptr<TSortableScanData>& data)
        : Position(position)
        , RecordsCount(recordsCount)
        , ReverseSort(reverseSort)
        , Sorting(sorting)
        , Data(data) {
        AFL_VERIFY(IsAvailablePosition(Position));
    }

public:
    TSortableBatchPosition() = default;

    i64 GetPosition() const {
        return Position;
    }

    i64 GetRecordsCount() const {
        return RecordsCount;
    }

    const std::shared_ptr<TSortableScanData>& GetSorting() const {
        return Sorting;
    }

    TCursor BuildSortingCursor() const {
        return Sorting->BuildCursor(Position);
    }

    TCursor BuildDataCursor() const {
        if (!Data) {
            return TCursor();
        }
        return Data->BuildCursor(Position);
    }

    const std::vector<std::shared_ptr<arrow::Field>>& GetSortFields() const {
        return Sorting->GetFields();
    }

    TSortableBatchPosition(const TRWSortableBatchPosition& source) = delete;
    TSortableBatchPosition(TRWSortableBatchPosition& source) = delete;
    TSortableBatchPosition(TRWSortableBatchPosition&& source) = delete;

    TSortableBatchPosition operator= (const TRWSortableBatchPosition& source) = delete;
    TSortableBatchPosition operator= (TRWSortableBatchPosition& source) = delete;
    TSortableBatchPosition operator= (TRWSortableBatchPosition&& source) = delete;

    TRWSortableBatchPosition BuildRWPosition(const bool needData, const bool deepCopy) const;

    std::shared_ptr<arrow::Table> SliceData(const ui64 offset, const ui64 count) const {
        AFL_VERIFY(Data);
        return Data->Slice(offset, count);
    }

    std::shared_ptr<arrow::Table> SliceKeys(const ui64 offset, const ui64 count) const {
        AFL_VERIFY(Sorting);
        return Sorting->Slice(offset, count);
    }

    class TFoundPosition {
    private:
        YDB_READONLY(ui32, Position, 0);
        std::optional<bool> GreaterIfNotEqual;
        explicit TFoundPosition(const ui32 pos, const bool greater)
            : Position(pos) {
            GreaterIfNotEqual = greater;
        }
        explicit TFoundPosition(const ui32 pos)
            : Position(pos) {
        }
    public:
        TString DebugString() const {
            TStringBuilder result;
            result << "pos=" << Position << ";";
            if (!GreaterIfNotEqual) {
                result << "state=equal;";
            } else if (*GreaterIfNotEqual) {
                result << "state=greater;";
            } else {
                result << "state=less;";
            }
            return result;
        }

        bool IsEqual() const {
            return !GreaterIfNotEqual;
        }
        bool IsLess() const {
            return !!GreaterIfNotEqual && !*GreaterIfNotEqual;
        }
        bool IsGreater() const {
            return !!GreaterIfNotEqual && *GreaterIfNotEqual;
        }

        static TFoundPosition Less(const ui32 pos) {
            return TFoundPosition(pos, false);
        }
        static TFoundPosition Greater(const ui32 pos) {
            return TFoundPosition(pos, true);
        }
        static TFoundPosition Equal(const ui32 pos) {
            return TFoundPosition(pos);
        }
    };

    [[nodiscard]] bool IsAvailablePosition(const i64 position) const {
        return 0 <= position && position < RecordsCount;
    }

    static std::optional<TFoundPosition> FindPosition(const std::shared_ptr<arrow::RecordBatch>& batch, const TSortableBatchPosition& forFound,
        const bool needGreater, const std::optional<ui32> includedStartPosition);
    static std::optional<TSortableBatchPosition::TFoundPosition> FindPosition(TRWSortableBatchPosition& position, const ui64 posStart, const ui64 posFinish, const TSortableBatchPosition& forFound, const bool greater);

    const TSortableScanData& GetData() const {
        AFL_VERIFY(!!Data);
        return *Data;
    }

    bool IsReverseSort() const {
        return ReverseSort;
    }
    NJson::TJsonValue DebugJson() const;

    TRWSortableBatchPosition BuildRWPosition(std::shared_ptr<arrow::RecordBatch> batch, const ui32 position) const;

    bool IsSameSortingSchema(const std::shared_ptr<arrow::Schema>& schema) const {
        return Sorting->IsSameSchema(schema);
    }

    template <class TRecords>
    TSortableBatchPosition(const std::shared_ptr<TRecords>& batch, const ui32 position, const std::vector<std::string>& sortingColumns,
        const std::vector<std::string>& dataColumns, const bool reverseSort)
        : Position(position)
        , ReverseSort(reverseSort) {
        Y_ABORT_UNLESS(batch);
        Y_ABORT_UNLESS(batch->num_rows());
        RecordsCount = batch->num_rows();
        AFL_VERIFY(Position < RecordsCount)("position", Position)("count", RecordsCount);

        if (dataColumns.size()) {
            Data = std::make_shared<TSortableScanData>(Position, batch, dataColumns);
        }
        Sorting = std::make_shared<TSortableScanData>(Position, batch, sortingColumns);
        Y_DEBUG_ABORT_UNLESS(batch->ValidateFull().ok());
        Y_ABORT_UNLESS(Sorting->GetColumns().size());
    }

    std::partial_ordering GetReverseForCompareResult(const std::partial_ordering directResult) const {
        if (directResult == std::partial_ordering::less) {
            return std::partial_ordering::greater;
        } else if (directResult == std::partial_ordering::greater) {
            return std::partial_ordering::less;
        } else {
            return directResult;
        }
    }

    std::partial_ordering ApplyOptionalReverseForCompareResult(const std::partial_ordering directResult) const {
        if (ReverseSort) {
            return GetReverseForCompareResult(directResult);
        } else {
            return directResult;
        }
    }

    std::partial_ordering Compare(const TCursor& cursor) const {
        if (ReverseSort) {
            return cursor.Compare(*Sorting, Position);
        } else {
            return GetReverseForCompareResult(cursor.Compare(*Sorting, Position));
        }
    }

    std::partial_ordering Compare(const TSortableBatchPosition& item) const {
        Y_ABORT_UNLESS(item.ReverseSort == ReverseSort);
        Y_ABORT_UNLESS(item.Sorting->GetColumns().size() == Sorting->GetColumns().size());
        const auto directResult = Sorting->Compare(Position, *item.Sorting, item.GetPosition());
        return ApplyOptionalReverseForCompareResult(directResult);
    }

    std::partial_ordering Compare(const TSortableScanData& data, const ui64 dataPosition) const {
        return Sorting->Compare(Position, data, dataPosition);
    }

    bool operator<(const TSortableBatchPosition& item) const {
        return Compare(item) == std::partial_ordering::less;
    }

    bool operator==(const TSortableBatchPosition& item) const {
        return Compare(item) == std::partial_ordering::equivalent;
    }

    bool operator!=(const TSortableBatchPosition& item) const {
        return Compare(item) != std::partial_ordering::equivalent;
    }

};

class TIntervalPosition {
private:
    TSortableBatchPosition Position;
    bool LeftIntervalInclude;
public:
    const TSortableBatchPosition& GetPosition() const {
        return Position;
    }
    bool IsIncludedToLeftInterval() const {
        return LeftIntervalInclude;
    }
    TIntervalPosition(TSortableBatchPosition&& position, const bool leftIntervalInclude)
        : Position(std::move(position))
        , LeftIntervalInclude(leftIntervalInclude) {

    }

    TIntervalPosition(const TSortableBatchPosition& position, const bool leftIntervalInclude)
        : Position(position)
        , LeftIntervalInclude(leftIntervalInclude) {

    }

    bool operator<(const TIntervalPosition& item) const {
        std::partial_ordering cmp = Position.Compare(item.Position);
        if (cmp == std::partial_ordering::equivalent) {
            return (LeftIntervalInclude ? 1 : 0) < (item.LeftIntervalInclude ? 1 : 0);
        } 
        return cmp == std::partial_ordering::less;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("position", Position.DebugJson());
        result.InsertValue("include", LeftIntervalInclude);
        return result;
    }
};

class TIntervalPositions {
private:
    std::vector<TIntervalPosition> Positions;
public:
    bool IsEmpty() const {
        return Positions.empty();
    }

    std::vector<TIntervalPosition>::const_iterator begin() const {
        return Positions.begin();
    }

    std::vector<TIntervalPosition>::const_iterator end() const {
        return Positions.end();
    }

    void InsertPosition(TIntervalPosition&& intervalPosition) {
        Positions.emplace_back(std::move(intervalPosition));
        ui32 index = Positions.size() - 1;
        while (index >= 1 && Positions[index] < Positions[index - 1]) {
            std::swap(Positions[index], Positions[index - 1]);
            index = index - 1;
        }
    }

    void InsertPosition(TSortableBatchPosition&& position, const bool includePositionToLeftInterval) {
        TIntervalPosition intervalPosition(std::move(position), includePositionToLeftInterval);
        InsertPosition(std::move(intervalPosition));
    }

    void InsertPosition(const TSortableBatchPosition& position, const bool includePositionToLeftInterval) {
        TIntervalPosition intervalPosition(position, includePositionToLeftInterval);
        InsertPosition(std::move(intervalPosition));
    }

    void AddPosition(TIntervalPosition&& intervalPosition) {
        if (Positions.size()) {
            AFL_VERIFY(Positions.back() < intervalPosition)("back", Positions.back().DebugJson())("pos", intervalPosition.DebugJson());
        }
        Positions.emplace_back(std::move(intervalPosition));
    }

    void AddPosition(TSortableBatchPosition&& position, const bool includePositionToLeftInterval) {
        TIntervalPosition intervalPosition(std::move(position), includePositionToLeftInterval);
        AddPosition(std::move(intervalPosition));
    }

    void AddPosition(const TSortableBatchPosition& position, const bool includePositionToLeftInterval) {
        TIntervalPosition intervalPosition(position, includePositionToLeftInterval);
        AddPosition(std::move(intervalPosition));
    }
};

class TRWSortableBatchPosition: public TSortableBatchPosition, public TMoveOnly {
private:
    using TBase = TSortableBatchPosition;
public:
    using TBase::TBase;

    [[nodiscard]] bool NextPosition(const i64 delta) {
        return InitPosition(Position + delta);
    }

    [[nodiscard]] bool InitPosition(const i64 position) {
        if (!IsAvailablePosition(position)) {
            return false;
        }
        AFL_VERIFY(Sorting->InitPosition(position))("pos", position)("count", RecordsCount);
        if (Data) {
            AFL_VERIFY(Data->InitPosition(position))("pos", position)("count", RecordsCount);
        }
        Position = position;
        return true;
    }

    class TAsymmetricPositionGuard: TNonCopyable {
    private:
        TRWSortableBatchPosition& Owner;
    public:
        TAsymmetricPositionGuard(TRWSortableBatchPosition& owner)
            : Owner(owner)
        {
        }

        [[nodiscard]] bool InitSortingPosition(const i64 position) {
            if (!Owner.IsAvailablePosition(position)) {
                return false;
            }
            AFL_VERIFY(Owner.Sorting->InitPosition(position));
            Owner.Position = position;
            return true;
        }

        ~TAsymmetricPositionGuard() {
            if (Owner.IsAvailablePosition(Owner.Position)) {
                if (Owner.Data) {
                    AFL_VERIFY(Owner.Data->InitPosition(Owner.Position));
                }
            }
        }
    };

    TAsymmetricPositionGuard CreateAsymmetricAccessGuard() {
        return TAsymmetricPositionGuard(*this);
    }

    TSortableBatchPosition::TFoundPosition SkipToLower(const TSortableBatchPosition& forFound);

    //  (-inf, it1), [it1, it2), [it2, it3), ..., [itLast, +inf)
    template <class TBordersIterator>
    static std::vector<std::shared_ptr<arrow::RecordBatch>> SplitByBorders(const std::shared_ptr<arrow::RecordBatch>& batch,
        const std::vector<std::string>& columnNames, TBordersIterator& it) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> result;
        if (!batch || batch->num_rows() == 0) {
            while (it.IsValid()) {
                result.emplace_back(nullptr);
                it.Next();
            }
            result.emplace_back(nullptr);
            return result;
        }
        TRWSortableBatchPosition pos(batch, 0, columnNames, {}, false);
        bool batchFinished = false;
        i64 recordsCountSplitted = 0;
        for (; it.IsValid() && !batchFinished; it.Next()) {
            const ui32 startPos = pos.GetPosition();
            auto posFound = pos.SkipToLower(it.CurrentPosition());
            if (posFound.IsGreater() || posFound.IsEqual()) {
                if (posFound.GetPosition() == startPos) {
                    result.emplace_back(nullptr);
                } else {
                    result.emplace_back(batch->Slice(startPos, posFound.GetPosition() - startPos));
                    recordsCountSplitted += result.back()->num_rows();
                }
            } else {
                result.emplace_back(batch->Slice(startPos, posFound.GetPosition() - startPos + 1));
                recordsCountSplitted += result.back()->num_rows();
                batchFinished = true;
            }
        }
        if (batchFinished) {
            for (; it.IsValid(); it.Next()) {
                result.emplace_back(nullptr);
            }
            result.emplace_back(nullptr);
        } else {
            AFL_VERIFY(!it.IsValid());
            result.emplace_back(batch->Slice(pos.GetPosition()));
            AFL_VERIFY(result.back()->num_rows());
            recordsCountSplitted += result.back()->num_rows();
        }
        AFL_VERIFY(batch->num_rows() == recordsCountSplitted);
        return result;
    }

    template <class TContainer>
    class TAssociatedContainerIterator {
    private:
        typename TContainer::const_iterator Current;
        typename TContainer::const_iterator End;
    public:
        TAssociatedContainerIterator(const TContainer& container)
            : Current(container.begin())
            , End(container.end()) {
        }

        bool IsValid() const {
            return Current != End;
        }

        void Next() {
            ++Current;
        }

        const auto& CurrentPosition() const {
            return Current->first;
        }
    };

    template <class TContainer>
    static std::vector<std::shared_ptr<arrow::RecordBatch>> SplitByBordersInAssociativeContainer(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columnNames, const TContainer& container) {
        TAssociatedContainerIterator<TContainer> it(container);
        return SplitByBorders(batch, columnNames, it);
    }

    template <class TContainer>
    class TSequentialContainerIterator {
    private:
        typename TContainer::const_iterator Current;
        typename TContainer::const_iterator End;
    public:
        TSequentialContainerIterator(const TContainer& container)
            : Current(container.begin())
            , End(container.end()) {
        }

        bool IsValid() const {
            return Current != End;
        }

        void Next() {
            ++Current;
        }

        const auto& CurrentPosition() const {
            return *Current;
        }
    };

    template <class TContainer>
    static std::vector<std::shared_ptr<arrow::RecordBatch>> SplitByBordersInSequentialContainer(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::string>& columnNames, const TContainer& container) {
        TSequentialContainerIterator<TContainer> it(container);
        return SplitByBorders(batch, columnNames, it);
    }

};

}
