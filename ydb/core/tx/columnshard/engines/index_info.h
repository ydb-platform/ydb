#pragma once

#include "defs.h"
#include "scalars.h"
#include "tier_info.h"

#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>

namespace arrow {
    class Array;
    class Field;
    class Schema;
}

namespace NKikimr::NArrow {
    struct TSortDescription;
}

namespace NKikimr::NOlap {

struct TInsertedData;

using TNameTypeInfo = std::pair<TString, NScheme::TTypeInfo>;

/// Column engine index description in terms of tablet's local table.
/// We have to use YDB types for keys here.
struct TIndexInfo : public NTable::TScheme::TTableSchema {
public:
    static constexpr const char* SPEC_COL_PLAN_STEP = "_yql_plan_step";
    static constexpr const char* SPEC_COL_TX_ID = "_yql_tx_id";
    static const TString STORE_INDEX_STATS_TABLE;
    static const TString TABLE_INDEX_STATS_TABLE;

    enum class ESpecialColumn : ui32 {
        PLAN_STEP = 0xffffff00,
        TX_ID,
    };

    /// Appends the special columns to the batch.
    static std::shared_ptr<arrow::RecordBatch> AddSpecialColumns(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        const ui64 platStep,
        const ui64 txId);

    /// Makes schema as set of the special columns.
    static std::shared_ptr<arrow::Schema> ArrowSchemaSnapshot();

    /// Matches name of the filed with names of the special columns.
    static bool IsSpecialColumn(const arrow::Field& field);
    static bool IsSpecialColumn(const ui32 field);
    static bool IsSpecialColumn(const std::string& fieldName);
    template <class TContainer>
    static bool IsSpecialColumns(const TContainer& c) {
        for (auto&& i : c) {
            if (!IsSpecialColumn(i)) {
                return false;
            }
        }
        return true;
    }
public:
    TIndexInfo(const TString& name, ui32 id);

    /// Returns id of the index.
    ui32 GetId() const noexcept {
        return Id;
    }

    /// Returns an id of the column located by name. The name should exists in the schema.
    ui32 GetColumnId(const std::string& name) const;
    std::optional<ui32> GetColumnIdOptional(const std::string& name) const;

    /// Returns a name of the column located by id.
    TString GetColumnName(ui32 id, bool required = true) const;

    /// Returns names of columns defined by the specific ids.
    TVector<TString> GetColumnNames(const TVector<ui32>& ids) const;

    /// Returns info of columns defined by specific ids.
    TVector<TNameTypeInfo> GetColumns(const TVector<ui32>& ids) const;

    /// Traditional Primary Key (includes uniqueness, search and sorting logic)
    TVector<TNameTypeInfo> GetPrimaryKey() const {
        return GetColumns(KeyColumns);
    }

    /// Returns id of the first column of the primary key.
    ui32 GetPKFirstColumnId() const {
        Y_VERIFY(KeyColumns.size());
        return KeyColumns[0];
    }

    // Sorting key: could be less or greater then traditional PK
    // It could be empty for append-only tables. It could be greater then PK for better columns compression.
    // If sorting key includes uniqueness key as a prefix we are able to use MergeSort for REPLACE.
    const std::shared_ptr<arrow::Schema>& GetSortingKey() const { return SortingKey; }
    const std::shared_ptr<arrow::Schema>& GetReplaceKey() const { return ReplaceKey; }
    const std::shared_ptr<arrow::Schema>& GetExtendedKey() const { return ExtendedKey; }
    const std::shared_ptr<arrow::Schema>& GetIndexKey() const { return IndexKey; }

    const std::shared_ptr<arrow::Schema> GetEffectiveKey() const {
        // TODO: composite key
        Y_VERIFY(IndexKey->num_fields() == 1);
        return std::make_shared<arrow::Schema>(arrow::FieldVector{GetIndexKey()->field(0)});
    }

    /// Initializes sorting, replace, index and extended keys.
    void SetAllKeys();

    void CheckTtlColumn(const TString& ttlColumn) const {
        Y_VERIFY(!ttlColumn.empty());
        Y_VERIFY(MinMaxIdxColumnsIds.contains(GetColumnId(ttlColumn)));
    }

    TVector<ui32> GetColumnIds(const TVector<TString>& columnNames) const;

    std::shared_ptr<arrow::Schema> ArrowSchema() const;
    std::shared_ptr<arrow::Schema> ArrowSchemaWithSpecials() const;
    std::shared_ptr<arrow::Schema> AddColumns(const std::shared_ptr<arrow::Schema>& schema,
                                              const TVector<TString>& columns) const;

    std::shared_ptr<arrow::Schema> ArrowSchema(const TVector<ui32>& columnIds, bool withSpecials = false) const;
    std::shared_ptr<arrow::Schema> ArrowSchema(const TVector<TString>& columnNames) const;
    std::shared_ptr<arrow::Field> ArrowColumnField(ui32 columnId) const;
    std::shared_ptr<arrow::RecordBatch> PrepareForInsert(const TString& data, const TString& metadata,
                                                         TString& strError) const;

    const THashSet<TString>& GetRequiredColumns() const {
        return RequiredColumns;
    }

    const THashSet<ui32>& GetMinMaxIdxColumns() const {
        return MinMaxIdxColumnsIds;
    }

    bool AllowTtlOverColumn(const TString& name) const;

    /// Returns whether the sorting keys defined.
    bool IsSorted() const { return SortingKey.get(); }

     /// Returns whether the replace keys defined.
    bool IsReplacing() const { return ReplaceKey.get(); }

    std::shared_ptr<NArrow::TSortDescription> SortDescription() const;
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription() const;

    void SetDefaultCompression(const TCompression& compression) { DefaultCompression = compression; }
    const TCompression& GetDefaultCompression() const { return DefaultCompression; }

private:
    ui32 Id;
    TString Name;
    mutable std::shared_ptr<arrow::Schema> Schema;
    mutable std::shared_ptr<arrow::Schema> SchemaWithSpecials;
    std::shared_ptr<arrow::Schema> SortingKey;
    std::shared_ptr<arrow::Schema> ReplaceKey;
    std::shared_ptr<arrow::Schema> ExtendedKey; // Extend PK with snapshot columns to allow old shapshot reads
    std::shared_ptr<arrow::Schema> IndexKey;
    THashSet<TString> RequiredColumns;
    THashSet<ui32> MinMaxIdxColumnsIds;
    TCompression DefaultCompression;
};

std::shared_ptr<arrow::Schema> MakeArrowSchema(const NTable::TScheme::TTableSchema::TColumns& columns, const TVector<ui32>& ids, bool withSpecials = false);

/// Extracts columns with the specific ids from the schema.
TVector<TNameTypeInfo> GetColumns(const NTable::TScheme::TTableSchema& tableSchema, const TVector<ui32>& ids);

} // namespace NKikimr::NOlap
