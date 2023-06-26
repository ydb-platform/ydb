#pragma once

#include "column_features.h"
#include "defs.h"
#include "scalars.h"
#include "tier_info.h"

#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/scheme/scheme_types_proto.h>

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
class TSnapshotColumnInfo;
using TNameTypeInfo = std::pair<TString, NScheme::TTypeInfo>;

/// Column engine index description in terms of tablet's local table.
/// We have to use YDB types for keys here.
struct TIndexInfo : public NTable::TScheme::TTableSchema {
private:
    mutable THashMap<ui32, TColumnFeatures> ColumnFeatures;
    mutable THashMap<ui32, std::shared_ptr<arrow::Field>> ArrowColumnByColumnIdCache;
    TIndexInfo(const TString& name, ui32 id);
    bool DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema);
    TColumnFeatures& GetOrCreateColumnFeatures(const ui32 columnId) const;
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
        const TSnapshot& snapshot);

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

    bool CheckAlterScheme(const NKikimrSchemeOp::TColumnTableSchema& scheme) const;
public:

    static TIndexInfo BuildDefault() {
        TIndexInfo result("dummy", 0);
        return result;
    }

    static std::optional<TIndexInfo> BuildFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema);

    /// Returns id of the index.
    ui32 GetId() const noexcept {
        return Id;
    }

    std::shared_ptr<arrow::Schema> GetColumnSchema(const ui32 columnId) const;
    TColumnSaver GetColumnSaver(const ui32 columnId, const TSaverContext& context) const;
    std::shared_ptr<TColumnLoader> GetColumnLoader(const ui32 columnId) const;

    /// Returns an id of the column located by name. The name should exists in the schema.
    ui32 GetColumnId(const std::string& name) const;
    std::optional<ui32> GetColumnIdOptional(const std::string& name) const;

    /// Returns a name of the column located by id.
    TString GetColumnName(ui32 id, bool required = true) const;

    /// Returns names of columns defined by the specific ids.
    std::vector<TString> GetColumnNames(const std::vector<ui32>& ids) const;
    std::vector<ui32> GetColumnIds() const;

    /// Returns info of columns defined by specific ids.
    std::vector<TNameTypeInfo> GetColumns(const std::vector<ui32>& ids) const;

    /// Traditional Primary Key (includes uniqueness, search and sorting logic)
    std::vector<TNameTypeInfo> GetPrimaryKey() const {
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

    /// Initializes sorting, replace, index and extended keys.
    void SetAllKeys();

    void CheckTtlColumn(const TString& ttlColumn) const {
        Y_VERIFY(!ttlColumn.empty());
        Y_VERIFY(MinMaxIdxColumnsIds.contains(GetColumnId(ttlColumn)));
    }

    std::vector<ui32> GetColumnIds(const std::vector<TString>& columnNames) const;

    std::shared_ptr<arrow::Schema> ArrowSchema() const;
    std::shared_ptr<arrow::Schema> ArrowSchemaWithSpecials() const;
    std::shared_ptr<arrow::Schema> AddColumns(const std::shared_ptr<arrow::Schema>& schema,
                                              const std::vector<TString>& columns) const;

    std::shared_ptr<arrow::Schema> ArrowSchema(const std::vector<ui32>& columnIds, bool withSpecials = false) const;
    std::shared_ptr<arrow::Schema> ArrowSchema(const std::vector<TString>& columnNames) const;
    std::shared_ptr<arrow::Field> ArrowColumnField(ui32 columnId) const;

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

    bool IsCompositeIndexKey() const {
        return CompositeIndexKey;
    }

    std::shared_ptr<NArrow::TSortDescription> SortDescription() const;
    std::shared_ptr<NArrow::TSortDescription> SortReplaceDescription() const;

    static const std::vector<std::string>& GetSpecialColumnNames() {
        static const std::vector<std::string> result = { std::string(SPEC_COL_PLAN_STEP), std::string(SPEC_COL_TX_ID) };
        return result;
    }

    static const std::vector<ui32>& GetSpecialColumnIds() {
        static const std::vector<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID };
        return result;
    }

private:
    ui32 Id;
    TString Name;
    bool CompositeIndexKey = false;
    mutable std::shared_ptr<arrow::Schema> Schema;
    mutable std::shared_ptr<arrow::Schema> SchemaWithSpecials;
    std::shared_ptr<arrow::Schema> SortingKey;
    std::shared_ptr<arrow::Schema> ReplaceKey;
    std::shared_ptr<arrow::Schema> ExtendedKey; // Extend PK with snapshot columns to allow old shapshot reads
    std::shared_ptr<arrow::Schema> IndexKey;
    THashSet<TString> RequiredColumns;
    THashSet<ui32> MinMaxIdxColumnsIds;
    std::optional<NArrow::TCompression> DefaultCompression;
    bool CompositeMarks = false;
};

std::shared_ptr<arrow::Schema> MakeArrowSchema(const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, bool withSpecials = false);

/// Extracts columns with the specific ids from the schema.
std::vector<TNameTypeInfo> GetColumns(const NTable::TScheme::TTableSchema& tableSchema, const std::vector<ui32>& ids);

} // namespace NKikimr::NOlap
