#pragma once

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/save_load/saver.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {

using TColumnLoader = NArrow::NAccessor::TColumnLoader;
using TColumnSaver = NArrow::NAccessor::TColumnSaver;

class IIndexInfo {
public:
    enum class ESpecialColumn : ui32 {
        PLAN_STEP = NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP_INDEX,
        TX_ID = NOlap::NPortion::TSpecialColumns::SPEC_COL_TX_ID_INDEX,
        WRITE_ID = NOlap::NPortion::TSpecialColumns::SPEC_COL_WRITE_ID_INDEX,
        DELETE_FLAG = NOlap::NPortion::TSpecialColumns::SPEC_COL_DELETE_FLAG_INDEX,
    };

    using TSystemColumnsSet = ui64;

    enum class ESystemColumnsSet : ui64 {
        Snapshot = 1,
        Deletion = 1 << 1,
    };

    static constexpr const char* SPEC_COL_PLAN_STEP = NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP;
    static constexpr const char* SPEC_COL_TX_ID = NOlap::NPortion::TSpecialColumns::SPEC_COL_TX_ID;
    static constexpr const char* SPEC_COL_WRITE_ID = NOlap::NPortion::TSpecialColumns::SPEC_COL_WRITE_ID;
    static constexpr const char* SPEC_COL_DELETE_FLAG = NOlap::NPortion::TSpecialColumns::SPEC_COL_DELETE_FLAG;

    static const inline std::shared_ptr<arrow::Field> PlanStepField = arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64());
    static const inline std::shared_ptr<arrow::Field> TxIdField = arrow::field(SPEC_COL_TX_ID, arrow::uint64());
    static const inline std::shared_ptr<arrow::Field> WriteIdField = arrow::field(SPEC_COL_WRITE_ID, arrow::uint64());

    static const char* GetDeleteFlagColumnName() {
        return SPEC_COL_DELETE_FLAG;
    }

    static const std::set<ui32>& GetNecessarySystemColumnIdsSet() {
        static const std::set<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID, (ui32)ESpecialColumn::WRITE_ID };
        return result;
    }

    static const std::vector<std::string>& GetSnapshotColumnNames() {
        static const std::vector<std::string> result = { std::string(SPEC_COL_PLAN_STEP), std::string(SPEC_COL_TX_ID),
            std::string(SPEC_COL_WRITE_ID) };
        return result;
    }

    static const std::vector<ui32>& GetSnapshotColumnIds() {
        static const std::vector<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID, (ui32)ESpecialColumn::WRITE_ID };
        return result;
    }

    static ui32 CalcDeletions(const std::shared_ptr<arrow::RecordBatch>& batch, const bool needExistsColumn);

    std::shared_ptr<arrow::Schema> BuildSpecialFieldsSchema() const {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        AddSpecialFields(fields);
        return std::make_shared<arrow::Schema>(std::move(fields));
    }

    static std::shared_ptr<arrow::Scalar> DefaultColumnValue(const ui32 colId);

    static std::shared_ptr<arrow::Schema> AddSpecialFields(const std::shared_ptr<arrow::Schema>& schema) {
        std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
        AddSpecialFields(fields);
        return std::make_shared<arrow::Schema>(std::move(fields));
    }

    static std::shared_ptr<arrow::Schema> AddSnapshotFields(const std::shared_ptr<arrow::Schema>& schema) {
        std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
        AddSnapshotFields(fields);
        return std::make_shared<arrow::Schema>(std::move(fields));
    }

    static void AddSpecialFields(std::vector<std::shared_ptr<arrow::Field>>& fields) {
        AddSnapshotFields(fields);
        static const std::shared_ptr<arrow::Field> f = arrow::field(SPEC_COL_DELETE_FLAG, arrow::boolean());
        fields.push_back(f);
    }

    static const std::vector<std::string>& SnapshotColumnNames() {
        static std::vector<std::string> result = { SPEC_COL_PLAN_STEP, SPEC_COL_TX_ID };
        return result;
    }

    static void AddSnapshotFields(std::vector<std::shared_ptr<arrow::Field>>& fields) {
        static const std::shared_ptr<arrow::Field> ps = arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64());
        static const std::shared_ptr<arrow::Field> txid = arrow::field(SPEC_COL_TX_ID, arrow::uint64());
        static const std::shared_ptr<arrow::Field> writeId = arrow::field(SPEC_COL_WRITE_ID, arrow::uint64());
        fields.push_back(ps);
        fields.push_back(txid);
        fields.push_back(writeId);
    }

    static void AddDeleteFields(std::vector<std::shared_ptr<arrow::Field>>& fields) {
        fields.push_back(arrow::field(SPEC_COL_DELETE_FLAG, arrow::boolean()));
    }

    static const std::set<ui32>& GetSnapshotColumnIdsSet() {
        static const std::set<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID, (ui32)ESpecialColumn::WRITE_ID };
        return result;
    }

    static const std::vector<std::string>& GetSystemColumnNames() {
        static const std::vector<std::string> result = { std::string(SPEC_COL_PLAN_STEP), std::string(SPEC_COL_TX_ID),
            std::string(SPEC_COL_WRITE_ID), std::string(SPEC_COL_DELETE_FLAG) };
        return result;
    }

    static const std::vector<ui32>& GetSystemColumnIds() {
        static const std::vector<ui32> result = { (ui32)ESpecialColumn::PLAN_STEP, (ui32)ESpecialColumn::TX_ID, (ui32)ESpecialColumn::WRITE_ID,
            (ui32)ESpecialColumn::DELETE_FLAG };
        return result;
    }

    [[nodiscard]] static std::vector<ui32> AddSpecialFieldIds(const std::vector<ui32>& baseColumnIds) {
        std::vector<ui32> result = baseColumnIds;
        const auto& cIds = GetSystemColumnIds();
        result.insert(result.end(), cIds.begin(), cIds.end());
        return result;
    }

    [[nodiscard]] static std::set<ui32> AddSpecialFieldIds(const std::set<ui32>& baseColumnIds) {
        std::set<ui32> result = baseColumnIds;
        const auto& cIds = GetSystemColumnIds();
        result.insert(cIds.begin(), cIds.end());
        return result;
    }

    [[nodiscard]] static std::vector<ui32> AddSnapshotFieldIds(const std::vector<ui32>& baseColumnIds) {
        std::vector<ui32> result = baseColumnIds;
        for (auto&& i : GetSnapshotColumnIds()) {
            result.emplace_back(i);
        }
        return result;
    }

    std::optional<ui32> GetColumnIdOptional(const std::string& name) const;
    std::optional<ui32> GetColumnIndexOptional(const std::string& name, const ui32 shift) const;
    TString GetColumnName(const ui32 id, const bool required) const;
    static std::shared_ptr<arrow::Field> GetColumnFieldOptional(const ui32 columnId);
    static std::shared_ptr<arrow::Field> GetColumnFieldVerified(const ui32 columnId);

    virtual std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const = 0;
    std::shared_ptr<TColumnLoader> GetColumnLoaderVerified(const ui32 columnId) const;

    static void NormalizeDeletionColumn(NArrow::TGeneralContainer& batch);

    static void AddSnapshotColumns(NArrow::TGeneralContainer& batch, const TSnapshot& snapshot, const ui64 insertWriteId);
    static void AddDeleteFlagsColumn(NArrow::TGeneralContainer& batch, const bool isDelete);

    static ui64 GetSpecialColumnsRecordSize() {
        return sizeof(ui64) + sizeof(ui64) + sizeof(bool);
    }

    static std::shared_ptr<arrow::Schema> ArrowSchemaSnapshot() {
        static std::shared_ptr<arrow::Schema> result = std::make_shared<arrow::Schema>(arrow::FieldVector{ arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()),
                arrow::field(SPEC_COL_TX_ID, arrow::uint64()), arrow::field(SPEC_COL_WRITE_ID, arrow::uint64()) });
        return result;
    }

    static std::shared_ptr<arrow::Schema> ArrowSchemaDeletion() {
        static std::shared_ptr<arrow::Schema> result =
            std::make_shared<arrow::Schema>(arrow::FieldVector{ arrow::field(SPEC_COL_DELETE_FLAG, arrow::boolean()) });
        return result;
    }

    static bool IsSpecialColumn(const arrow::Field& field) {
        return IsSpecialColumn(field.name());
    }

    static bool IsSpecialColumn(const std::string& fieldName) {
        return fieldName == SPEC_COL_PLAN_STEP || fieldName == SPEC_COL_TX_ID || fieldName == SPEC_COL_WRITE_ID ||
               fieldName == SPEC_COL_DELETE_FLAG;
    }

    static bool IsSpecialColumn(const ui32 fieldId) {
        return fieldId == (ui32)ESpecialColumn::PLAN_STEP || fieldId == (ui32)ESpecialColumn::TX_ID ||
               fieldId == (ui32)ESpecialColumn::WRITE_ID || fieldId == (ui32)ESpecialColumn::DELETE_FLAG;
    }

    static bool IsNullableVerified(const ui32 /*fieldId*/) {
        return false;
    }

    static ui32 GetSpecialColumnByteWidth(const ui32 field) {
        Y_ABORT_UNLESS(IsSpecialColumn(field));
        return 8;
    }

    template <class TContainer>
    static bool IsSpecialColumns(const TContainer& c) {
        for (auto&& i : c) {
            if (!IsSpecialColumn(i)) {
                return false;
            }
        }
        return true;
    }

    virtual ~IIndexInfo() = default;
};

}   // namespace NKikimr::NOlap
