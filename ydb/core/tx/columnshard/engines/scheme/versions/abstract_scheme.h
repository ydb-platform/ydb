#pragma once
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/core/formats/arrow/save_load/saver.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/tx/data_events/common/modification_type.h>

#include <ydb/core/tx/columnshard/common/snapshot.h>

#include <string>

namespace NKikimr::NOlap {

struct TIndexInfo;
class TSaverContext;

class ISnapshotSchema {
protected:
    virtual TString DoDebugString() const = 0;
public:
    using TPtr = std::shared_ptr<ISnapshotSchema>;

    virtual ~ISnapshotSchema() {}
    virtual std::shared_ptr<NArrow::NAccessor::TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const = 0;
    std::shared_ptr<NArrow::NAccessor::TColumnLoader> GetColumnLoaderVerified(const ui32 columnId) const;
    std::shared_ptr<NArrow::NAccessor::TColumnLoader> GetColumnLoaderOptional(const std::string& columnName) const;
    std::shared_ptr<NArrow::NAccessor::TColumnLoader> GetColumnLoaderVerified(const std::string& columnName) const;

    bool IsSpecialColumnId(const ui32 columnId) const;
    virtual const std::vector<ui32>& GetColumnIds() const = 0;

    virtual NArrow::NAccessor::TColumnSaver GetColumnSaver(const ui32 columnId) const = 0;
    NArrow::NAccessor::TColumnSaver GetColumnSaver(const TString& columnName) const {
        return GetColumnSaver(GetColumnId(columnName));
    }
    NArrow::NAccessor::TColumnSaver GetColumnSaver(const std::string& columnName) const {
        return GetColumnSaver(TString(columnName.data(), columnName.size()));
    }

    std::vector<std::shared_ptr<arrow::Field>> GetAbsentFields(const std::shared_ptr<arrow::Schema>& existsSchema) const;

    std::shared_ptr<arrow::Scalar> GetExternalDefaultValueVerified(const std::string& columnName) const;
    std::shared_ptr<arrow::Scalar> GetExternalDefaultValueVerified(const ui32 columnId) const;

    TConclusion<std::shared_ptr<arrow::RecordBatch>> BuildDefaultBatch(
        const std::vector<std::shared_ptr<arrow::Field>>& fields, const ui32 rowsCount, const bool force) const;
    TConclusionStatus CheckColumnsDefault(const std::vector<std::shared_ptr<arrow::Field>>& fields) const;

    std::vector<std::string> GetPKColumnNames() const;

    virtual std::optional<ui32> GetColumnIdOptional(const std::string& columnName) const = 0;
    virtual ui32 GetColumnIdVerified(const std::string& columnName) const = 0;
    virtual int GetFieldIndex(const ui32 columnId) const = 0;
    bool HasColumnId(const ui32 columnId) const {
        return GetFieldIndex(columnId) >= 0;
    }

    ui32 GetColumnId(const std::string& columnName) const;
    std::shared_ptr<arrow::Field> GetFieldByIndex(const int index) const;
    std::shared_ptr<arrow::Field> GetFieldByColumnIdOptional(const ui32 columnId) const;
    std::shared_ptr<arrow::Field> GetFieldByColumnIdVerified(const ui32 columnId) const;

    TString DebugString() const {
        return DoDebugString();
    }
    virtual const std::shared_ptr<NArrow::TSchemaLite>& GetSchema() const = 0;
    virtual const TIndexInfo& GetIndexInfo() const = 0;
    virtual const TSnapshot& GetSnapshot() const = 0;
    virtual ui64 GetVersion() const = 0;
    virtual ui32 GetColumnsCount() const = 0;

    std::set<ui32> GetPkColumnsIds() const;

    static std::set<ui32> GetColumnsWithDifferentDefaults(const THashMap<ui64, ISnapshotSchema::TPtr>& schemas, const ISnapshotSchema::TPtr& targetSchema);

    [[nodiscard]] TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> NormalizeBatch(
        const ISnapshotSchema& dataSchema, const std::shared_ptr<NArrow::TGeneralContainer>& batch, const std::set<ui32>& restoreColumnIds) const;
    [[nodiscard]] TConclusion<std::shared_ptr<arrow::RecordBatch>> PrepareForModification(
        const std::shared_ptr<arrow::RecordBatch>& incomingBatch, const NEvWrite::EModificationType mType) const;
    void AdaptBatchToSchema(NArrow::TGeneralContainer& batch, const ISnapshotSchema::TPtr& targetSchema) const;
};

} // namespace NKikimr::NOlap
