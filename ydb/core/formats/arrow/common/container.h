#pragma once
#include "accessor.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

#include <util/system/types.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow {

class TGeneralContainer {
private:
    YDB_READONLY(ui64, RecordsCount, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);
    std::vector<std::shared_ptr<NAccessor::IChunkedArray>> Columns;
public:
    TString DebugString() const {
        return TStringBuilder()
            << "records_count=" << RecordsCount << ";"
            << "schema=" << Schema->ToString() << ";"
            ;
    }

    ui64 num_rows() const {
        return RecordsCount;
    }

    std::shared_ptr<arrow::Table> BuildTable(const std::optional<std::set<std::string>>& columnNames = {}) const;

    std::shared_ptr<TGeneralContainer> BuildEmptySame() const;

    [[nodiscard]] TConclusionStatus MergeColumnsStrictly(const TGeneralContainer& container);
    [[nodiscard]] TConclusionStatus AddField(const std::shared_ptr<arrow::Field>& f, const std::shared_ptr<NAccessor::IChunkedArray>& data);

    TGeneralContainer(const std::shared_ptr<arrow::Table>& table);

    TGeneralContainer(const std::shared_ptr<arrow::RecordBatch>& table);

    TGeneralContainer(const std::shared_ptr<arrow::Schema>& schema, std::vector<std::shared_ptr<NAccessor::IChunkedArray>>&& columns);

    arrow::Status ValidateFull() const {
        return arrow::Status::OK();
    }

    std::shared_ptr<NAccessor::IChunkedArray> GetAccessorByNameOptional(const std::string& fieldId) const {
        for (i32 i = 0; i < Schema->num_fields(); ++i) {
            if (Schema->field(i)->name() == fieldId) {
                return Columns[i];
            }
        }
        return nullptr;
    }

    std::shared_ptr<NAccessor::IChunkedArray> GetAccessorByNameVerified(const std::string& fieldId) const;
};

}
