#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NKqp {

class TBulkUpsertCommand: public ICommand {
private:
    TString TableName;
    std::shared_ptr<arrow::RecordBatch> ArrowBatch;
    Ydb::StatusIds_StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS;
    ui32 PartsCount = 1;

    virtual std::set<TString> DoGetCommandProperties() const override {
        return { "EXPECT_STATUS", "PARTS_COUNT" };
    }
    virtual TConclusionStatus DoDeserializeProperties(const TPropertiesCollection& props) override;

public:
    TBulkUpsertCommand() = default;

    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override;
};

}   // namespace NKikimr::NKqp
