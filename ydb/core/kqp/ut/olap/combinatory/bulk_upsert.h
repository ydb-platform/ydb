#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

class TBulkUpsertCommand: public ICommand {
private:
    TString TableName;
    TString ArrowBatch;
    Ydb::StatusIds_StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS;

public:
    TBulkUpsertCommand() = default;

    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override;

    bool DeserializeFromString(const TString& info);
};

}   // namespace NKikimr::NKqp
