#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

class TSelectCommand: public ICommand {
private:
    TString Command;
    TString Compare;
    std::optional<ui64> ExpectIndexSkip;
    std::optional<ui64> ExpectIndexNoData;
    std::optional<ui64> ExpectIndexApprove;

    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override;

public:
    bool DeserializeFromString(const TString& info);

    TSelectCommand() = default;
};

}   // namespace NKikimr::NKqp
