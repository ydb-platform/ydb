#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

class TCheckCounterCommand : public ICommand {
private:
    TString Path;   // service/subgroup_key1/subgroup_value1/... e.g. tablets/subsystem/columnshard/module_id/Scan
    TString CounterName;  // e.g. Deriviative/Dictionary/OnlyOptimization/Count
    i64 ExpectedValue = 0;
    bool AtLeast = false;  // if set, assert actual >= ExpectedValue instead of ==

    TConclusionStatus DoExecute(TKikimrRunner& kikimr) override;
    std::set<TString> DoGetCommandProperties() const override {
        return {"PATH", "EXPECTED", "AT_LEAST"};
    }
    TConclusionStatus DoDeserializeProperties(const TPropertiesCollection& props) override;

public:
    TCheckCounterCommand() = default;
};

}   // namespace NKikimr::NKqp
