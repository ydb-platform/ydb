#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

class TSchemaCommand: public ICommand {
private:
    TString Command;
    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
        Cerr << "EXECUTE: " << Command << Endl;
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(Command).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoDeserializeProperties(const TPropertiesCollection& props) override {
        if (props.GetFreeArgumentsCount() == 0) {
            return TConclusionStatus::Fail("no free arguments count for SCHEMA command");
        }
        Command = props.JoinFreeArguments("\n");
        return TConclusionStatus::Success();
    }

public:
    TSchemaCommand() = default;
};

class TDataCommand: public ICommand {
private:
    TString Command;
    virtual TConclusionStatus DoExecute(TKikimrRunner& kikimr) override {
        Cerr << "EXECUTE: " << Command << Endl;
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto client = kikimr.GetQueryClient();
        auto prepareResult = client.ExecuteQuery(Command, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoDeserializeProperties(const TPropertiesCollection& props) override {
        if (props.GetFreeArgumentsCount() == 0) {
            return TConclusionStatus::Fail("no free arguments count for SCHEMA command");
        }
        Command = props.JoinFreeArguments("\n");
        return TConclusionStatus::Success();
    }

public:
    TDataCommand() = default;
};

}   // namespace NKikimr::NKqp
