#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/conclusion/status.h>

#include <util/string/type.h>

namespace NKikimr::NKqp {

class TRestartTabletsCommand: public ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

public:
    TRestartTabletsCommand() {
    }
};

class TStopSchemasCleanupCommand: public ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

public:
    TStopSchemasCleanupCommand() {
    }
};

class TOneSchemasCleanupCommand: public ICommand {
private:
    std::optional<bool> Expected;

    virtual std::set<TString> DoGetCommandProperties() const override {
        return { "EXPECTED" };
    }
    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

    virtual TConclusionStatus DoDeserializeProperties(const TPropertiesCollection& props) override {
        if (props.GetFreeArgumentsCount() != 0) {
            return TConclusionStatus::Fail("no free arguments have to been in one schemas cleanup command");
        }
        if (auto expected = props.GetOptional("EXPECTED")) {
            Expected = IsTrue(*expected);
        }
        return TConclusionStatus::Success();
    }

public:
    TOneSchemasCleanupCommand() {
    }
};

class TFastPortionsCleanupCommand: public ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

public:
    TFastPortionsCleanupCommand() {
    }
};

class TStopCompactionCommand: public ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

public:
    TStopCompactionCommand() {
    }
};

class TOneCompactionCommand: public ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

public:
    TOneCompactionCommand() {
    }
};

class TWaitCompactionCommand: public ICommand {
private:
    virtual TConclusionStatus DoExecute(TKikimrRunner& /*kikimr*/) override;

public:
    TWaitCompactionCommand() {
    }
};

}   // namespace NKikimr::NKqp
