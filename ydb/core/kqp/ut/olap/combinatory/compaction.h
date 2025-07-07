#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

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
