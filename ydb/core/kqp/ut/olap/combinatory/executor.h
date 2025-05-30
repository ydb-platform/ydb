#pragma once
#include "abstract.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

class TScriptExecutor {
private:
    std::vector<std::shared_ptr<ICommand>> Commands;

public:
    TScriptExecutor(const std::vector<std::shared_ptr<ICommand>>& commands)
        : Commands(commands) {
    }
    void Execute();
};

}   // namespace NKikimr::NKqp
