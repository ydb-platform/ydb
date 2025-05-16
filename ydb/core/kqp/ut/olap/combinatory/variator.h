#pragma once
#include "abstract.h"
#include "executor.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/conclusion/status.h>

namespace NKikimr::NKqp {

class TScriptVariator {
private:
    std::vector<TScriptExecutor> Scripts;
    std::shared_ptr<ICommand> BuildCommand(const TString command);
    void BuildScripts(const std::vector<std::vector<std::shared_ptr<ICommand>>>& commands, const ui32 currentLayer,
        std::vector<std::shared_ptr<ICommand>>& currentScript, std::vector<TScriptExecutor>& scripts);

    void BuildVariantsImpl(const std::vector<std::vector<TString>>& chunks, const ui32 currentLayer, std::vector<TString>& currentCommand,
        std::vector<TString>& results);
    std::vector<TString> BuildVariants(const TString& command);

public:
    TScriptVariator(const TString& script);

    void Execute() {
        for (auto&& i : Scripts) {
            i.Execute();
        }
    }
};

}   // namespace NKikimr::NKqp
