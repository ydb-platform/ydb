#include "actualization.h"
#include "bulk_upsert.h"
#include "compaction.h"
#include "execute.h"
#include "select.h"
#include "variator.h"

namespace NKikimr::NKqp {

std::shared_ptr<ICommand> TScriptVariator::BuildCommand(TString command) {
    if (command.StartsWith("BULK_UPSERT:")) {
        command = command.substr(12);
        auto result = std::make_shared<TBulkUpsertCommand>();
        AFL_VERIFY(result->DeserializeFromString(command));
        return result;
    } else if (command.StartsWith("SCHEMA:")) {
        command = command.substr(7);
        return std::make_shared<TSchemaCommand>(command);
    } else if (command.StartsWith("DATA:")) {
        command = command.substr(5);
        return std::make_shared<TDataCommand>(command);
    } else if (command.StartsWith("READ:")) {
        auto result = std::make_shared<TSelectCommand>();
        AFL_VERIFY(result->DeserializeFromString(command));
        return result;
    } else if (command.StartsWith("WAIT_COMPACTION")) {
        return std::make_shared<TWaitCompactionCommand>();
    } else if (command.StartsWith("STOP_COMPACTION")) {
        return std::make_shared<TStopCompactionCommand>();
    } else if (command.StartsWith("ONE_COMPACTION")) {
        return std::make_shared<TOneCompactionCommand>();
    } else if (command.StartsWith("ONE_ACTUALIZATION")) {
        return std::make_shared<TOneActualizationCommand>();
    } else {
        AFL_VERIFY(false)("command", command);
        return nullptr;
    }
}

void TScriptVariator::BuildScripts(const std::vector<std::vector<std::shared_ptr<ICommand>>>& commands, const ui32 currentLayer,
    std::vector<std::shared_ptr<ICommand>>& currentScript, std::vector<TScriptExecutor>& scripts) {
    if (currentLayer == commands.size()) {
        scripts.emplace_back(currentScript);
        return;
    }
    for (auto&& i : commands[currentLayer]) {
        currentScript.emplace_back(i);
        BuildScripts(commands, currentLayer + 1, currentScript, scripts);
        currentScript.pop_back();
    }
}

void TScriptVariator::BuildVariantsImpl(const std::vector<std::vector<TString>>& chunks, const ui32 currentLayer,
    std::vector<TString>& currentCommand, std::vector<TString>& results) {
    if (currentLayer == chunks.size()) {
        results.emplace_back(JoinSeq("", currentCommand));
        return;
    }
    for (auto&& i : chunks[currentLayer]) {
        currentCommand.emplace_back(i);
        BuildVariantsImpl(chunks, currentLayer + 1, currentCommand, results);
        currentCommand.pop_back();
    }
}

std::vector<TString> TScriptVariator::BuildVariants(const TString& command) {
    auto chunks = StringSplitter(command).SplitByString("$$").ToList<TString>();
    std::vector<std::vector<TString>> chunksVariants;
    for (ui32 i = 0; i < chunks.size(); ++i) {
        if (i % 2 == 0) {
            chunksVariants.emplace_back(std::vector<TString>({ chunks[i] }));
        } else {
            chunksVariants.emplace_back(StringSplitter(chunks[i]).SplitBySet("|").ToList<TString>());
        }
    }
    std::vector<TString> result;
    std::vector<TString> currentCommand;
    BuildVariantsImpl(chunksVariants, 0, currentCommand, result);
    return result;
}

TScriptVariator::TScriptVariator(const TString& script) {
    auto lines = StringSplitter(script).SplitByString("\n").ToList<TString>();
    lines.erase(std::remove_if(lines.begin(), lines.end(),
                    [](const TString& l) {
                        return Strip(l).StartsWith("#");
                    }),
        lines.end());
    auto commands = StringSplitter(JoinSeq("\n", lines)).SplitByString("------").ToList<TString>();
    std::vector<std::vector<std::shared_ptr<ICommand>>> commandsDescription;
    for (auto&& i : commands) {
        auto& cVariants = commandsDescription.emplace_back();
        i = Strip(i);
        std::vector<TString> variants = BuildVariants(i);
        for (auto&& v : variants) {
            cVariants.emplace_back(BuildCommand(v));
        }
    }
    std::vector<TScriptExecutor> scripts;
    std::vector<std::shared_ptr<ICommand>> scriptCommands;
    BuildScripts(commandsDescription, 0, scriptCommands, Scripts);
}

}   // namespace NKikimr::NKqp
