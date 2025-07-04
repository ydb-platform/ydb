#include "actualization.h"
#include "bulk_upsert.h"
#include "compaction.h"
#include "execute.h"
#include "select.h"
#include "variator.h"

namespace NKikimr::NKqp::Variator {
namespace {
    using TCommandFactory = std::function<std::shared_ptr<ICommand>()>;

    const std::unordered_map<TString, TCommandFactory> CommandFactories = {
        {"BULK_UPSERT", []() { return std::make_shared<TBulkUpsertCommand>(); }},
        {"SCHEMA", []() { return std::make_shared<TSchemaCommand>(); }},
        {"DATA", []() { return std::make_shared<TDataCommand>(); }},
        {"READ", []() { return std::make_shared<TSelectCommand>(); }},
        {"WAIT_COMPACTION", []() { return std::make_shared<TWaitCompactionCommand>(); }},
        {"STOP_COMPACTION", []() { return std::make_shared<TStopCompactionCommand>(); }},
        {"STOP_SCHEMAS_CLEANUP", []() { return std::make_shared<TStopSchemasCleanupCommand>(); }},
        {"ONE_SCHEMAS_CLEANUP", []() { return std::make_shared<TOneSchemasCleanupCommand>(); }},
        {"FAST_PORTIONS_CLEANUP", []() { return std::make_shared<TFastPortionsCleanupCommand>(); }},
        {"ONE_COMPACTION", []() { return std::make_shared<TOneCompactionCommand>(); }},
        {"ONE_ACTUALIZATION", []() { return std::make_shared<TOneActualizationCommand>(); }},
        {"RESTART_TABLETS", []() { return std::make_shared<TRestartTabletsCommand>(); }}
    };

    std::pair<TString, TString> ParseCommandString(const TString& command) {
        TString cleanCommand = Strip(command);

        size_t pos = cleanCommand.find(':');
        if (pos == TString::npos) {
            return {cleanCommand, TString()};
        }

        return {Strip(cleanCommand.substr(0, pos)), Strip(cleanCommand.substr(pos + 1))};
    }
}

std::shared_ptr<ICommand> BuildCommand(const TString& command) {
    const auto [commandName, arguments] = ParseCommandString(command);

    auto it = CommandFactories.find(commandName);
    if (it == CommandFactories.end()) {
        AFL_VERIFY(false)("Unknown command", command)("commandName", commandName);
        return nullptr;
    }

    auto result = it->second();

    if (!arguments.empty()) {
        result->DeserializeFromString(arguments).Validate();
    }

    return result;
}

void BuildScripts(
    const std::vector<std::vector<std::tuple<TString, TString>>>& commands,
    const ui32 currentLayer,
    Script& currentScript,
    std::vector<TString>& currentVariant,
    std::vector<std::tuple<Script, Varinat>>& scripts
) {
    if (currentLayer == commands.size()) {
        scripts.emplace_back(currentScript, JoinSeq(",", currentVariant));
        return;
    }

    for (const auto& [command, variant] : commands[currentLayer]) {
        if (!variant.empty()) {
            currentVariant.push_back(variant);
        }

        currentScript.push_back(command);
        BuildScripts(commands, currentLayer + 1, currentScript, currentVariant, scripts);
        currentScript.pop_back();

        if (!variant.empty()) {
            currentVariant.pop_back();
        }
    }
}

void BuildVariantsImpl(
    const std::vector<std::vector<TString>>& chunks,
    const ui32 currentLayer,
    std::vector<TString>& currentCommand,
    std::vector<TString>& currentVariant,
    std::vector<std::tuple<TString, TString>>& results
) {
    if (currentLayer == chunks.size()) {
        results.emplace_back(JoinSeq("", currentCommand), JoinSeq(",", currentVariant));
        return;
    }

    const auto& chunkVariants = chunks[currentLayer];
    const bool hasMultipleVariants = chunkVariants.size() > 1;

    for (const auto& chunk : chunkVariants) {
        if (hasMultipleVariants) {
            currentVariant.push_back(chunk);
        }

        currentCommand.push_back(chunk);
        BuildVariantsImpl(chunks, currentLayer + 1, currentCommand, currentVariant, results);
        currentCommand.pop_back();

        if (hasMultipleVariants) {
            currentVariant.pop_back();
        }
    }
}

std::vector<std::tuple<TString, TString>> BuildVariants(const TString& command) {
    auto chunks = StringSplitter(command).SplitByString("$$").ToList<TString>();
    std::vector<std::vector<TString>> chunksVariants;
    chunksVariants.reserve(chunks.size());

    for (ui32 i = 0; i < chunks.size(); ++i) {
        if (i % 2 == 0) {
            chunksVariants.push_back({chunks[i]});
        } else {
            chunksVariants.push_back(
                StringSplitter(chunks[i]).SplitBySet("|").ToList<TString>()
            );
        }
    }

    std::vector<std::tuple<TString, TString>> result;
    std::vector<TString> currentCommand;
    std::vector<TString> currentVariant;

    BuildVariantsImpl(chunksVariants, 0, currentCommand, currentVariant, result);
    return result;
}

std::vector<std::tuple<Script, Varinat>> ScriptVariants(const TString& script) {
    auto commands = SingleScript(script);

    std::vector<std::vector<std::tuple<TString, TString>>> commandsDescription;
    commandsDescription.reserve(commands.size());

    for (auto& cmd : commands) {
        cmd = Strip(cmd);
        commandsDescription.push_back(BuildVariants(cmd));
    }

    std::vector<std::tuple<Script, Varinat>> result;
    Script currentScript;
    std::vector<TString> currentVariant;

    BuildScripts(commandsDescription, 0, currentScript, currentVariant, result);
    return result;
}

Script SingleScript(const TString& script) {
    auto lines = StringSplitter(script).SplitByString("\n").ToList<TString>();

    lines.erase(
        std::remove_if(lines.begin(), lines.end(),
            [](const TString& line) {
                return Strip(line).StartsWith("#");
            }),
        lines.end()
    );

    return StringSplitter(JoinSeq("\n", lines)).SplitByString("------").ToList<TString>();
}

TScriptExecutor ToExecutor(const Script& script) {
    std::vector<std::shared_ptr<ICommand>> result;
    result.reserve(script.size());

    for (const auto& command : script) {
        result.push_back(BuildCommand(command));
    }

    return TScriptExecutor(result);
}

TString ToString(const Script& script) {
    return JoinSeq("\n------\n", script);
}

}  // namespace NKikimr::NKqp::Variator
