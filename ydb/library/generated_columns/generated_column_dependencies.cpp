#include "generated_column_dependencies.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NGeneratedColumns {

bool BuildGeneratedColumnDependencyGraph(const TVector<TString>& columnNames,
    const TMap<TString, TVector<TString>>& directDependencies,
    TGeneratedColumnDependencyGraph& result, TString& error)
{
    result = {};
    error.clear();
    TGeneratedColumnDependencyGraph graph;
    const THashSet<TString> columns(columnNames.begin(), columnNames.end());
    graph.Dependencies = directDependencies;
    THashMap<TString, const TVector<TString>*> generatedNodes;
    for (auto& [name, dependencies] : graph.Dependencies) {
        if (!columns.contains(name)) {
            error = TStringBuilder() << "Unknown generated column: " << name;
            return false;
        }
        std::sort(dependencies.begin(), dependencies.end());
        dependencies.erase(std::unique(dependencies.begin(), dependencies.end()), dependencies.end());
        generatedNodes.emplace(name, &dependencies);
        for (const auto& dependency : dependencies) {
            if (!columns.contains(dependency)) {
                error = TStringBuilder() << "Generated column " << name << " references an unknown column: " << dependency;
                return false;
            }
            if (dependency == name) {
                error = TStringBuilder() << "Generated column " << name << " can not reference itself";
                return false;
            }
            graph.Dependents[dependency].push_back(name);
        }
    }

    // A frame's next edge is advanced before descending, so every edge is visited once.
    struct TFrame {
        const TString* Name;
        const TVector<TString>* Dependencies;
        size_t Next = 0;
    };
    enum class EColor { White, Gray, Black };
    THashMap<TString, EColor> colors;
    THashMap<TString, size_t> activePositions;
    TVector<TFrame> stack;
    for (const auto& [name, dependencies] : graph.Dependencies) {
        if (colors[name] == EColor::Black) {
            continue;
        }
        colors[name] = EColor::Gray;
        activePositions[name] = 0;
        stack.push_back({&name, &dependencies});
        while (!stack.empty()) {
            auto& frame = stack.back();
            if (frame.Next == frame.Dependencies->size()) {
                colors[*frame.Name] = EColor::Black;
                graph.EvaluationOrder.push_back(*frame.Name);
                activePositions.erase(*frame.Name);
                stack.pop_back();
                continue;
            }
            const auto& dependency = (*frame.Dependencies)[frame.Next++];
            const auto* child = generatedNodes.FindPtr(dependency);
            if (!child || colors[dependency] == EColor::Black) {
                continue;
            }
            if (colors[dependency] == EColor::Gray) {
                TStringBuilder cycle;
                cycle << "Generated column dependency cycle: ";
                for (size_t i = activePositions.at(dependency); i < stack.size(); ++i) {
                    cycle << *stack[i].Name << " -> ";
                }
                cycle << dependency;
                error = cycle;
                return false;
            }
            colors[dependency] = EColor::Gray;
            activePositions[dependency] = stack.size();
            stack.push_back({&dependency, *child});
        }
    }
    result = std::move(graph);
    return true;
}

THashSet<TString> CollectAffectedGeneratedColumns(const TGeneratedColumnDependencyGraph& graph,
    const THashSet<TString>& changedColumns)
{
    THashSet<TString> affected;
    TVector<TString> pending(changedColumns.begin(), changedColumns.end());
    while (!pending.empty()) {
        const auto column = std::move(pending.back());
        pending.pop_back();
        if (const auto* dependents = graph.Dependents.FindPtr(column)) {
            for (const auto& dependent : *dependents) {
                if (affected.insert(dependent).second) {
                    pending.push_back(dependent);
                }
            }
        }
    }
    return affected;
}

} // namespace NKikimr::NGeneratedColumns
