#include "subsystem.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <queue>

namespace NActors {

    namespace {
        std::atomic_size_t Counter_ = 0;

        bool IsAlternativeAvailable(
                const TSubSystemDependencyAlternative& alternative,
                const std::vector<bool>& active) {
            for (const TSubSystemTypeId dependency : alternative.AllOf) {
                if (dependency >= active.size() || !active[dependency]) {
                    return false;
                }
            }
            return true;
        }

        std::optional<std::vector<size_t>> BuildTopologicalOrder(
                const std::vector<bool>& active,
                const std::vector<TSubSystemDependencies>& dependencies,
                const std::vector<std::optional<size_t>>& selectedAlternatives,
                size_t activeCount) {
            const size_t count = active.size();
            std::vector<size_t> incomingEdges(count, 0);
            std::vector<std::vector<size_t>> dependents(count);
            for (size_t index = 0; index < count; ++index) {
                if (!active[index] || dependencies[index].empty()) {
                    continue;
                }

                if (!selectedAlternatives[index]) {
                    return std::nullopt;
                }
                std::vector<size_t> directDependencies =
                    dependencies[index][*selectedAlternatives[index]].AllOf;
                std::sort(directDependencies.begin(), directDependencies.end());
                directDependencies.erase(
                    std::unique(directDependencies.begin(), directDependencies.end()),
                    directDependencies.end());

                incomingEdges[index] = directDependencies.size();
                for (const size_t dependency : directDependencies) {
                    dependents[dependency].push_back(index);
                }
            }

            std::priority_queue<size_t, std::vector<size_t>, std::greater<size_t>> ready;
            for (size_t index = 0; index < count; ++index) {
                if (active[index] && incomingEdges[index] == 0) {
                    ready.push(index);
                }
            }

            std::vector<size_t> result;
            result.reserve(activeCount);
            while (!ready.empty()) {
                const size_t index = ready.top();
                ready.pop();
                result.push_back(index);

                for (const size_t dependent : dependents[index]) {
                    if (--incomingEdges[dependent] == 0) {
                        ready.push(dependent);
                    }
                }
            }

            if (result.size() != activeCount) {
                return std::nullopt;
            }
            return result;
        }

        bool SelectDependencyAlternatives(
                size_t position,
                const std::vector<size_t>& activeIndices,
                const std::vector<bool>& active,
                const std::vector<TSubSystemDependencies>& dependencies,
                std::vector<std::optional<size_t>>& selectedAlternatives,
                std::optional<std::vector<size_t>>& order) {
            // Alternatives are tried in declaration order. A full combination
            // is accepted only when its dependency graph is acyclic.
            if (position == activeIndices.size()) {
                order = BuildTopologicalOrder(
                    active, dependencies, selectedAlternatives, activeIndices.size());
                return order.has_value();
            }

            const size_t index = activeIndices[position];
            if (dependencies[index].empty()) {
                return SelectDependencyAlternatives(
                    position + 1,
                    activeIndices,
                    active,
                    dependencies,
                    selectedAlternatives,
                    order);
            }

            for (size_t alternativeIndex = 0;
                    alternativeIndex < dependencies[index].size();
                    ++alternativeIndex) {
                if (!IsAlternativeAvailable(dependencies[index][alternativeIndex], active)) {
                    continue;
                }

                selectedAlternatives[index] = alternativeIndex;
                if (SelectDependencyAlternatives(
                        position + 1,
                        activeIndices,
                        active,
                        dependencies,
                        selectedAlternatives,
                        order)) {
                    return true;
                }
            }

            selectedAlternatives[index].reset();
            return false;
        }
    }

    size_t TSubSystemRegistry::NextIndex() noexcept {
        return Counter_.fetch_add(1, std::memory_order_relaxed);
    }

    std::optional<std::vector<size_t>> ResolveSubSystemDependencies(TSubSystems& subsystems) {
        const size_t count = subsystems.size();
        std::vector<bool> active(count, false);
        std::vector<TSubSystemDependencies> dependencies(count);

        for (size_t index = 0; index < count; ++index) {
            if (subsystems[index]) {
                active[index] = true;
                dependencies[index] = subsystems[index]->GetDependencies();
            }
        }

        bool changed = false;
        do {
            changed = false;
            for (size_t index = 0; index < count; ++index) {
                if (!active[index]) {
                    continue;
                }

                if (dependencies[index].empty()) {
                    continue;
                }

                const bool satisfied = std::any_of(
                    dependencies[index].begin(),
                    dependencies[index].end(),
                    [&active](const TSubSystemDependencyAlternative& alternative) {
                        return IsAlternativeAvailable(alternative, active);
                    });
                if (!satisfied) {
                    active[index] = false;
                    changed = true;
                }
            }
        } while (changed);

        std::vector<size_t> activeIndices;
        activeIndices.reserve(count);
        for (size_t index = 0; index < count; ++index) {
            if (active[index]) {
                activeIndices.push_back(index);
            }
        }

        std::vector<std::optional<size_t>> selectedAlternatives(count);
        std::optional<std::vector<size_t>> result;

        for (size_t index = 0; index < count; ++index) {
            if (!active[index]) {
                subsystems[index].reset();
            }
        }

        if (!SelectDependencyAlternatives(
                0,
                activeIndices,
                active,
                dependencies,
                selectedAlternatives,
                result)) {
            return std::nullopt;
        }

        for (const size_t index : *result) {
            TResolvedSubSystemDependencies resolved;
            if (selectedAlternatives[index]) {
                const auto& selected = dependencies[index][*selectedAlternatives[index]];
                resolved.reserve(selected.AllOf.size());
                for (const TSubSystemTypeId dependency : selected.AllOf) {
                    resolved.push_back({
                        .Type = dependency,
                        .Instance = subsystems[dependency].get(),
                    });
                }
            }
            subsystems[index]->OnDependenciesResolved(resolved);
        }
        return result;
    }

} // namespace NActors
