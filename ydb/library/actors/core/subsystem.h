#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace NActors {
    class TActorSystem;
    class ISubSystem;

    using TSubSystemTypeId = size_t;

    // Internal disjunctive-normal-form representation. An alternative is
    // satisfied when every subsystem listed in AllOf is available.
    // Alternatives are combined with OR semantics and preserve declaration
    // order for deterministic dependency-plan selection.
    struct TSubSystemDependencyAlternative {
        std::vector<TSubSystemTypeId> AllOf;

        bool operator==(const TSubSystemDependencyAlternative& other) const {
            return AllOf == other.AllOf;
        }
    };

    using TSubSystemDependencies = std::vector<TSubSystemDependencyAlternative>;

    class TSubSystemDependencyExpression {
    public:
        explicit TSubSystemDependencyExpression(TSubSystemTypeId subsystem)
            : Alternatives{{{subsystem}}}
        {
        }

        operator TSubSystemDependencies() && {
            return std::move(Alternatives);
        }

        friend TSubSystemDependencyExpression operator&&(
                const TSubSystemDependencyExpression& left,
                const TSubSystemDependencyExpression& right) {
            TSubSystemDependencies dependencies;
            dependencies.reserve(left.Alternatives.size() * right.Alternatives.size());
            for (const auto& leftAlternative : left.Alternatives) {
                for (const auto& rightAlternative : right.Alternatives) {
                    TSubSystemDependencyAlternative alternative = leftAlternative;
                    for (const TSubSystemTypeId subsystem : rightAlternative.AllOf) {
                        if (std::find(
                                alternative.AllOf.begin(),
                                alternative.AllOf.end(),
                                subsystem) == alternative.AllOf.end()) {
                            alternative.AllOf.push_back(subsystem);
                        }
                    }
                    if (std::find(
                            dependencies.begin(),
                            dependencies.end(),
                            alternative) == dependencies.end()) {
                        dependencies.push_back(std::move(alternative));
                    }
                }
            }
            return TSubSystemDependencyExpression(std::move(dependencies));
        }

        friend TSubSystemDependencyExpression operator||(
                TSubSystemDependencyExpression left,
                TSubSystemDependencyExpression right) {
            left.Alternatives.reserve(
                left.Alternatives.size() + right.Alternatives.size());
            for (auto& alternative : right.Alternatives) {
                if (std::find(
                        left.Alternatives.begin(),
                        left.Alternatives.end(),
                        alternative) == left.Alternatives.end()) {
                    left.Alternatives.push_back(std::move(alternative));
                }
            }
            return left;
        }

    private:
        explicit TSubSystemDependencyExpression(TSubSystemDependencies alternatives)
            : Alternatives(std::move(alternatives))
        {
        }

    private:
        TSubSystemDependencies Alternatives;
    };

    struct TResolvedSubSystem {
        TSubSystemTypeId Type;
        ISubSystem* Instance;
    };

    // The concrete DNF alternative selected by dependency resolution.
    using TResolvedSubSystemDependencies = std::vector<TResolvedSubSystem>;

    class ISubSystem {
    public:
        virtual ~ISubSystem() = default;

        virtual TSubSystemDependencies GetDependencies() const {
            return {};
        }

        // Called once at actor-system start with the selected DNF alternative.
        // Dependencies preserve their declaration order.
        virtual void OnDependenciesResolved(const TResolvedSubSystemDependencies&) {}

        virtual void OnBeforeStart(TActorSystem&) {}
        virtual void OnAfterStart(TActorSystem&) {}
        virtual void OnBeforeStop(TActorSystem&) {}
        virtual void OnAfterStop(TActorSystem&) {}
    };

    class TSubSystemRegistry {
    private:
        static size_t NextIndex() noexcept;

    public:
        template<class T>
        struct TItem {
            static size_t Index() noexcept {
                static const size_t value = NextIndex();
                return value;
            }
        };
    };

    // Dependencies are declared as boolean expressions, for example:
    // DependsOn<A>() && (DependsOn<B>() || DependsOn<C>()).
    template<class T>
    TSubSystemDependencyExpression DependsOn() {
        static_assert(std::is_base_of_v<ISubSystem, T>,
            "a dependency must implement ISubSystem");
        return TSubSystemDependencyExpression(TSubSystemRegistry::TItem<T>::Index());
    }

    using TSubSystems = std::vector<std::unique_ptr<ISubSystem>>;

    // Removes subsystems whose DNF alternatives cannot be satisfied, including
    // transitively unavailable dependencies. Selects the first combination of
    // alternatives that produces a dependencies-first initialization order,
    // or returns nullopt when every available combination has a cycle.
    std::optional<std::vector<size_t>> ResolveSubSystemDependencies(TSubSystems& subsystems);

    template<class T>
    void RegisterSubSystem(TSubSystems& subsystems, std::unique_ptr<T>&& subsystem) {
        static_assert(std::is_base_of_v<ISubSystem, T>, "T must implement ISubSystem");
        const size_t index = TSubSystemRegistry::TItem<T>::Index();
        if (subsystems.size() <= index) {
            subsystems.resize(index + 1);
        }
        subsystems[index] = std::move(subsystem);
    }

    template<class T>
    T* GetSubSystem(TSubSystems& subsystems) {
        static_assert(std::is_base_of_v<ISubSystem, T>, "T must implement ISubSystem");
        const size_t index = TSubSystemRegistry::TItem<T>::Index();
        if (index >= subsystems.size()) {
            return nullptr;
        }
        return static_cast<T*>(subsystems[index].get());
    }

    template<class T>
    const T* GetSubSystem(const TSubSystems& subsystems) {
        static_assert(std::is_base_of_v<ISubSystem, T>, "T must implement ISubSystem");
        const size_t index = TSubSystemRegistry::TItem<T>::Index();
        if (index >= subsystems.size()) {
            return nullptr;
        }
        return static_cast<const T*>(subsystems[index].get());
    }

} // namespace NActors
