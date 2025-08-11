#pragma once

#include "../fwd.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>

#include <optional>

template <typename Base, typename Derived>
concept CStaticallyDowncastable = requires(Base* b) {
    static_cast<Derived*>(b);
};

namespace std {

    // TODO: remove after switch to C++26
    template <typename Base, typename Derived>
    inline constexpr bool is_virtual_base_of_v =
        std::is_base_of_v<Base, Derived> && !CStaticallyDowncastable<Base, Derived>;

} // namespace std

namespace NKikimr::NKqp::NScheduler::NHdrf {

    static constexpr ui64 Infinity() {
        return std::numeric_limits<ui64>::max();
    }

    struct TStaticAttributes {
        std::optional<ui64> Limit;
        std::optional<ui64> Guarantee;
        std::optional<double> Weight;

        inline auto GetLimit() const {
            return Limit.value_or(Infinity());
        }

        inline auto GetGuarantee() const {
            return Guarantee.value_or(0);
        }

        inline auto GetWeight() const {
            return Weight.value_or(1);
        }

        inline void Update(const TStaticAttributes& other) {
            if (other.Limit) {
                Limit = other.Limit;
            }
            if (other.Guarantee) {
                Guarantee = other.Guarantee;
            }
            if (other.Weight) {
                Weight = other.Weight;
            }
        }
    };

    enum class ETreeType : ui8 {
        DYNAMIC,
        SNAPSHOT,
    };

    template <ETreeType>
    struct TTreeElementBase : public TStaticAttributes {
        using TPtr = std::shared_ptr<TTreeElementBase>;

        explicit TTreeElementBase(const TId& id, const TStaticAttributes& attrs = {}) : Id(id) { Update(attrs); }
        virtual ~TTreeElementBase() = default;

        inline const TId& GetId() const {
            return Id;
        }

        void AddChild(const TPtr& element) {
            Y_ENSURE(Y_LIKELY(element));
            Children.push_back(element);
            element->Parent = this;
        }

        void RemoveChild(const TPtr& element) {
            Y_ENSURE(Y_LIKELY(element));
            for (auto it = Children.begin(); it != Children.end(); ++it) {
                if (*it == element) {
                    element->Parent = nullptr;
                    Children.erase(it);
                    return;
                }
            }

            // TODO: throw exception that child not found.
        }

        inline size_t ChildrenSize() const {
            return Children.size();
        }

        template <class T, class Fn>
        void ForEachChild(Fn&& fn) const {
            size_t i = 0;

            for (const auto& child : Children) {
                T* ptr;
                if constexpr (std::is_virtual_base_of_v<TTreeElementBase, T>) {
                    ptr = dynamic_cast<T*>(child.get());
                    Y_ASSERT(ptr);
                } else {
                    ptr = static_cast<T*>(child.get());
                }
                if constexpr (std::is_void_v<std::invoke_result_t<Fn, T*, size_t>>) {
                    fn(ptr, i++);
                } else {
                    if (fn(ptr, i++)) {
                        break;
                    }
                }
            }
        }

        // True for parentless and real root
        virtual inline bool IsRoot() const {
            return !Parent;
        }

        // True for everything except queries
        virtual inline bool IsPool() const {
            return true;
        }

        // True for queries and leaf-pools
        virtual inline bool IsLeaf() const {
            return Children.empty();
        }

    protected:
        const TId Id;
        TTreeElementBase* Parent = nullptr;

    private:
        std::vector<TPtr> Children;
    };

    template <ETreeType T>
    struct TQuery : public virtual TTreeElementBase<T> {
        using TBase = TTreeElementBase<T>;
        using TPtr = std::shared_ptr<TQuery>;

        explicit TQuery(const TQueryId& id, const TStaticAttributes& attrs = {}) : TBase(id, attrs) {}

        void AddChild(const typename TBase::TPtr& element) = delete;
        void RemoveChild(const typename TBase::TPtr& element) = delete;

        inline bool IsRoot() const final {
            return false;
        }

        inline bool IsPool() const final {
            return false;
        }
    };

    struct TPoolCounters {
        NMonitoring::TDynamicCounters::TCounterPtr Limit;
        NMonitoring::TDynamicCounters::TCounterPtr Guarantee;
        NMonitoring::TDynamicCounters::TCounterPtr Demand;
        NMonitoring::TDynamicCounters::TCounterPtr Usage;
        NMonitoring::TDynamicCounters::TCounterPtr UsageResume;
        NMonitoring::TDynamicCounters::TCounterPtr Throttle;
        NMonitoring::TDynamicCounters::TCounterPtr FairShare;
        NMonitoring::TDynamicCounters::TCounterPtr InFlight;
        NMonitoring::TDynamicCounters::TCounterPtr Waiting;
        NMonitoring::TDynamicCounters::TCounterPtr Satisfaction;
        NMonitoring::TDynamicCounters::TCounterPtr InFlightExtra;
        NMonitoring::TDynamicCounters::TCounterPtr UsageExtra;
        NMonitoring::THistogramPtr                 Delay;
    };

    template <ETreeType T>
    struct TPool : public virtual TTreeElementBase<T> {
        using TBase = TTreeElementBase<T>;
        using TPtr = std::shared_ptr<TPool>;

        explicit TPool(const TPoolId& id, const TStaticAttributes& attrs = {}) : TBase(id, attrs) {}

        void AddQuery(const TQuery<T>::TPtr& query) {
            Y_ENSURE(Pools.empty());
            Y_ENSURE(!Queries.contains(std::get<TQueryId>(query->GetId())));
            Queries.emplace(std::get<TQueryId>(query->GetId()), query);
            AddChild(query);
        }

        void RemoveQuery(const TQueryId& queryId) {
            Y_ENSURE(Pools.empty());
            auto queryIt = Queries.find(queryId);
            Y_ENSURE(queryIt != Queries.end());
            RemoveChild(queryIt->second);
            Queries.erase(queryIt);
        }

        TQuery<T>::TPtr GetQuery(const TQueryId& queryId) const {
            Y_ENSURE(Pools.empty());
            auto it = Queries.find(queryId);
            return it == Queries.end() ? nullptr : it->second;
        }

        void AddPool(const TPool::TPtr& pool) {
            Y_ENSURE(Queries.empty());
            Y_ENSURE(!Pools.contains(std::get<TPoolId>(pool->GetId())));
            Pools.emplace(std::get<TPoolId>(pool->GetId()), pool);
            AddChild(pool);
        }

        void RemovePool(const TPoolId& poolId) {
            Y_ENSURE(Queries.empty());
            auto poolIt = Pools.find(poolId);
            Y_ENSURE(poolIt != Pools.end());
            RemoveChild(poolIt->second);
            Pools.erase(poolIt);
        }

        TPool::TPtr GetPool(const TPoolId& poolId) const {
            Y_ENSURE(Queries.empty());
            auto it = Pools.find(poolId);
            return it == Pools.end() ? nullptr : it->second;
        }

        inline bool IsPool() const final {
            return true;
        }

        inline bool IsLeaf() const final {
            return this->ChildrenSize() == 0 || !Queries.empty();
        }

    protected:
        std::optional<TPoolCounters> Counters;

    private:
        using TBase::AddChild;
        using TBase::RemoveChild;

        THashMap<TQueryId, typename TQuery<T>::TPtr> Queries;
        THashMap<TPoolId, typename TPool::TPtr> Pools;
    };

} // namespace NKikimr::NKqp::NScheduler::NHdrf
