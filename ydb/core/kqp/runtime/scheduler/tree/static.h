#pragma once

#include "common.h"

namespace NKikimr::NKqp::NScheduler::NHdrf::NStatic {

// Build under exclusive ownership, then publish as const. References to published
// nodes alias the root's shared_ptr and keep the whole version alive.
struct TTreeElement : public NHdrf::TTreeElementBase<ETreeType::STATIC>, public TStaticAttributes {
public:
    using TBase = NHdrf::TTreeElementBase<ETreeType::STATIC>;

    TTreeElement(const TId& id, const TStaticAttributes& attrs)
        : TBase(id), TStaticAttributes(attrs) {}

    const TPool* GetParent() const;

    void SetAttributes(const TStaticAttributes& attrs) {
        static_cast<TStaticAttributes&>(*this) = attrs;
    }

protected:
    using TBase::AddChild;
    using TBase::RemoveChild;
};

class TPool : public TTreeElement {
public:
    TPool(const TPoolId& id, const TStaticAttributes& attrs = {})
        : TTreeElement(id, attrs) {}

    const TPool* GetPool(const TPoolId& id) const {
        const auto it = Pools.find(id);
        return it == Pools.end() ? nullptr : it->second.get();
    }

    TPool* GetPool(const TPoolId& id) {
        const auto it = Pools.find(id);
        return it == Pools.end() ? nullptr : it->second.get();
    }

    TPool& GetOrCreatePool(const TPoolId& id) {
        if (const auto it = Pools.find(id); it != Pools.end()) {
            return *it->second;
        }
        auto pool = std::make_shared<TPool>(id);
        auto& result = *pool;
        AddPool(std::move(pool));
        return result;
    }

    void RemovePool(const TPoolId& id) {
        if (const auto it = Pools.find(id); it != Pools.end()) {
            RemoveChild(it->second);
            Pools.erase(it);
        }
    }

protected:
    TPool(const TPool& other)
        : TTreeElement(other.GetId(), other)
    {
        other.ForEachChild<TPool>([&](const TPool* child, size_t) {
            AddPool(std::shared_ptr<TPool>(new TPool(*child)));
        });
    }

    void AddPool(std::shared_ptr<TPool> pool) {
        const auto id = std::get<TPoolId>(pool->GetId());
        AddChild(pool);
        Y_ENSURE(Pools.emplace(id, std::move(pool)).second);
    }

private:
    THashMap<TPoolId, std::shared_ptr<TPool>> Pools;
};

inline const TPool* TTreeElement::GetParent() const {
    return static_cast<const TPool*>(Parent);
}

class TDatabase final : public TPool {
public:
    TDatabase(const TDatabaseId& id, const TStaticAttributes& attrs = {})
        : TPool(id, attrs) {}

    TDatabase(const TDatabase& other)
        : TPool(other) {}
};

class TRoot final : public TPool {
public:
    explicit TRoot(ui64 totalCpuLimit = Infinity(), ui64 totalMemoryLimit = Infinity())
        : TPool("(ROOT)")
        , TotalCpuLimit(totalCpuLimit)
        , TotalMemoryLimit(totalMemoryLimit)
    {}

    TRoot(const TRoot& other)
        : TRoot(other.TotalCpuLimit, other.TotalMemoryLimit)
    {
        SetAttributes(other);
        other.ForEachChild<TDatabase>([&](const TDatabase* database, size_t) {
            AddPool(std::make_shared<TDatabase>(*database));
        });
    }

    const TDatabase* GetDatabase(const TDatabaseId& id) const {
        return static_cast<const TDatabase*>(GetPool(id));
    }

    TDatabase& GetOrCreateDatabase(const TDatabaseId& id) {
        if (auto* database = TPool::GetPool(id)) {
            return *static_cast<TDatabase*>(database);
        }
        auto database = std::make_shared<TDatabase>(id);
        auto& result = *database;
        AddPool(std::move(database));
        return result;
    }

    bool SupportsGuarantees() const { return ChildrenSize() <= 1; }

    std::unique_ptr<TRoot> Clone() const {
        return std::make_unique<TRoot>(*this);
    }

    ui64 TotalCpuLimit;
    ui64 TotalMemoryLimit;

private:
    using TPool::GetOrCreatePool;
    using TPool::RemovePool;
};

// Aliasing ownership keeps ancestors and siblings alive together with the node.
inline TPoolPtr GetPool(const TPoolPtr& parent, const TPoolId& id) {
    const auto* pool = parent ? parent->GetPool(id) : nullptr;
    return pool ? TPoolPtr(parent, pool) : nullptr;
}

inline TDatabasePtr GetDatabase(const TRootPtr& root, const TDatabaseId& id) {
    const auto* database = root->GetDatabase(id);
    return database ? TDatabasePtr(root, database) : nullptr;
}

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NStatic
