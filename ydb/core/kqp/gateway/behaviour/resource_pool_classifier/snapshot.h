#pragma once

#include "object.h"

#include <ydb/services/metadata/abstract/fetcher.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierSnapshot : public NMetadata::NFetcher::ISnapshot {
    using TBase = NMetadata::NFetcher::ISnapshot;

public:
    struct TClassifiersInfo {
        std::unordered_map<TString, TResourcePoolClassifierConfig> ByName;
        std::map<i64, TResourcePoolClassifierConfig> ByRank;
    };

private:
    using TConfigsMap = std::unordered_map<TString, TClassifiersInfo>;

    YDB_ACCESSOR_DEF(TConfigsMap, ResourcePoolClassifierConfigs);

protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;

public:
    using TBase::TBase;

    std::optional<TResourcePoolClassifierConfig> GetClassifierConfig(const TString& database, const TString& name) const;
};

class TClassifierConfigsView {
    using TConfigsByRankMap = std::map<i64, TResourcePoolClassifierConfig>;
    using TResourcePoolClassifierSnapshotPtr = std::shared_ptr<const TResourcePoolClassifierSnapshot>;

public:
    TClassifierConfigsView() = default;

    TClassifierConfigsView(TResourcePoolClassifierSnapshotPtr snapshot, const TString& databaseId)
        : Snapshot(std::move(snapshot))
    {
        if (Snapshot) {
            const auto& all = Snapshot->GetResourcePoolClassifierConfigs();
            if (auto it = all.find(databaseId); it != all.end()) {
                Configs = &it->second.ByRank;
            }
        }
    }

    explicit operator bool() const { return Configs != nullptr; }
    const TConfigsByRankMap* operator->() const { return Configs; }
    const TConfigsByRankMap& operator*() const { return *Configs; }
    const TConfigsByRankMap* Get() const { return Configs; }

private:
    TResourcePoolClassifierSnapshotPtr Snapshot;
    const TConfigsByRankMap* Configs = nullptr;
};

}  // namespace NKikimr::NKqp
