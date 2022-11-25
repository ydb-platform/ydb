#pragma once
#include "manager.h"
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/actor_virtual.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/object_factory/object_factory.h>
#include <ydb/core/base/events.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/metadata/initializer/common.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/manager/alter.h>

namespace NKikimr::NMetadataProvider {

class ISnapshot;

class ISnapshotAcceptorController {
public:
    using TPtr = std::shared_ptr<ISnapshotAcceptorController>;
    virtual ~ISnapshotAcceptorController() = default;
    virtual void Enriched(std::shared_ptr<ISnapshot> enrichedSnapshot) = 0;
    virtual void EnrichProblem(const TString& errorMessage) = 0;
};

class ISnapshot {
private:
    YDB_READONLY_DEF(TInstant, Actuality);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) = 0;
    virtual TString DoSerializeToString() const = 0;
public:
    using TPtr = std::shared_ptr<ISnapshot>;
    ISnapshot(const TInstant actuality)
        : Actuality(actuality) {

    }

    bool DeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) {
        return DoDeserializeFromResultSet(rawData);
    }

    TString SerializeToString() const {
        return DoSerializeToString();
    }

    virtual ~ISnapshot() = default;
};

class ISnapshotsFetcher {
private:
    mutable std::vector<NMetadata::IOperationsManager::TPtr> Managers;
protected:
    virtual ISnapshot::TPtr CreateSnapshot(const TInstant actuality) const = 0;
    virtual std::vector<NMetadata::IOperationsManager::TPtr> DoGetManagers() const = 0;
public:
    using TPtr = std::shared_ptr<ISnapshotsFetcher>;

    TString GetComponentId() const;
    ISnapshot::TPtr ParseSnapshot(const Ydb::Table::ExecuteQueryResult& rawData, const TInstant actuality) const;

    virtual void EnrichSnapshotData(ISnapshot::TPtr original, ISnapshotAcceptorController::TPtr controller) const {
        controller->Enriched(original);
    }

    const std::vector<NMetadata::IOperationsManager::TPtr>& GetManagers() const {
        if (Managers.empty()) {
            Managers = DoGetManagers();
        }
        return Managers;
    }

    virtual ~ISnapshotsFetcher() = default;
};

template <class TSnapshot>
class TSnapshotsFetcher: public ISnapshotsFetcher {
protected:
    virtual ISnapshot::TPtr CreateSnapshot(const TInstant actuality) const override {
        return std::make_shared<TSnapshot>(actuality);
    }
};

}
