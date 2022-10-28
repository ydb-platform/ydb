#pragma once
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/actor_virtual.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/events.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/metadata/initializer/common.h>

namespace NKikimr::NMetadataProvider {

enum EEvSubscribe {
    EvRefreshSubscriberData = EventSpaceBegin(TKikimrEvents::ES_METADATA_PROVIDER),
    EvRefresh,
    EvEnrichSnapshot,
    EvSubscribeLocal,
    EvUnsubscribeLocal,
    EvSubscribeExternal,
    EvUnsubscribeExternal,
    EvEnd
};

static_assert(EEvSubscribe::EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_PROVIDER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_PROVIDER)");

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

class ISnapshotParser {
protected:
    virtual ISnapshot::TPtr CreateSnapshot(const TInstant actuality) const = 0;
    virtual TVector<ITableModifier::TPtr> DoGetTableSchema() const = 0;
    virtual const TVector<TString>& DoGetTables() const = 0;
    mutable std::optional<TString> SnapshotId;
public:
    using TPtr = std::shared_ptr<ISnapshotParser>;

    TString GetSnapshotId() const;
    ISnapshot::TPtr ParseSnapshot(const Ydb::Table::ExecuteQueryResult& rawData, const TInstant actuality) const;
    TVector<ITableModifier::TPtr> GetTableSchema() const {
        return DoGetTableSchema();
    }

    virtual NThreading::TFuture<ISnapshot::TPtr> EnrichSnapshotData(ISnapshot::TPtr original) const {
        return NThreading::MakeFuture(original);
    }

    const TVector<TString>& GetTables() const {
        return DoGetTables();
    }

    virtual ~ISnapshotParser() = default;
};

template <class TSnapshot>
class TGenericSnapshotParser: public ISnapshotParser {
protected:
    virtual ISnapshot::TPtr CreateSnapshot(const TInstant actuality) const override {
        return std::make_shared<TSnapshot>(actuality);
    }
};

class TEvRefreshSubscriberData: public NActors::TEventLocal<TEvRefreshSubscriberData, EvRefreshSubscriberData> {
private:
    YDB_READONLY_DEF(ISnapshot::TPtr, Snapshot);
public:
    TEvRefreshSubscriberData(ISnapshot::TPtr snapshot)
        : Snapshot(snapshot) {

    }

    template <class TSnapshot>
    const TSnapshot* GetSnapshotAs() const {
        return dynamic_cast<const TSnapshot*>(Snapshot.get());
    }
};

}
