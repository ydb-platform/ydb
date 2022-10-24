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
    virtual bool DoDeserializeFromResultSet(const Ydb::ResultSet& rawData) = 0;
    virtual TString DoSerializeToString() const = 0;
    i32 GetFieldIndex(const Ydb::ResultSet& rawData, const TString& columnId) const;
public:
    using TPtr = std::shared_ptr<ISnapshot>;
    ISnapshot(const TInstant actuality)
        : Actuality(actuality) {

    }

    bool DeserializeFromResultSet(const Ydb::ResultSet& rawData) {
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
    virtual const TString& DoGetTablePath() const = 0;
public:
    using TPtr = std::shared_ptr<ISnapshotParser>;

    ISnapshot::TPtr ParseSnapshot(const Ydb::ResultSet& rawData, const TInstant actuality) const {
        ISnapshot::TPtr result = CreateSnapshot(actuality);
        Y_VERIFY(result);
        if (!result->DeserializeFromResultSet(rawData)) {
            return nullptr;
        }
        return result;
    }

    TVector<ITableModifier::TPtr> GetTableSchema() const {
        return DoGetTableSchema();
    }

    const TString& GetTablePath() const {
        return DoGetTablePath();
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
