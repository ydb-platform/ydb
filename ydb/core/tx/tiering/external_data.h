#pragma once
#include "rule.h"
#include "tier_config.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TConfigsSnapshot: public NMetadataProvider::ISnapshot {
private:
    using TBase = NMetadataProvider::ISnapshot;
    using TConfigsMap = TMap<TString, TTierConfig>;
    YDB_ACCESSOR_DEF(TConfigsMap, TierConfigs);
    using TTieringMap = TMap<TString, TTableTiering>;
    YDB_ACCESSOR_DEF(TTieringMap, TableTierings);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) override;
    virtual TString DoSerializeToString() const override;
public:
    std::vector<TTierConfig> GetTiersForPathId(const ui64 pathId) const;
    const TTableTiering* GetTableTiering(const TString& tablePath) const;
    void RemapTablePathToId(const TString& path, const ui64 pathId);
    std::optional<TTierConfig> GetValue(const TString& key) const;
    using TBase::TBase;
};

class TSnapshotConstructor;

class TSnapshotConstructorAgent: public TActor<TSnapshotConstructorAgent> {
private:
    using TBase = TActor<TSnapshotConstructorAgent>;
    std::shared_ptr<TSnapshotConstructor> Owner;
private:
    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        PassAway();
    }
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                break;
        }
    }

    TSnapshotConstructorAgent(std::shared_ptr<TSnapshotConstructor> owner)
        : TBase(&TThis::StateMain)
        , Owner(owner)
    {

    }

    ~TSnapshotConstructorAgent();

    void ProvideEvent(IEventBase* event, const TActorId& recipient);
};

class TSnapshotConstructor: public NMetadataProvider::TGenericSnapshotParser<TConfigsSnapshot> {
private:
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TBaseActor = TActor<TSnapshotConstructor>;
    using ISnapshot = NMetadataProvider::ISnapshot;
    TVector<TString> Tables;
    TMap<TString, ui64> TablesRemapper;
    TSnapshotConstructorAgent* Actor = nullptr;
    TString TablePath;
    mutable std::shared_ptr<TConfigsSnapshot> WaitSnapshot;
    mutable NThreading::TPromise<NMetadataProvider::ISnapshot::TPtr> WaitPromise;
protected:
    virtual TVector<NMetadataProvider::ITableModifier::TPtr> DoGetTableSchema() const override;
    virtual const TVector<TString>& DoGetTables() const override {
        return Tables;
    }
public:
    void ResolveInfo(const TEvTxProxySchemeCache::TEvNavigateKeySetResult* info);
    virtual NThreading::TFuture<ISnapshot::TPtr> EnrichSnapshotData(ISnapshot::TPtr original) const override;

    TSnapshotConstructor();

    void Start(std::shared_ptr<TSnapshotConstructor> ownerPtr) {
        Y_VERIFY(!Actor);
        Actor = new TSnapshotConstructorAgent(ownerPtr);
        TActivationContext::AsActorContext().RegisterWithSameMailbox(Actor);
    }

    void ActorStopped() {
        Actor = nullptr;
    }

    void Stop() {
        if (Actor && TlsActivationContext) {
            TActivationContext::AsActorContext().Send(Actor->SelfId(), new NActors::TEvents::TEvPoison);
        }
    }

    ~TSnapshotConstructor() {
    }
};

}
