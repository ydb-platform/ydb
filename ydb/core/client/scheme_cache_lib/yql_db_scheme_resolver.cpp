#include "yql_db_scheme_resolver.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {
namespace NSchCache {

using namespace NYql;
using namespace NThreading;

namespace {

class TTableProxyActor : public TActorBootstrapped<TTableProxyActor> {
    using TTable = NYql::IDbSchemeResolver::TTable;
    using TTableResult = NYql::IDbSchemeResolver::TTableResult;
    using TTableResults = NYql::IDbSchemeResolver::TTableResults;

    const TActorId SchemeCache;
    const TActorId ResponseTo;
    TVector<TTable> Tables;

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx) {
        const NSchemeCache::TSchemeCacheNavigate &request = *ev->Get()->Request;
        Y_ABORT_UNLESS(request.ResultSet.size() == Tables.size());

        TVector<TTableResult> results;
        results.reserve(Tables.size());

        for (ui32 idx = 0, end = Tables.size(); idx != end; ++idx) {
            auto &src = Tables[idx];
            const NSchemeCache::TSchemeCacheNavigate::TEntry &res  = request.ResultSet[idx];

            switch (res.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                {
                    results.push_back({TTableResult::Ok});
                    auto &reply = results.back();

                    reply.Status = TTableResult::Ok;
                    reply.Table.TableName = src.TableName;
                    reply.Table.ColumnNames = std::move(src.ColumnNames);
                    reply.TableId = new TTableId(res.TableId);
                    reply.CacheGeneration = 0;

                    TSet<TString> replyColumns;

                    TMap<TString, THashMap<ui32, TSysTables::TTableColumnInfo>::const_iterator> backindex;
                    for (auto it = res.Columns.begin(), eit = res.Columns.end(); it != eit; ++it) {
                        auto& name = it->second.Name;

                        backindex.insert({name, it});

                        if (it->second.KeyOrder >= 0) {
                            replyColumns.insert(name);
                            ++reply.KeyColumnCount;
                        }
                    }

                    replyColumns.insert(reply.Table.ColumnNames.begin(), reply.Table.ColumnNames.end());

                    for (const auto &column : replyColumns) {
                        auto systemColumn = GetSystemColumns().find(column);
                        if (systemColumn != GetSystemColumns().end()) {
                            reply.Columns.insert({
                                column,
                                {systemColumn->second.ColumnId, -1, NScheme::TTypeInfo(systemColumn->second.TypeId), 0, EColumnTypeConstraint::Nullable}
                            });
                            continue;
                        }
                        const auto *x = backindex.FindPtr(column);
                        if (x == nullptr) {
                            reply.Status = TTableResult::Error;
                            reply.Reason = "column '" + column + "' not exist";
                        } else {
                            const auto &col = (*x)->second;
                            auto nullConstraint = res.NotNullColumns.contains(col.Name) ? EColumnTypeConstraint::NotNull : EColumnTypeConstraint::Nullable;
                            reply.Columns.insert(std::make_pair(column,
                                IDbSchemeResolver::TTableResult::TColumn{col.Id, col.KeyOrder, col.PType, 0, nullConstraint}));
                        }
                    }
                }
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                results.push_back({TTableResult::Error, ToString(res.Status)});
                break;
            default:
                results.push_back({TTableResult::LookupError, ToString(res.Status)});
                break;
            }
        }

        ctx.Send(ResponseTo, new NYql::IDbSchemeResolver::TEvents::TEvResolveTablesResult(std::move(results)));
        return Die(ctx);
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLE_SCHEME_RESOLVER;
    }

    TTableProxyActor(TActorId schemeCache, TActorId responseTo, const TVector<TTable> &tables)
        : SchemeCache(schemeCache)
        , ResponseTo(responseTo)
        , Tables(tables)
    {}

    void Bootstrap(const TActorContext &ctx) {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());

        request->ResultSet.reserve(Tables.size());
        for (auto &table : Tables) {
            request->ResultSet.push_back({});
            auto &x = request->ResultSet.back();
            x.Path = SplitPath(table.TableName);
            x.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            x.ShowPrivatePath = true;
        }

        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }
};

} // anonymous namespace

class TDbSchemeResolver : public IDbSchemeResolver {
public:
    TDbSchemeResolver(TActorSystem *actorSystem)
        : HostActorSystem(actorSystem)
    {
    }

    TDbSchemeResolver(TActorSystem *actorSystem, const TActorId &schemeCacheActor)
        : HostActorSystem(actorSystem)
        , SchemeCacheActor(schemeCacheActor)
    {}

    virtual ~TDbSchemeResolver() {}

    virtual NThreading::TFuture<TTableResults> ResolveTables(const TVector<TTable>& tables) override {
        TTableResults results;
        for (auto& table : tables) {
            TTableResult result(TTableResult::Error, "Not implemented");
            result.Table = table;
            results.push_back(result);
        }

        return NThreading::MakeFuture(results);
    }

    virtual void ResolveTables(const TVector<TTable>& tables, NActors::TActorId responseTo) override {
        TAutoPtr<NActors::IActor> proxyActor(new TTableProxyActor(SchemeCacheActor, responseTo, tables));
        HostActorSystem->Register(proxyActor.Release(), TMailboxType::HTSwap, HostActorSystem->AppData<TAppData>()->UserPoolId);
    }

private:
    TActorSystem *HostActorSystem;
    TActorId SchemeCacheActor;
};

NYql::IDbSchemeResolver* CreateDbSchemeResolver(TActorSystem *actorSystem, const TActorId &schemeCacheActor) {
    TAutoPtr<NYql::IDbSchemeResolver> resolver(new TDbSchemeResolver(actorSystem, schemeCacheActor));
    return resolver.Release();
}


} // namespace NSchCache
} // namespace NKikimr
