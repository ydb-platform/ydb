#include "list.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/rule/object.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

class TFetchTieringRulesActor: public NKqp::NWorkload::TSchemeActorBase<TFetchTieringRulesActor> {
private:
    TActorId Recipient;
    std::set<TPathId> UnfetchedObjects;
    THashMap<TString, TTieringRule> Result;

private:
    void ReplyErrorAndPassAway(const TString& errorMessage) {
        Send(Recipient, new NTiers::TEvListTieringRulesResult(TConclusionStatus::Fail(errorMessage)));
        PassAway();
    }

    void ReplySuccessAndPassAway() {
        Send(Recipient, new NTiers::TEvListTieringRulesResult(std::move(Result)));
        PassAway();
    }

    static THolder<NSchemeCache::TSchemeCacheNavigate> BuildFetchRequest(const std::set<TPathId>& paths) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = AppDataVerified().TenantName;
        request->UserToken = MakeIntrusive<NACLib::TUserToken>(NACLib::TSystemUsers::Metadata());

        for (const auto& pathId : paths) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
            entry.TableId.PathId = pathId;
            entry.ShowPrivatePath = true;
        }

        return request;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        for (const auto& result : results) {
            switch (result.Status) {
                case EStatus::PathNotTable:
                case EStatus::PathNotPath:
                case EStatus::AccessDenied:
                    AFL_VERIFY(false)("status", result.Status)("result", result.ToString());
                    return;
                case EStatus::Unknown:
                case EStatus::RedirectLookupError:
                case EStatus::LookupError:
                case EStatus::TableCreationNotComplete:
                    if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                        ReplyErrorAndPassAway("Retry limit exceeded");
                    }
                    return;
                case EStatus::RootUnknown:
                case EStatus::PathErrorUnknown:
                    OnObjectFetched(std::nullopt, result.TableId.PathId);
                    return;
                case EStatus::Ok:
                    AFL_VERIFY(result.Kind == NSchemeCache::TSchemeCacheNavigate::KindTieringRule)("kind", result.Kind)("result", result.ToString());
                    OnObjectFetched(result.TieringRuleInfo->Description, result.TableId.PathId);
                    return;
            }
        }

        if (UnfetchedObjects.empty()) {
            ReplySuccessAndPassAway();
            return;
        }
    }

    void OnObjectFetched(std::optional<NKikimrSchemeOp::TTieringRuleDescription> description, const TPathId& pathId) {
        if (description) {
            TTieringRule tieringRule;
            AFL_VERIFY(tieringRule.DeserializeFromProto(*description));
            Result.emplace(description->GetName(), std::move(tieringRule));
        }
        UnfetchedObjects.erase(pathId);
    }

protected:
    void StartRequest() override {
        auto event = BuildFetchRequest(UnfetchedObjects);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode /*status*/, NYql::TIssue issue) override {
        ReplyErrorAndPassAway(issue.ToString(true));
    }

    TString LogPrefix() const override {
        return "[TFetchTieringRulesActor] ";
    }

public:
    TFetchTieringRulesActor(const TActorId& recipient, std::set<TPathId> tieringRules)
        : Recipient(recipient)
        , UnfetchedObjects(std::move(tieringRules)) {
    }

    void DoBootstrap() {
        Become(&TFetchTieringRulesActor::StateMain);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }
};

class TListTieringRulesActor: public NKqp::NWorkload::TSchemeActorBase<TListTieringRulesActor> {
private:
    TActorId Recipient;

private:
    void ReplyErrorAndPassAway(const TString& errorMessage) {
        Send(Recipient, new NTiers::TEvListTieringRulesResult(TConclusionStatus::Fail(errorMessage)));
        PassAway();
    }

    static THolder<NSchemeCache::TSchemeCacheNavigate> BuildListRequest(const TVector<TString>& pathComponents) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = AppDataVerified().TenantName;
        request->UserToken = MakeIntrusive<NACLib::TUserToken>(NACLib::TSystemUsers::Metadata());

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.Path = pathComponents;

        return request;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        AFL_VERIFY(results.size() == 1)("size", results.size());
        const auto& result = results[0];
        switch (result.Status) {
            case EStatus::AccessDenied:
            case EStatus::PathNotTable:
            case EStatus::PathNotPath:
                AFL_VERIFY(false)("status", result.Status)("result", result.ToString());
                return;
            case EStatus::Unknown:
            case EStatus::RedirectLookupError:
            case EStatus::LookupError:
            case EStatus::TableCreationNotComplete:
                if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                    ReplyErrorAndPassAway("Retry limit exceeded");
                }
                return;
            case EStatus::RootUnknown:
            case EStatus::PathErrorUnknown:
                OnObjectsListed({});
                return;
            case EStatus::Ok:
                OnObjectsListed(result.ListNodeEntry->Children);
                return;
        }
    }

    void OnObjectsListed(TVector<NSchemeCache::TSchemeCacheNavigate::TListNodeEntry::TChild> nodes) {
        std::set<TPathId> objects;
        for (const auto& node : nodes) {
            objects.insert(node.PathId);
        }
        TActivationContext::Register(new TFetchTieringRulesActor(Recipient, std::move(objects)), Recipient);
        PassAway();
    }

protected:
    void StartRequest() override {
        auto event = BuildListRequest({ NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath() });
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode /*status*/, NYql::TIssue issue) override {
        ReplyErrorAndPassAway(issue.ToString(true));
    }

    TString LogPrefix() const override {
        return "[TListTieringRulesActor] ";
    }

public:
    TListTieringRulesActor(const TActorId recipient)
        : Recipient(recipient) {
    }

    void DoBootstrap() {
        Become(&TListTieringRulesActor::StateMain);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }
};

}   // namespace NTiers

THolder<IActor> MakeListTieringRulesActor(TActorId recipient) {
    return MakeHolder<NTiers::TListTieringRulesActor>(recipient);
}

}   // namespace NKikimr::NColumnShard
