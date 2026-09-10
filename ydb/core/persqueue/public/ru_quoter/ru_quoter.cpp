#include "ru_quoter.h"

#include <ydb/core/base/path.h>
#include <ydb/core/metering/bill_record.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ::NRuQuoter {

namespace {

using namespace NSchemeCache;

TMaybe<TRlContext> DoParseRlContext(
    const THashMap<TString, TString>& attrs,
    const TString& database,
    const TString& token)
{
    TString coordinationNode;
    TString resourcePath;
    if (const auto* value = attrs.FindPtr(RL_COORDINATION_NODE_ATTR)) {
        coordinationNode = *value;
    }
    if (const auto* value = attrs.FindPtr(RL_TOPIC_RESOURCE_ATTR)) {
        resourcePath = *value;
    }
    if (coordinationNode.empty() || resourcePath.empty()) {
        return Nothing();
    }
    return TRlContext(coordinationNode, resourcePath, database, token);
}

TMeteringIds DoParseMeteringIds(const THashMap<TString, TString>& attrs) {
    TMeteringIds ids;
    if (const auto* value = attrs.FindPtr(CLOUD_ID_ATTR)) {
        ids.CloudId = *value;
    }
    if (const auto* value = attrs.FindPtr(FOLDER_ID_ATTR)) {
        ids.FolderId = *value;
    }
    if (const auto* value = attrs.FindPtr(DATABASE_ID_ATTR)) {
        ids.DatabaseId = *value;
    }
    return ids;
}

class TRequestUnitsQuoter
    : public TBaseActor<TRequestUnitsQuoter>
    , public TConstantLogPrefix
    , public TRlHelpers
{
    using EWakeupTag = TRlHelpers::EWakeupTag;

public:
    TRequestUnitsQuoter(NActors::TActorId parent, TRequestUnitsQuoterSettings settings)
        : TBaseActor(NKikimrServices::PERSQUEUE)
        , TRlHelpers({}, TRlContext(), 4_KB, false)
        , Parent_(parent)
        , Settings_(std::move(settings))
    {
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        TRlHelpers::Bootstrap(SelfId(), ctx);
        SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
        Become(&TRequestUnitsQuoter::StateWork);
        if (Settings_.Ru == 0) {
            return Reply(EStatus::Success);
        }
        SendDatabaseNavigate();
    }

    TLogPrefix BuildLogPrefix() const override {
        return YDB_LOG_CREATE_MESSAGE(
            {"actorClassName", "RequestUnitsQuoter"},
            {"database", Settings_.Database});
    }

    void OnException(const std::exception& exc) override {
        // Do not PassAway here: TBaseActor::OnUnhandledException will.
        Send(Parent_, new TEvChargeRequestUnitsResponse(
            EStatus::UnknownError,
            TStringBuilder() << "Unhandled exception: " << exc.what()));
    }

private:
    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
        sFunc(NActors::TEvents::TEvPoison, PassAway);
    )

    void PassAway() override {
        TRlHelpers::PassAway(SelfId());
        TBaseActor::PassAway();
    }

    void SendDatabaseNavigate() {
        auto request = MakeHolder<TSchemeCacheNavigate>();
        TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(Settings_.Database);
        entry.Operation = TSchemeCacheNavigate::OpPath;
        entry.SyncVersion = false;
        request->ResultSet.emplace_back(std::move(entry));
        request->DatabaseName = Settings_.Database;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    static const TSchemeCacheNavigate::TEntry* GetNavigateEntry(
        const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev)
    {
        const TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        if (!result || result->ResultSet.empty()) {
            return nullptr;
        }
        const auto& entry = result->ResultSet.front();
        if (entry.Status != TSchemeCacheNavigate::EStatus::Ok) {
            return nullptr;
        }
        return &entry;
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& ctx = TlsActivationContext->AsActorContext();
        if (const auto* entry = GetNavigateEntry(ev)) {
            MeteringIds_ = DoParseMeteringIds(entry->Attributes);
            if (auto rlContext = DoParseRlContext(entry->Attributes, Settings_.Database, Settings_.Token)) {
                SetRlContext(*rlContext);
            }
        }
        if (IsQuotaRequired()) {
            AFL_ENSURE(MaybeRequestQuota(Settings_.Ru, EWakeupTag::RlAllowed, ctx))
                ("ru", Settings_.Ru);
            return;
        }
        WriteBill(ctx);
        Reply(EStatus::Success);
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        TRlHelpers::OnWakeup(tag);
        const auto& ctx = TlsActivationContext->AsActorContext();
        switch (tag) {
            case EWakeupTag::RlAllowed:
                WriteBill(ctx);
                Reply(EStatus::Success);
                return;
            case EWakeupTag::RlNoResource:
                Reply(EStatus::Throttled);
                return;
            default:
                Reply(EStatus::UnknownError);
                return;
        }
    }

    void WriteBill(const NActors::TActorContext& ctx) {
        if (!MeteringIds_.IsComplete() || Settings_.Ru == 0) {
            return;
        }
        NMetering::SendMeteringJson(
            ctx,
            MakeRequestUnitsBill(MeteringIds_, Settings_.Ru, ctx.Now(), CreateGuidAsString()));
    }

    void Reply(EStatus status) {
        Send(Parent_, new TEvChargeRequestUnitsResponse(status, Description(status)));
        PassAway();
    }

private:
    const NActors::TActorId Parent_;
    const TRequestUnitsQuoterSettings Settings_;
    TMeteringIds MeteringIds_;
};

} // namespace

TMaybe<TRlContext> ParseRlContext(
    const THashMap<TString, TString>& attrs,
    const TString& database,
    const TString& token)
{
    return DoParseRlContext(attrs, database, token);
}

TMeteringIds ParseMeteringIds(const THashMap<TString, TString>& attrs) {
    return DoParseMeteringIds(attrs);
}

TString MakeRequestUnitsBill(const TMeteringIds& ids, ui64 ru, TInstant now, const TString& id) {
    return TBillRecord()
        .Id(id)
        .Schema(TString(REQUEST_UNITS_SCHEMA))
        .CloudId(ids.CloudId)
        .FolderId(ids.FolderId)
        .ResourceId(ids.DatabaseId)
        .SourceWt(now)
        .Usage(TBillRecord::RequestUnits(ru, now))
        .ToString();
}

NActors::IActor* CreateRequestUnitsQuoter(const NActors::TActorId& parent, TRequestUnitsQuoterSettings settings) {
    return new TRequestUnitsQuoter(parent, std::move(settings));
}

Ydb::StatusIds::StatusCode Convert(const EStatus status) {
    switch (status) {
        case EStatus::Success:
            return Ydb::StatusIds::SUCCESS;
        case EStatus::Throttled:
            return Ydb::StatusIds::OVERLOADED;
        case EStatus::UnknownError:
            return Ydb::StatusIds::INTERNAL_ERROR;
    }
}

TString Description(const EStatus status) {
    switch (status) {
        case EStatus::Success:
            return "Request units have been charged";
        case EStatus::Throttled:
            return "Request was throttled by the rate limiter";
        case EStatus::UnknownError:
            return "Unexpected rate limiter wakeup";
    }
}

} // namespace NKikimr::NPQ::NRuQuoter
