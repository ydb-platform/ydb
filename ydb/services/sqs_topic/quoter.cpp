#include "quoter.h"
#include "metering_attrs.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/guid.h>

namespace NKikimr::NSqsTopic::V1 {

    class TRequestUnitsQuoter
        : public NActors::TActorBootstrapped<TRequestUnitsQuoter>
        , public NPQ::TRlHelpers
    {
        using EWakeupTag = NPQ::TRlHelpers::EWakeupTag;

    public:
        TRequestUnitsQuoter(NActors::TActorId parent, TRequestUnitsQuoterSettings settings)
            : NPQ::TRlHelpers({}, NPQ::TRlContext(), NBilling::WRITE_BLOCK_SIZE, false)
            , Parent_(parent)
            , Settings_(std::move(settings))
        {
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            NPQ::TRlHelpers::Bootstrap(SelfId(), ctx);
            SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
            Become(&TRequestUnitsQuoter::StateWork);
            if (Settings_.Ru == 0) {
                return Reply(TEvChargeRequestUnitsResponse::EStatus::Ok);
            }
            SendDatabaseNavigate();
        }

    private:
        STRICT_STFUNC(StateWork,
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            sFunc(NActors::TEvents::TEvPoison, HandlePoison);
        )

        void HandlePoison() {
            NActors::TActor<TRequestUnitsQuoter>::PassAway();
        }

        void Die(const NActors::TActorContext& ctx) override {
            NPQ::TRlHelpers::PassAway(SelfId());
            NActors::TActor<TRequestUnitsQuoter>::Die(ctx);
        }

        void SendDatabaseNavigate() {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = NKikimr::SplitPath(Settings_.Database);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.SyncVersion = false;
            request->ResultSet.emplace_back(std::move(entry));
            request->DatabaseName = Settings_.Database;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        }

        static const NSchemeCache::TSchemeCacheNavigate::TEntry* GetNavigateEntry(
            const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev)
        {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            if (!result || result->ResultSet.empty()) {
                return nullptr;
            }
            const auto& entry = result->ResultSet.front();
            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                return nullptr;
            }
            return &entry;
        }

        void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            const auto& ctx = TlsActivationContext->AsActorContext();
            if (const auto* entry = GetNavigateEntry(ev)) {
                MeteringIds_ = ParseMeteringIds(entry->Attributes);
                if (auto rlContext = ParseRlContext(entry->Attributes, Settings_.Database, Settings_.Token)) {
                    SetRlContext(*rlContext);
                }
            }
            if (IsQuotaRequired()) {
                AFL_ENSURE(MaybeRequestQuota(Settings_.Ru, EWakeupTag::RlAllowed, ctx))
                    ("ru", Settings_.Ru);
                return;
            }
            WriteBill(ctx);
            Reply(TEvChargeRequestUnitsResponse::EStatus::Ok);
        }

        void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
            const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
            NPQ::TRlHelpers::OnWakeup(tag);
            const auto& ctx = TlsActivationContext->AsActorContext();
            switch (tag) {
                case EWakeupTag::RlAllowed:
                    WriteBill(ctx);
                    Reply(TEvChargeRequestUnitsResponse::EStatus::Ok);
                    return;
                case EWakeupTag::RlNoResource:
                    Reply(TEvChargeRequestUnitsResponse::EStatus::Throttled,
                        "Request was throttled by the rate limiter");
                    return;
                default:
                    Reply(TEvChargeRequestUnitsResponse::EStatus::Error,
                        "Unexpected rate limiter wakeup");
                    return;
            }
        }

        void WriteBill(const NActors::TActorContext& ctx) {
            if (!MeteringIds_.IsComplete() || Settings_.Ru == 0) {
                return;
            }
            NMetering::SendMeteringJson(
                ctx,
                NBilling::MakeRequestUnitsBill(MeteringIds_, Settings_.Ru, ctx.Now(), CreateGuidAsString()));
        }

        void Reply(TEvChargeRequestUnitsResponse::EStatus status, TString message = {}) {
            auto ev = MakeHolder<TEvChargeRequestUnitsResponse>();
            ev->Status = status;
            ev->Message = std::move(message);
            Send(Parent_, ev.Release());
            NActors::TActor<TRequestUnitsQuoter>::PassAway();
        }

    private:
        const NActors::TActorId Parent_;
        const TRequestUnitsQuoterSettings Settings_;
        NBilling::TMeteringIds MeteringIds_;
    };

    NActors::IActor* CreateRequestUnitsQuoter(
        const NActors::TActorId& parent,
        TRequestUnitsQuoterSettings settings)
    {
        return new TRequestUnitsQuoter(parent, std::move(settings));
    }

} // namespace NKikimr::NSqsTopic::V1
