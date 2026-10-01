#include "schemeshard_iam_delegation.h"
#include "schemeshard_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

TVector<TIamDelegationRevocation> NamedIamDelegations(const NKikimrSchemeOp::TSecretDescription& secret) {
    TVector<TIamDelegationRevocation> result;
    const auto add = [&](const NKikimrSchemeOp::TIamDelegation& delegation, TInstant notBefore) {
        if (!delegation.GetReferrerId().empty()) {
            result.push_back({
                .ReferrerId = delegation.GetReferrerId(),
                .ServiceAccountId = delegation.GetServiceAccountId(),
                .CloudId = delegation.GetCloudId(),
                .NotBefore = notBefore,
            });
        }
    };
    if (secret.HasIamDelegation()) {
        add(secret.GetIamDelegation(), secret.GetIamDelegationSetUp()
            ? TInstant::Zero()
            : TInstant::MicroSeconds(secret.GetIamDelegationNamedAt()) + StagedIamDelegationLease);
    }
    if (secret.HasPendingIamDelegation()) {
        add(secret.GetPendingIamDelegation(), TInstant::MicroSeconds(secret.GetPendingIamDelegationStagedAt()) + StagedIamDelegationLease);
    }
    return result;
}

namespace {

constexpr TDuration MinClaimLease = TDuration::Seconds(10);
constexpr TDuration MaxClaimLease = TDuration::Hours(1);
constexpr TDuration DefaultClaimLease = TDuration::Minutes(5);
constexpr size_t MaxRevocationsPerClaim = 1000;

// Deletes the records of the revocations a node reports as accepted by IAM.
class TTxIamDelegationsRevoked: public TTransactionBase<TSchemeShard> {
public:
    TTxIamDelegationsRevoked(TSchemeShard* self, TEvSchemeShard::TEvIamDelegationsRevoked::TPtr& ev)
        : TTransactionBase(self)
        , Request(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_IAM_DELEGATIONS_REVOKED;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);
        const ui64 claimId = Request->Get()->Record.GetClaimId();
        for (const auto& referrerId : Request->Get()->Record.GetReferrerIds()) {
            if (!Self->IamDelegationRevocations.contains(referrerId)) {
                continue;
            }
            // only the claim the record was handed out with may report the revocation: a late report of a
            // claim that has run out (or of a record written again for a reused referrer) must not delete it
            const auto claim = Self->ClaimedIamDelegationRevocations.find(referrerId);
            if (claim == Self->ClaimedIamDelegationRevocations.end() || claim->second.ClaimId != claimId || claim->second.Until <= ctx.Now()) {
                YDB_LOG_WARN("IAM delegation revocation reported without a live claim, ignored",
                    {"revocation", Self->IamDelegationRevocations.at(referrerId).ToString()}, {"claim", claimId}, {"sender", Request->Sender});
                continue;
            }
            YDB_LOG_NOTICE("IAM delegation revoked", {"revocation", Self->IamDelegationRevocations.at(referrerId).ToString()});
            Self->PersistIamDelegationRevocationRemove(db, referrerId);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Request->Sender, new TEvSchemeShard::TEvIamDelegationsRevokedResult(), 0, Request->Cookie);
    }

private:
    TEvSchemeShard::TEvIamDelegationsRevoked::TPtr Request;
};

} // namespace

std::optional<TString> TSchemeShard::FindIamDelegationReferrer(const TString& referrerId) const {
    // A scan: delegation secrets are few, and only CREATE and STAGE of one ask. An index would have to follow
    // every change of Secrets, the rollbacks of MemChanges included.
    for (const auto& [pathId, secretInfo] : Secrets) {
        for (const TSecretInfo* info : {secretInfo.Get(), secretInfo->AlterData.Get()}) {
            if (!info) {
                continue;
            }
            for (const auto& named : NamedIamDelegations(info->Description)) {
                if (named.ReferrerId == referrerId) {
                    return TStringBuilder() << "secret " << pathId;
                }
            }
        }
    }
    if (IamDelegationRevocations.contains(referrerId)) {
        return "the outbox of revocations";
    }
    return std::nullopt;
}

void TSchemeShard::PersistIamDelegationRevocation(NIceDb::TNiceDb& db, const TIamDelegationRevocation& revocation) {
    AFL_ENSURE(!revocation.ReferrerId.empty())("tablet_id", TabletID())("secret_local_path_id", revocation.PathId.LocalPathId);
    db.Table<Schema::IamDelegationRevocations>().Key(revocation.ReferrerId).Update(
        NIceDb::TUpdate<Schema::IamDelegationRevocations::ServiceAccountId>(revocation.ServiceAccountId),
        NIceDb::TUpdate<Schema::IamDelegationRevocations::CloudId>(revocation.CloudId),
        NIceDb::TUpdate<Schema::IamDelegationRevocations::PathId>(revocation.PathId.LocalPathId),
        NIceDb::TUpdate<Schema::IamDelegationRevocations::NotBefore>(revocation.NotBefore.MicroSeconds()));
    IamDelegationRevocations[revocation.ReferrerId] = revocation;
}

void TSchemeShard::PersistIamDelegationRevocationRemove(NIceDb::TNiceDb& db, const TString& referrerId) {
    db.Table<Schema::IamDelegationRevocations>().Key(referrerId).Delete();
    IamDelegationRevocations.erase(referrerId);
    ClaimedIamDelegationRevocations.erase(referrerId);
}

void TSchemeShard::PersistIamDelegationRevocations(NIceDb::TNiceDb& db, TPathId pathId,
    const NKikimrSchemeOp::TSecretDescription& before, const NKikimrSchemeOp::TSecretDescription* after, EPendingIamDelegationSetup pendingSetup)
{
    THashSet<TString> stillNamed;
    if (after) {
        for (const auto& delegation : NamedIamDelegations(*after)) {
            stillNamed.insert(delegation.ReferrerId);
        }
    }
    for (auto& revocation : NamedIamDelegations(before)) {
        if (stillNamed.contains(revocation.ReferrerId)) {
            continue;
        }
        if (pendingSetup == EPendingIamDelegationSetup::Over && before.HasPendingIamDelegation()
            && revocation.ReferrerId == before.GetPendingIamDelegation().GetReferrerId())
        {
            revocation.NotBefore = TInstant::Zero();
        }
        revocation.PathId = pathId;
        YDB_LOG_NOTICE("IAM delegation is no longer named by its secret, recording the revocation", {"revocation", revocation.ToString()});
        PersistIamDelegationRevocation(db, revocation);
    }
}

void TSchemeShard::Handle(TEvSchemeShard::TEvClaimIamDelegationRevocations::TPtr& ev, const TActorContext& ctx) {
    const TInstant now = ctx.Now();
    const auto& record = ev->Get()->Record;
    const TDuration lease = record.HasLeaseSeconds()
        ? TDuration::Seconds(ClampVal(record.GetLeaseSeconds(), MinClaimLease.Seconds(), MaxClaimLease.Seconds()))
        : DefaultClaimLease;
    AFL_ENSURE(NextIamDelegationClaimId < Max<ui32>())("tablet_id", TabletID());
    const ui64 claimId = (Generation() << 32) | ++NextIamDelegationClaimId;
    auto result = MakeHolder<TEvSchemeShard::TEvClaimIamDelegationRevocationsResult>();
    result->Record.SetClaimId(claimId);
    for (const auto& [referrerId, revocation] : IamDelegationRevocations) {
        if (result->Record.RevocationsSize() >= MaxRevocationsPerClaim) {
            break; // the claimer comes back for the rest
        }
        if (revocation.NotBefore > now) {
            continue;
        }
        if (const auto it = ClaimedIamDelegationRevocations.find(referrerId); it != ClaimedIamDelegationRevocations.end() && it->second.Until > now) {
            continue;
        }
        ClaimedIamDelegationRevocations[referrerId] = {.ClaimId = claimId, .Until = now + lease};
        auto& claimed = *result->Record.AddRevocations();
        claimed.SetReferrerId(revocation.ReferrerId);
        claimed.SetServiceAccountId(revocation.ServiceAccountId);
        claimed.SetCloudId(revocation.CloudId);
    }
    if (result->Record.RevocationsSize()) {
        YDB_LOG_INFO("IAM delegation revocations claimed", {"claimer", ev->Sender}, {"claim", claimId},
            {"count", result->Record.RevocationsSize()}, {"lease", lease});
    }
    ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvIamDelegationsRevoked::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxIamDelegationsRevoked(this, ev), ctx);
}

} // namespace NKikimr::NSchemeShard

#undef YDB_LOG_THIS_FILE_COMPONENT
