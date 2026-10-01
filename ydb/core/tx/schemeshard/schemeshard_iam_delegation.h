#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

// How long the statement that named a delegation (CREATE, or the STAGE of a replacement) may still be setting
// it up in IAM when it has not reported the outcome (CONFIRM, PROMOTE, CANCEL). A delegation whose setup may be
// in flight must not be revoked: IAM would create it after the revocation and nothing would name it. So a
// staged replacement can be staged over only after the lease, and the revocation of an unreported delegation
// is handed out only after it. Twice the time the KQP orchestrator of the statement gives one delegation call.
constexpr TDuration StagedIamDelegationLease = TDuration::Minutes(10);

// The outbox of delegation revocations: a delegation its secret no longer names (dropped, alone or with its
// directory or subdomain; a promoted-over or cancelled replacement). The record is written in the transaction
// that makes the change and lives in the local database until the node that claimed it reports that IAM
// accepted the revocation. The schemeshard only keeps the records and hands them out
// (TEvSchemeShard::TEvClaimIamDelegationRevocations, TEvIamDelegationsRevoked); the revoking is done on a node.
//
// The one exception is the drop of a database (an extsubdomain): its schemeshard is deleted with the database,
// secrets and outbox included. The cloud revokes those delegations with the other resources of the database.
struct TIamDelegationRevocation {
    TString ReferrerId;
    TString ServiceAccountId;
    TString CloudId;
    TPathId PathId; // the secret, for logs
    TInstant NotBefore; // when the setup of the delegation can no longer be in flight

    TString ToString() const {
        return TStringBuilder() << "{referrer: " << ReferrerId << ", sa: " << ServiceAccountId << ", cloud: " << CloudId
            << ", secret: " << PathId << ", not before: " << NotBefore << "}";
    }
};

// The current delegation of the secret and the staged replacement, if any (PathId is not set), each with the
// time its revocation may be handed out: at once for a delegation whose setup was reported, after the lease
// otherwise
TVector<TIamDelegationRevocation> NamedIamDelegations(const NKikimrSchemeOp::TSecretDescription& secret);

} // namespace NKikimr::NSchemeShard
