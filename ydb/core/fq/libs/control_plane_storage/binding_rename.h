#pragma once

#include "events/events.h"

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/util/exceptions.h>

#include <google/protobuf/util/message_differencer.h>

namespace NFq {

using TBindingRename = FederatedQuery::Internal::BindingRename;

inline bool HasPendingBindingRename(const FederatedQuery::Internal::BindingInternal& binding) {
    return binding.has_rename() && binding.rename().state() != TBindingRename::FINISHED;
}

inline bool MatchesBindingRename(const TBindingRename& rename, const FederatedQuery::ModifyBindingRequest& request, const TString& user) {
    return rename.user() == user && rename.idempotency_key() == request.idempotency_key() &&
           (request.previous_revision() <= 0 || request.previous_revision() == rename.source_revision()) &&
           google::protobuf::util::MessageDifferencer::Equals(rename.destination(), request.content());
}

enum class EBindingRenameWrite {
    None,
    Journal,
    Binding,
    Forget,
};

inline EBindingRenameWrite AdvanceBindingRename(TBindingRename& rename, EBindingRenameAction action, const TString& operationId,
    const TString& identity, const FederatedQuery::Binding& binding) {
    if (operationId.empty() || rename.operation_id() != operationId) {
        ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "Binding rename operation has changed";
    }
    switch (action) {
        case EBindingRenameAction::RecordSource:
            if (rename.state() != TBindingRename::PREPARED) {
                return EBindingRenameWrite::None;
            }
            if (identity.empty() || (!rename.source_identity().empty() && rename.source_identity() != identity)) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "Binding rename source has changed";
            }
            if (rename.source_identity().empty()) {
                rename.set_source_identity(identity);
                return EBindingRenameWrite::Journal;
            }
            return EBindingRenameWrite::None;
        case EBindingRenameAction::Commit:
            if (identity.empty() || (!rename.candidate_identity().empty() && rename.candidate_identity() != identity)) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "Binding rename candidate has changed";
            }
            if (rename.state() != TBindingRename::PREPARED) {
                return EBindingRenameWrite::None;
            }
            if (rename.source_identity().empty() || binding.meta().revision() != rename.source_revision() ||
                binding.content().name() != rename.source_name()) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "Binding rename source has changed";
            }
            rename.set_candidate_identity(identity);
            rename.set_state(TBindingRename::COMMITTED);
            return EBindingRenameWrite::Binding;
        case EBindingRenameAction::Abort:
            if (rename.state() != TBindingRename::PREPARED) {
                return EBindingRenameWrite::None;
            }
            rename.set_state(TBindingRename::ABORTED);
            return EBindingRenameWrite::Journal;
        case EBindingRenameAction::Finish:
            switch (rename.state()) {
                case TBindingRename::COMMITTED:
                    rename.set_state(TBindingRename::FINISHED);
                    return rename.idempotency_key().empty() ? EBindingRenameWrite::Forget : EBindingRenameWrite::Journal;
                case TBindingRename::ABORTED:
                    return EBindingRenameWrite::Forget;
                case TBindingRename::FINISHED:
                    return EBindingRenameWrite::None;
                default:
                    ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "Binding rename has no durable decision";
            }
        case EBindingRenameAction::None:
        case EBindingRenameAction::Prepare:
            Y_ABORT("Unexpected binding rename action");
    }
    Y_ABORT("Unknown binding rename action");
}

}   // namespace NFq
