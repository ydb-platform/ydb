#include "kqp_run_script_actor_impl.h"

namespace NKikimr::NKqp::NPrivate {

void TFinishInfo::Update(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
    if (!Status) {
        Status = status;
        Issues = std::move(issues);
        return;
    }

    if (*Status == Ydb::StatusIds::SUCCESS) {
        Status = status;
    }

    Issues.AddIssues(issues);

    return;
}

bool TFinishInfo::IsFinished() const {
    return Status.has_value();
}

bool TFinishInfo::IsSuccess() const {
    return Status.has_value() && *Status == Ydb::StatusIds::SUCCESS;
}

bool TFinishInfo::IsFailed() const {
    return Status.has_value() && *Status != Ydb::StatusIds::SUCCESS;
}

} // namespace NKikimr::NKqp::NPrivate
