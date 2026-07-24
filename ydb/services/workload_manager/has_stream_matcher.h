#pragma once

#include <ydb/core/kqp/common/kqp_user_request_context.h>

#include <optional>


namespace NKikimr::NWorkloadManager {

/// Returns true if the classifier should accept the query based on the HAS_STREAM filter.
bool MatchesStream(const std::optional<bool>& hasStream,
                   const NKqp::TUserRequestContext& userRequestContext);

}  // namespace NKikimr::NWorkloadManager
