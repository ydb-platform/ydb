#pragma once

#include <ydb/core/kqp/common/kqp_user_request_context.h>

#include <optional>


namespace NKikimr::NKqp::NWorkload {

/// Returns true if the classifier should accept the query based on the HAS_STREAM filter.
bool MatchesStream(const std::optional<bool>& hasStream,
                   const TUserRequestContext& userRequestContext);

}  // namespace NKikimr::NKqp::NWorkload
