#include "helpers.h"

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

void ValidateMaintenanceComment(const TString& comment)
{
    constexpr int MaxMaintenanceCommentLength = 512;

    if (comment.size() > MaxMaintenanceCommentLength) {
        THROW_ERROR_EXCEPTION("Maintenance comment is too long")
            << TErrorAttribute("comment_length", comment.size())
            << TErrorAttribute("max_comment_length", MaxMaintenanceCommentLength);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
