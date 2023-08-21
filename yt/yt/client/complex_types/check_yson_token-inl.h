#ifndef CHECK_YSON_TOKEN_INL_H_
#error "Direct inclusion of this file is not allowed, include check_yson.h"
// For the sake of sane code completion.
#include "check_yson_token-inl.h"
#endif

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void EnsureYsonToken(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const NYson::TYsonPullParserCursor& cursor,
    NYson::EYsonItemType expected)
{
    if (expected != cursor->GetType()) {
        ThrowUnexpectedYsonTokenException(descriptor, cursor, {expected});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
