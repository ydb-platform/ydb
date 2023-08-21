#include "check_yson_token.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NComplexTypes {

using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void ThrowUnexpectedYsonTokenException(
    const TComplexTypeFieldDescriptor& descriptor,
    const TYsonPullParserCursor& cursor,
    const std::vector<EYsonItemType>& expected)
{
    ThrowUnexpectedYsonTokenException(descriptor.GetDescription(), cursor, expected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
