#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto DateLength = std::char_traits<char>::length("YYYY-MM-DD");
static constexpr auto DateTimeLength = std::char_traits<char>::length("YYYY-MM-DDThh:mm:ssZ");
static constexpr auto TimestampLength = std::char_traits<char>::length("YYYY-MM-DDThh:mm:ss.123456Z");

ui64 BinaryTimeFromText(TStringBuf data, NTableClient::ESimpleLogicalValueType valueType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
