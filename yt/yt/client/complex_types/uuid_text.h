#include <yt/yt/core/misc/guid.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t UuidYqlTextSize = 36;
constexpr size_t UuidYtTextSize = 35;
constexpr size_t UuidBinarySize = 16;

void TextYqlUuidToBytes(TStringBuf uuid, char* ptr);
char* TextYqlUuidFromBytes(TStringBuf bytes, char* ptr);

void GuidToBytes(TGuid guid, char* ptr);
TGuid GuidFromBytes(TStringBuf bytes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
