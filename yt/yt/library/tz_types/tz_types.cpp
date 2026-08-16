#include "tz_types.h"

#include <library/cpp/type_info/tz/tz.h>

namespace NYT::NTzTypes {

////////////////////////////////////////////////////////////////////////////////

std::string_view GetTzName(int tzIndex)
{
    return Singleton<TTzRegistry>()->GetTzName(tzIndex);
}

int GetTzIndex(std::string_view tzName)
{
    return Singleton<TTzRegistry>()->GetTzIndex(tzName);
}

void ValidateTzName(std::string_view tzName)
{
    Singleton<TTzRegistry>()->ValidateTzName(tzName);
}

void ValidateTzId(ui16 tzId)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        NTi::IsValidTimezoneIndex(tzId), "Invalid timezone id: %v", tzId);
}

int GetTimezonesSize()
{
    return std::ssize(NTi::GetTimezones());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTzTypes
