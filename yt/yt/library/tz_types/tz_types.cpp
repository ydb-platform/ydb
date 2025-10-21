#include "tz_types.h"

namespace NYT::NTzTypes {

////////////////////////////////////////////////////////////////////////////////

int GetMaxPossibleTzStringSize()
{
    return Singleton<TTzRegistry>()->GetMaxPossibleTzSize() + sizeof(i64);
}

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTzTypes
