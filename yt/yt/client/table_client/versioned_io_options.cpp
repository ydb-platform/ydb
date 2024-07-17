#include "versioned_io_options.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void TVersionedReadOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("read_mode", &TThis::ReadMode)
        .Default(EVersionedIOMode::Default);
}

std::optional<TString> GetTimestampColumnOriginalNameOrNull(TStringBuf name)
{
    auto prefixEnd = name.begin() + ssize(TimestampColumnPrefix);
    return ssize(name) >= ssize(TimestampColumnPrefix) && std::equal(name.begin(), prefixEnd, TimestampColumnPrefix.begin())
        ? TString(prefixEnd, name.end())
        : std::optional<TString>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
