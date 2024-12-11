#include "versioned_io_options.h"

#include <yt_proto/yt/client/table_client/proto/versioned_io_options.pb.h>

namespace NYT::NTableClient {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TVersionedReadOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("read_mode", &TThis::ReadMode)
        .Default(EVersionedIOMode::Default);
}

void TVersionedWriteOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("write_mode", &TThis::WriteMode)
        .Default(EVersionedIOMode::Default);
}

void ToProto(
    NProto::TVersionedReadOptions* protoOptions,
    const TVersionedReadOptions& options)
{
    protoOptions->set_read_mode(ToProto(options.ReadMode));
}

void FromProto(
    TVersionedReadOptions* options,
    const NProto::TVersionedReadOptions& protoOptions)
{
    options->ReadMode = FromProto<EVersionedIOMode>(protoOptions.read_mode());
}

void ToProto(
    NProto::TVersionedWriteOptions* protoOptions,
    const NTableClient::TVersionedWriteOptions& options)
{
    protoOptions->set_write_mode(ToProto(options.WriteMode));
}

void FromProto(
    NTableClient::TVersionedWriteOptions* options,
    const NProto::TVersionedWriteOptions& protoOptions)
{
    FromProto(&options->WriteMode, protoOptions.write_mode());
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
