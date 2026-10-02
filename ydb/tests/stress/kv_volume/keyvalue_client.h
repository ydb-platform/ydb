#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>
#include <memory>

namespace NKvVolumeStress {

struct TOptions;

class IKeyValueClient {
public:
    using TStatusCallback = std::function<void(bool ok, TString error)>;
    using TReadCallback = std::function<void(bool ok, TString value, TString error)>;

    virtual ~IKeyValueClient() = default;

    virtual bool CreateVolume(
        const TString& path,
        ui32 partitionCount,
        const TVector<TString>& channels,
        TString* error) = 0;

    virtual bool DropVolume(const TString& path, TString* error) = 0;

    virtual bool Write(
        const TString& path,
        ui32 partitionId,
        const TVector<std::pair<TString, TString>>& kvPairs,
        ui32 channel,
        TString* error) = 0;

    virtual bool DeleteKey(const TString& path, ui32 partitionId, const TString& key, TString* error) = 0;

    virtual bool Read(
        const TString& path,
        ui32 partitionId,
        const TString& key,
        ui32 offset,
        ui32 size,
        TString* value,
        TString* error) = 0;

    virtual void WriteAsync(
        const TString& path,
        ui32 partitionId,
        const TVector<std::pair<TString, TString>>& kvPairs,
        ui32 channel,
        TStatusCallback done) = 0;

    virtual void DeleteKeyAsync(
        const TString& path,
        ui32 partitionId,
        const TString& key,
        TStatusCallback done) = 0;

    virtual void ReadAsync(
        const TString& path,
        ui32 partitionId,
        const TString& key,
        ui32 offset,
        ui32 size,
        TReadCallback done) = 0;
};

std::unique_ptr<IKeyValueClient> MakeKeyValueClient(const TString& hostPort, const TOptions& options);

} // namespace NKvVolumeStress
