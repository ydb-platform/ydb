#include "config.h"

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const auto DefaultTraceSamplePeriod = TDuration::MicroSeconds(1000);
const auto DefaultPBufferReplyTimeout = TDuration::MicroSeconds(50000);
const auto DefaultHedgingDelay = TDuration::MicroSeconds(700);
const auto DefaultWriteRequestTimeout = TDuration::MilliSeconds(10000);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TStorageConfig::TStorageConfig(
    const NProto::TStorageServiceConfig& storageServiceConfig)
    : StorageServiceConfig(storageServiceConfig)
{}

////////////////////////////////////////////////////////////////////////////////

// clang-format off
#define BLOCKSTORE_STORAGE_CONFIG_RO(xxx)                                     \
    xxx(SyncRequestsBatchSize,              ui32,     3                       )\
    xxx(StripeSize,                         ui64,     512_KB                  )\
    xxx(DDiskPoolName,                      TString,  "ddp1"                  )\
    xxx(PersistentBufferDDiskPoolName,      TString,  "ddp1"                  )\
    xxx(WriteMode,                                                             \
        NProto::EWriteMode,                                                    \
        NProto::DirectPBuffersFilling)                                         \
    xxx(VChunkSize,                         ui64,     128_MB                  )\
    xxx(ThreadPoolSize,                     ui32,     2                       )\

// BLOCKSTORE_STORAGE_CONFIG_RO
// clang-format on

#define BLOCKSTORE_STORAGE_CONFIG(xxx) \
    BLOCKSTORE_STORAGE_CONFIG_RO(xxx)  \
    // BLOCKSTORE_STORAGE_CONFIG

#define BLOCKSTORE_STORAGE_DECLARE_CONFIG(name, type, value)  \
    Y_DECLARE_UNUSED static const type Default##name = value; \
    // BLOCKSTORE_STORAGE_DECLARE_CONFIG

BLOCKSTORE_STORAGE_CONFIG(BLOCKSTORE_STORAGE_DECLARE_CONFIG)

#undef BLOCKSTORE_STORAGE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
}

template <>
TVector<TString> ConvertValue(
    const google::protobuf::RepeatedPtrField<TString>& value)
{
    return TVector<TString>(value.begin(), value.end());
}

template <>
TString ConvertValue<TString, TString>(const TString& value)
{
    return value;
}

#define CONFIG_ITEM_IS_SET_CHECKER(name, ...)               \
    template <typename TProto>                              \
    [[nodiscard]] bool Is##name##Set(const TProto& proto)   \
    {                                                       \
        if constexpr (requires() { proto.name##Size(); }) { \
            return proto.name##Size() > 0;                  \
        } else {                                            \
            return proto.Has##name();                       \
        }                                                   \
    }

BLOCKSTORE_STORAGE_CONFIG(CONFIG_ITEM_IS_SET_CHECKER);

#undef CONFIG_ITEM_IS_SET_CHECKER

#define BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(config, name, type, value) \
    (Is##name##Set(config) ? ConvertValue<type>(config.Get##name()) : value)

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)  \
    type TStorageConfig::Get##name() const         \
    {                                              \
        return BLOCKSTORE_CONFIG_GET_CONFIG_VALUE( \
            StorageServiceConfig,                  \
            name,                                  \
            type,                                  \
            Default##name);                        \
    }

BLOCKSTORE_STORAGE_CONFIG_RO(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

////////////////////////////////////////////////////////////////////////////////

EWriteMode GetWriteModeFromProto(NProto::EWriteMode writeMode)
{
    switch (writeMode) {
        case NProto::EWriteMode::PBufferReplication:
            return EWriteMode::PBufferReplication;
        case NProto::EWriteMode::DirectPBuffersFilling:
            return EWriteMode::DirectPBuffersFilling;
        default:
            break;
    }
    Y_ABORT_UNLESS(false);
}

NProto::EWriteMode GetProtoWriteMode(EWriteMode writeMode)
{
    switch (writeMode) {
        case EWriteMode::PBufferReplication:
            return NProto::EWriteMode::PBufferReplication;
        case EWriteMode::DirectPBuffersFilling:
            return NProto::EWriteMode::DirectPBuffersFilling;
    }
}

////////////////////////////////////////////////////////////////////////////////

TDuration TStorageConfig::GetTraceSamplePeriod() const
{
    return StorageServiceConfig.HasTraceSamplePeriod()
               ? TDuration::MilliSeconds(
                     StorageServiceConfig.GetTraceSamplePeriod())
               : DefaultTraceSamplePeriod;
}

TDuration TStorageConfig::GetWriteHedgingDelay() const
{
    return StorageServiceConfig.HasWriteHedgingDelay()
               ? TDuration::MicroSeconds(
                     StorageServiceConfig.GetWriteHedgingDelay())
               : DefaultHedgingDelay;
}

TDuration TStorageConfig::GetWriteRequestTimeout() const
{
    return StorageServiceConfig.HasWriteRequestTimeout()
               ? TDuration::MilliSeconds(
                     StorageServiceConfig.GetWriteRequestTimeout())
               : DefaultWriteRequestTimeout;
}

TDuration TStorageConfig::GetPBufferReplyTimeout() const
{
    return StorageServiceConfig.HasPBufferReplyTimeoutMicroseconds()
               ? TDuration::MicroSeconds(
                     StorageServiceConfig.GetPBufferReplyTimeoutMicroseconds())
               : DefaultPBufferReplyTimeout;
}

}   // namespace NYdb::NBS::NBlockStore
