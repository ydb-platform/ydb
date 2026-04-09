#include "config.h"

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

TStorageConfig::TStorageConfig(
    NProto::TStorageServiceConfig storageServiceConfig)
    : StorageServiceConfig(std::move(storageServiceConfig))
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
        NProto::PBufferReplication)                                            \

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

TDuration TStorageConfig::GetTraceSamplePeriod() const
{
    return StorageServiceConfig.HasTraceSamplePeriod()
               ? TDuration::MilliSeconds(
                     StorageServiceConfig.GetTraceSamplePeriod())
               : TDuration::MicroSeconds(1000);
}

TDuration TStorageConfig::GetWriteHandoffDelay() const
{
    return StorageServiceConfig.HasWriteHandoffDelay()
               ? TDuration::MicroSeconds(
                     StorageServiceConfig.GetWriteHandoffDelay())
               : TDuration::MicroSeconds(700);
}

TDuration TStorageConfig::GetPBufferReplyTimeout() const
{
    return StorageServiceConfig.HasPBufferReplyTimeoutMicroseconds()
               ? TDuration::MicroSeconds(
                     StorageServiceConfig.GetPBufferReplyTimeoutMicroseconds())
               : TDuration::MicroSeconds(50000);
}

}   // namespace NYdb::NBS::NStorage
