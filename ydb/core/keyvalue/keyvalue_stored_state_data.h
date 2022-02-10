#pragma once
#include "defs.h"
#include "keyvalue_data_header.h"
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NKeyValue {

#pragma pack(push, 1)
struct TKeyValueStoredStateData {
    TDataHeader DataHeader;
    ui64 Version;
    ui64 UserGeneration;

    ui64 CollectGeneration;
    ui64 CollectStep;
    ui64 ChannelGeneration;
    ui64 ChannelStep;

    ui64 GetVersion() const {
        return ReadUnaligned<ui64>(&Version);
    }
    ui64 GetUserGeneration() const {
        return ReadUnaligned<ui64>(&UserGeneration);
    }
    ui64 GetCollectGeneration() const {
        return ReadUnaligned<ui64>(&CollectGeneration);
    }
    ui64 GetCollectStep() const {
        return ReadUnaligned<ui64>(&CollectStep);
    }
    ui64 GetChannelGeneration() const {
        return ReadUnaligned<ui64>(&ChannelGeneration);
    }
    ui64 GetChannelStep() const {
        return ReadUnaligned<ui64>(&ChannelStep);
    }
    void SetVersion(ui64 value) {
        WriteUnaligned<ui64>(&Version, value);
    }
    void SetUserGeneration(ui64 value) {
        WriteUnaligned<ui64>(&UserGeneration, value);
    }
    void SetCollectGeneration(ui64 value) {
        WriteUnaligned<ui64>(&CollectGeneration, value);
    }
    void SetCollectStep(ui64 value) {
        WriteUnaligned<ui64>(&CollectStep, value);
    }
    void SetChannelGeneration(ui64 value) {
        WriteUnaligned<ui64>(&ChannelGeneration, value);
    }
    void SetChannelStep(ui64 value) {
        WriteUnaligned<ui64>(&ChannelStep, value);
    }

    TKeyValueStoredStateData();
    void Clear();
    void UpdateChecksum();
    bool CheckChecksum() const;
};
#pragma pack(pop)

} // NKeyValue
} // NKikimr
