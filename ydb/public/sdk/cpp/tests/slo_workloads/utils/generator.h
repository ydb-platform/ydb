#pragma once

#include "utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>


class TValueGenerator {
public:
    TValueGenerator(const TCommonOptions& opts, ui32 startId = 0);
    TRecordData Get();
    TDuration GetComputeTime() const;

private:
    const TCommonOptions& Opts;
    ui32 CurrentObjectId = 0;
    TDuration ComputeTime;
};

class TKeyValueGenerator {
public:
    TKeyValueGenerator(const TCommonOptions& opts, ui32 startId = 0);
    TKeyValueRecordData Get();
    TDuration GetComputeTime() const;

private:
    const TCommonOptions& Opts;
    ui32 CurrentObjectId = 0;
    TDuration ComputeTime;
};

template<typename TGeneratorType, typename TRecordType>
class TPackGenerator {
public:
    TPackGenerator(
        const TCommonOptions& opts,
        ui32 packSize,
        NYdb::TValue(*buildValueFromRecordFunc)(const TRecordType&),
        ui64 remain,
        ui32 startId = 0
    )
        : BuildValueFromRecordFunc(buildValueFromRecordFunc)
        , Generator(opts, startId)
        , PackSize(packSize)
        , Remain(remain)
    {
    }

    // Returns One-shard pack
    bool GetNextPack(std::vector<NYdb::TValue>& pack) {
        pack.clear();
        while (Remain) {
            TRecordType record = Generator.Get();
            --Remain;
            ui32 specialId = GetSpecialId(GetHash(record.ObjectId));
            auto& existingPack = Packs[specialId];
            existingPack.emplace_back(BuildValueFromRecordFunc(record));
            if (existingPack.size() >= PackSize) {
                existingPack.swap(pack);
                return true;
            }
        }
        for (auto& it : Packs) {
            if (it.second.size()) {
                it.second.swap(pack);
                return true;
            }
        }
        return false;
    }

    ui32 GetPackSize() const {
        return PackSize;
    }

    TDuration GetComputeTime() const {
        return Generator.GetComputeTime();
    }

    ui64 GetRemain() const {
        return Remain;
    }

private:
    NYdb::TValue(*BuildValueFromRecordFunc)(const TRecordType&);
    TGeneratorType Generator;
    ui32 PackSize;
    std::unordered_map<ui32, std::vector<NYdb::TValue>> Packs;
    ui64 Remain;
};
