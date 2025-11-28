#pragma once

#include "utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>


class TValueGenerator {
public:
    TValueGenerator(const TCommonOptions& opts, std::uint32_t startId = 0);
    TRecordData Get();
    TDuration GetComputeTime() const;

private:
    const TCommonOptions& Opts;
    std::uint32_t CurrentObjectId = 0;
    TDuration ComputeTime;
};

class TKeyValueGenerator {
public:
    TKeyValueGenerator(const TCommonOptions& opts, std::uint32_t startId = 0);
    TKeyValueRecordData Get();
    TDuration GetComputeTime() const;

private:
    const TCommonOptions& Opts;
    std::uint32_t CurrentObjectId = 0;
    TDuration ComputeTime;
};

template<typename TGeneratorType, typename TRecordType>
class TPackGenerator {
public:
    TPackGenerator(
        const TCommonOptions& opts,
        std::uint32_t packSize,
        NYdb::TValue(*buildValueFromRecordFunc)(const TRecordType&),
        std::uint64_t remain,
        std::uint32_t startId = 0
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
            std::uint32_t specialId = GetSpecialId(GetHash(record.ObjectId));
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

    std::uint32_t GetPackSize() const {
        return PackSize;
    }

    TDuration GetComputeTime() const {
        return Generator.GetComputeTime();
    }

    std::uint64_t GetRemain() const {
        return Remain;
    }

private:
    NYdb::TValue(*BuildValueFromRecordFunc)(const TRecordType&);
    TGeneratorType Generator;
    std::uint32_t PackSize;
    std::unordered_map<std::uint32_t, std::vector<NYdb::TValue>> Packs;
    std::uint64_t Remain;
};
