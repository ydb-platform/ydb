#pragma once

#include <util/generic/fwd.h>

#include <vector>
#include <unordered_map>

namespace NYdb::NTPCC {

class TDataSplitter {
public:
    TDataSplitter(int warehouseCount)
        : WarehouseCount(warehouseCount)
    {
    }

    std::vector<int> GetSplitKeys(const TString& table) const;

    // return warehouse ids in string form, e.g. "1,5,10,1000"
    TString GetSplitKeysString(const TString& table) const;

    // Calculate minimum partitions based on warehouse count
    static int CalcMinParts(int warehouseCount);

    static double GetPerWarehouseMB(const TString& table);

    // Expose internal constants for testing
    static int GetDefaultMinPartitions();
    static int GetDefaultMinWarehousesPerShard();
    static int GetMinItemsPerShard();
    static const std::unordered_map<TString, double>& GetPerWarehouseMBMap();

private:
    int WarehouseCount;
};

} // namespace NYdb::NTPCC
