#pragma once

#include "workload_query_gen.h"

#include <cctype>

struct TStockWorkloadParams : public TWorkloadParams {
    bool PartitionsByLoad;
};

class TStockWorkloadGenerator : public IWorkloadQueryGenerator {
public:

    static TStockWorkloadGenerator* New(const TStockWorkloadParams* params) {
        if (!validateDbPath(params->DbPath)) {
            return nullptr;
        }
        return new TStockWorkloadGenerator(params);
    }

    virtual ~TStockWorkloadGenerator() {}

    std::string GetDDLQueries() override {
        static const char TablesDdl[] = R"(--!syntax_v1
            CREATE TABLE `%s/stock`(product Utf8, quantity Int64, PRIMARY KEY(product)) %s;
            CREATE TABLE `%s/orders`(id Uint64, customer Utf8, created Datetime, processed Datetime, PRIMARY KEY(id));
            CREATE TABLE `%s/orderLines`(id_order Uint64, product Utf8, quantity Int64, PRIMARY KEY(id_order, product));
            )";
        static const char PartitionsDdl[] = R"(WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT	= 3))";
        char buf[sizeof(TablesDdl) + sizeof(PartitionsDdl) + 8192*3]; // 32*256 for DbPath

        int res = std::sprintf(buf, TablesDdl, DbPath.c_str(), (PartitionsByLoad ? PartitionsDdl : ""), DbPath.c_str(), DbPath.c_str());
        if (res < 0) {
            return "";
        }
        return buf;
    }

private:

    TStockWorkloadGenerator(const TStockWorkloadParams* params) {
        DbPath = params->DbPath;
        PartitionsByLoad = params->PartitionsByLoad;
    }

    static bool validateDbPath(const std::string& path) {
        for (size_t i = 0; i < path.size(); ++i) {
            char c = path[i];
            if (!std::isalnum(c) && c != '/' && c != '_' && c != '-') {
                return false;
            }
        }
        return true;
    }

    std::string DbPath;
    bool PartitionsByLoad;
};
