#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/getopt/last_getopt.h>

struct TItem {
    std::string Id;
    std::string Document;
    std::vector<float> Embedding;
};

struct TResultItem {
    std::string Id;
    std::string Document;
    float Score;
};

void DropVectorTable(NYdb::NQuery::TQueryClient& client, const std::string& tableName);

void CreateVectorTable(NYdb::NQuery::TQueryClient& client, const std::string& tableName);

void InsertItems(
    NYdb::NQuery::TQueryClient& client,
    const std::string& tableName,
    const std::vector<TItem>& items);

void AddIndex(
    NYdb::TDriver& driver,
    NYdb::NQuery::TQueryClient& client,
    const std::string& database,
    const std::string& tableName,
    const std::string& indexName,
    const std::string& strategy,
    std::uint64_t dim,
    std::uint64_t levels,
    std::uint64_t clusters);

std::vector<TResultItem> SearchItems(
    NYdb::NQuery::TQueryClient& client,
    const std::string& tableName,
    const std::vector<float>& embedding,
    const std::string& strategy,
    std::uint64_t limit,
    const std::optional<std::string>& indexName = std::nullopt);
