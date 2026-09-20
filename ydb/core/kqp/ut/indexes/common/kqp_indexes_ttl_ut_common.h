#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <string>

namespace NKikimr::NKqp {

struct TTtlNotAllowedIndexTestConfig {
    std::string TextColumnType = "String";
    std::string IndexInCreateTable;
    std::string AlterAddIndex;
    std::string ExpectedError;
};

void TestTtlNotAllowedBoth(NYdb::NQuery::TQueryClient db, const TTtlNotAllowedIndexTestConfig& config);
void TestTtlNotAllowedAlterTtl(NYdb::NQuery::TQueryClient db, const TTtlNotAllowedIndexTestConfig& config);
void TestTtlNotAllowedAlterIndex(NYdb::NQuery::TQueryClient db, const TTtlNotAllowedIndexTestConfig& config);
void TestTtlNotAllowedAlterTtlIndex(NYdb::NQuery::TQueryClient db, const TTtlNotAllowedIndexTestConfig& config);
void TestTtlNotAllowedAlterIndexTtl(NYdb::NQuery::TQueryClient db, const TTtlNotAllowedIndexTestConfig& config);

} // namespace NKikimr::NKqp
