#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

namespace NTestUtils {

constexpr TStringBuf TEST_CONTENT =
    R"({"key": "1", "value": "trololo"}
       {"key": "2", "value": "hello world"})"sv;

constexpr TStringBuf TEST_CONTENT_KEYS =
    R"({"key": "1"}
       {"key": "3"})"sv;

extern const TString TEST_SCHEMA;
extern const TString TEST_SCHEMA_IDS;

std::shared_ptr<NKikimr::NKqp::TKikimrRunner> MakeKikimrRunner(std::optional<NKikimrConfig::TAppConfig> appConfig = std::nullopt, const TString& domainRoot = "Root");

} // namespace NTestUtils
