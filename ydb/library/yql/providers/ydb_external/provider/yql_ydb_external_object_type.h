#pragma once

#include <ydb/library/yql/providers/abstract/object_kind.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/threading/future/future.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <functional>

namespace NYql::NYdbExternal {

using TDescribePathErrorHandler = std::function<void(const TString&, const TString&)>;

NThreading::TFuture<NFq::TExternalObjectKindResult> GetYdbObjectType(
    const std::shared_ptr<NYdb::TDriver>& driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    const TString& endpoint,
    const TString& database,
    bool useTls,
    const TString& structuredToken,
    const TString& path,
    bool requireMessageStream,
    TDescribePathErrorHandler onDescribePathError = {});

} // namespace NYql::NYdbExternal
