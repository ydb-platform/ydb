#pragma once
#include <yt/yt/client/api/public.h>
#include <ydb/library/yql/providers/abstract/object_kind.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <library/cpp/threading/future/future.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql {
NYT::NApi::IClientPtr CreateYtClient(const TString& endpoint, const TString& token);
NThreading::TFuture<bool> IsYtQueue(const NYT::NApi::IClientPtr& client, const TString& path);

NThreading::TFuture<NFq::TExternalObjectKindResult> GetYtObjectType(
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    const TString& endpoint,
    const TString& structuredToken,
    const TString& path);
}
