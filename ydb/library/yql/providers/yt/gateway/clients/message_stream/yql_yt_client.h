#pragma once
#include <library/cpp/threading/future/future.h>
#include <util/generic/string.h>
#include <yt/yt/client/api/public.h>

namespace NYql {
NYT::NApi::IClientPtr CreateYtClient(const TString& endpoint, const TString& token);
NThreading::TFuture<bool> IsYtQueue(const NYT::NApi::IClientPtr& client, const TString& path);
}   // namespace NYql
