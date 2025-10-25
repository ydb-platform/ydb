#pragma once

#include <yt/yql/providers/yt/common/yql_yt_settings.h>

#include <util/generic/string.h>

namespace NYql {

inline const TString YtGateway_FileDumpPath = "YtGateway_FileDumpPath";

TString MakeDumpKey(const TStringBuf& name, const TStringBuf& cluster);
TString MakeDumpPath(const TString& srcPath, const TString& cluster, const TYqlOperationOptions& opOptions, const TYtSettings::TConstPtr& configuration, bool out = false);

TMaybe<TString> GetDumpPath(const TStringBuf& name, const TStringBuf& cluster, const TString& component, const TQContext& qContext);

} // namespace NYql
