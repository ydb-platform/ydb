#pragma once
 
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <functional>

namespace NYql {

class TGatewaysConfig;

const TStringBuf ConfReadName = "ConfRead!";

using TAllowSettingPolicy = std::function<bool(TStringBuf settingName)>;

TIntrusivePtr<IDataProvider> CreateConfigProvider(TTypeAnnotationContext& types, const TGatewaysConfig* config,
    const TAllowSettingPolicy& policy = TAllowSettingPolicy()); // allow all settings by default

const THashSet<TStringBuf>& ConfigProviderFunctions();

} // namespace NYql 
