#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/yson/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBuildInfo
    : public NYTree::TYsonStruct
{
public:
    std::optional<TString> Name;
    TString Version;
    TString BuildHost;
    std::optional<TInstant> BuildTime;
    TInstant StartTime;

    REGISTER_YSON_STRUCT(TBuildInfo);

    static void Register(TRegistrar registrar);

private:
    static std::optional<TInstant> ParseBuildTime();
};

DEFINE_REFCOUNTED_TYPE(TBuildInfo)

////////////////////////////////////////////////////////////////////////////////

//! Build build (pun intended) attributes as a TBuildInfo a-la /orchid/service. If service name is not provided,
//! it is omitted from the result.
TBuildInfoPtr BuildBuildAttributes(const char* serviceName = nullptr);

void SetBuildAttributes(NYTree::IYPathServicePtr orchidRoot, const char* serviceName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
