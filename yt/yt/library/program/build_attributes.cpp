#include "build_attributes.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/misc/error_code.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Build");

////////////////////////////////////////////////////////////////////////////////

void TBuildInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name)
        .Default();

    registrar.Parameter("version", &TThis::Version)
        .Default(GetVersion());

    registrar.Parameter("build_host", &TThis::BuildHost)
        .Default(GetBuildHost());

    registrar.Parameter("build_time", &TThis::BuildTime)
        .Default(ParseBuildTime());

    registrar.Parameter("start_time", &TThis::StartTime)
        .Default(TInstant::Now());
}

std::optional<TInstant> TBuildInfo::ParseBuildTime()
{
    TString rawBuildTime(GetBuildTime());

    // Build time may be empty if code is building
    // without -DBUILD_DATE (for example, in opensource build).
    if (rawBuildTime.empty()) {
        return std::nullopt;
    }

    try {
        return TInstant::ParseIso8601(rawBuildTime);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error parsing build time");
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

TBuildInfoPtr BuildBuildAttributes(const char* serviceName)
{
    auto info = New<TBuildInfo>();
    if (serviceName) {
        info->Name = serviceName;
    }
    return info;
}

void SetBuildAttributes(IYPathServicePtr orchidRoot, const char* serviceName)
{
    SyncYPathSet(
        orchidRoot,
        "/service",
        BuildYsonStringFluently()
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(BuildBuildAttributes(serviceName)));
    SyncYPathSet(
        orchidRoot,
        "/error_codes",
        BuildYsonStringFluently()
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .DoMapFor(TErrorCodeRegistry::Get()->GetAllErrorCodes(), [] (TFluentMap fluent, const auto& pair) {
                fluent
                    .Item(ToString(pair.first)).BeginMap()
                        .Item("cpp_literal").Value(ToString(pair.second))
                    .EndMap();
            }));
    SyncYPathSet(
        orchidRoot,
        "/error_code_ranges",
        BuildYsonStringFluently()
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .DoMapFor(TErrorCodeRegistry::Get()->GetAllErrorCodeRanges(), [] (TFluentMap fluent, const TErrorCodeRegistry::TErrorCodeRangeInfo& range) {
                fluent
                    .Item(ToString(range)).BeginMap()
                        .Item("cpp_enum").Value(range.Namespace)
                    .EndMap();
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

