#pragma once

#include <ydb/core/base/event_filter.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <library/cpp/getopt/small/last_getopt_opts.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NKikimr::NConfig {

struct TConfigItemInfo {
    enum class EUpdateKind {
        MutableConfigPartFromFile,
        MutableConfigPartFromBaseConfig,
        MutableConfigPartMergeFromFile,
        ReplaceConfigWithConsoleYaml,
        ReplaceConfigWithConsoleProto,
        ReplaceConfigWithBase,
        LoadYamlConfigFromFile,
        SetExplicitly,
        UpdateExplicitly,
    };

    struct TUpdate {
        const char* File;
        ui32 Line;
        EUpdateKind Kind;
    };

    TVector<TUpdate> Updates;
};

struct TDenyList {
    std::set<ui32> Items;
};

struct TAllowList {
    std::set<ui32> Items;
};

struct TDebugInfo {
    NKikimrConfig::TAppConfig StaticConfig;
    NKikimrConfig::TAppConfig InitialCmsConfig;
    NKikimrConfig::TAppConfig InitialCmsYamlConfig;
    THashMap<ui32, TConfigItemInfo> InitInfo;
};

struct TConfigsDispatcherInitInfo {
    NKikimrConfig::TAppConfig InitialConfig;
    TMap<TString, TString> Labels;
    std::variant<std::monostate, TDenyList, TAllowList> ItemsServeRules;
    std::optional<TDebugInfo> DebugInfo;
};

} // NKikimr::NConfig
