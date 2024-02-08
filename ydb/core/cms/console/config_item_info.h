#pragma once

#include <util/generic/vector.h>
#include <util/system/types.h>

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
