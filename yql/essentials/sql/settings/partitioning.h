#pragma once

#include "translation_settings.h"
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLTranslation {
    struct TBindingInfo {
        TString ClusterType;
        TString Cluster;
        TString Path;
        TString Schema;
        TMap<TString, TVector<TString>> Attributes;
    };

    // returns error message if any
    TString ExtractBindingInfo(const TTranslationSettings& settings, const TString& binding, TBindingInfo& result);
}  // namespace NSQLTranslation
