#pragma once

#include "url_tree.h"

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <optional>

namespace NActors {

struct TUrlPattern {
    TString Path;
    std::optional<TString> ParamKey;
    std::optional<TString> ParamValue;

    TUrlPattern(TString path,
                std::optional<TString> key = std::nullopt,
                std::optional<TString> value = std::nullopt)
        : Path(std::move(path))
        , ParamKey(std::move(key))
        , ParamValue(std::move(value))
    {}
};

class TUrlTree {
public:
    void AddPattern(const TUrlPattern& rule);
    bool Match(const TString& url, const TCgiParameters& params) const;
    bool Match(const TString& url, const TString& params = "") const;

private:
    struct TUrlTreeParam {
        TString Key;
        std::optional<TString> Value;
    };

    struct TUrlTreeNode {
        THashMap<TString, THolder<TUrlTreeNode>> Children;
        bool MatchedWithoutParams = false;
        TVector<TUrlTreeParam> MatchedParams;
    };

    THolder<TUrlTreeNode> Root = MakeHolder<TUrlTreeNode>();
};

}
