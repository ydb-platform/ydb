#pragma once

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

struct TProjectSettings {
    struct TPragma {
        TString Name;           // e.g. "yt.StaticPool"
        TVector<TString> Value; // multi-value
    };

    TVector<TPragma> Pragmas;
    TString DefaultCluster;
    TMaybe<TLangVersion> LangVer;
    NYT::TNode Attributes = NYT::TNode::CreateMap();

    void AddPragma(TString name, TVector<TString> values);

    void AddPragma(TString name, TString value);

    // Returns the first pragma with the given name or nullptr when not found.
    const TPragma* FindPragma(TStringBuf name) const;
};

} // namespace NYql
