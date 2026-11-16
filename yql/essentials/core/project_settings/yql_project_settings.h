#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

// Project settings, mirrors the TProjectSettings proto message
// (yql/tools/yqlworker/interface/proto/task.proto).
struct TProjectSettings {
    struct TPragma {
        TString Name;           // e.g. "yt.StaticPool"
        TVector<TString> Value; // multi-value
    };

    TVector<TPragma> Pragmas;

    void AddPragma(TString name, TVector<TString> values);

    void AddPragma(TString name, TString value);

    // Returns the first pragma with the given name or nullptr when not found.
    const TPragma* FindPragma(TStringBuf name) const;
};

} // namespace NYql
