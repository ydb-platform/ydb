#pragma once
#include <util/generic/string.h>

namespace NYdb::NUdfManifest {

enum class EModuleType { Module,
                         Library };
enum class EModuleKind { Wasm,
                         Native };

struct TManifest {
    TString Name;
    EModuleType Type;
    EModuleKind Kind;
    TString Extension = "wasm";
};

// Validates common fields and field applicability. Throws yexception on invalid JSON.
TManifest Parse(TStringBuf json);

} // namespace NYdb::NUdfManifest
