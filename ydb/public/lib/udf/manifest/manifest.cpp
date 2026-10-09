#include "manifest.h"
#include <library/cpp/json/json_reader.h>
#include <util/generic/yexception.h>
#include <util/string/strip.h>

namespace NYdb::NUdfManifest {
namespace {
void RejectAbiSelector(const NJson::TJsonValue& node, TStringBuf path) {
    if (node.IsMap()) {
        for (const auto& [key, value] : node.GetMap()) {
            const TString child = TString(path) + "." + key;
            if (key == "calling_convention") {
                ythrow yexception() << child << " is not supported: WASM UDFs use bridge";
            }
            RejectAbiSelector(value, child);
        }
    } else if (node.IsArray()) {
        for (const auto& value : node.GetArray()) {
            RejectAbiSelector(value, path);
        }
    }
}
} // namespace

TManifest Parse(TStringBuf json) {
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(json, &root, true) || !root.IsMap()) {
        ythrow yexception() << "Manifest must be a JSON object";
    }
    const auto required = [&root](TStringBuf key) -> TString {
        const auto& value = root[key];
        if (!value.IsString() || Strip(value.GetString()).empty()) {
            ythrow yexception() << "Manifest requires a non-empty string " << key;
        }
        return value.GetString();
    };
    RejectAbiSelector(root, "manifest");
    TManifest result;
    result.Name = required("module_name");
    if (result.Name != Strip(result.Name)) {
        ythrow yexception() << "module_name must not have leading or trailing whitespace";
    }
    const auto type = required("module_type");
    if (type == "module") {
        result.Type = EModuleType::Module;
    } else if (type == "library") {
        result.Type = EModuleType::Library;
    } else {
        ythrow yexception() << "module_type must be module or library";
    }
    const auto kind = required("module_kind");
    if (kind == "wasm") {
        result.Kind = EModuleKind::Wasm;
    } else if (kind == "native") {
        result.Kind = EModuleKind::Native;
    } else {
        ythrow yexception() << "module_kind must be wasm or native";
    }

    for (const auto field : {"functions", "objects", "required_libraries"}) {
        if (root.Has(field) && (result.Type != EModuleType::Module || result.Kind != EModuleKind::Wasm)) {
            ythrow yexception() << field << " is only applicable to WASM modules";
        }
    }
    if (root.Has("module_extension")) {
        if (result.Kind != EModuleKind::Wasm) {
            ythrow yexception() << "module_extension is only applicable to WASM";
        }
        result.Extension = required("module_extension");
        if (result.Extension != "wasm" && result.Extension != "wat" && result.Extension != "wast") {
            ythrow yexception() << "module_extension must be wasm, wat or wast";
        }
    }
    return result;
}
} // namespace NYdb::NUdfManifest
