#include "operation_registry_data.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <google/protobuf/descriptor.pb.h>
#include <jinja2cpp/reflected_value.h>
#include <jinja2cpp/template.h>
#include <jinja2cpp/template_env.h>
#include <jinja2cpp/value.h>

#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/src_location.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

std::string replace(
    const std::string& str,
    const std::string& from,
    const std::string& to)
{
    std::string result;

    size_t pos = str.find(from);
    result.append(str, 0, pos);
    result.append(to);
    result.append(str, pos + from.size(), std::string::npos);

    return result;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        Cerr << "Usage: " << argv[0] << " INPUT OUTPUT ..." << Endl;
        return 1;
    }

    std::vector<std::string> opTypes;
    const auto* d = NKikimrSchemeOp::EOperationType_descriptor();
    for (int i = 0; i < d->value_count(); ++i) {
        const auto* v = d->value(i);
        if (v) {
            auto name = v->full_name();
            name = replace(name, ".", "::");
            opTypes.emplace_back(name);
        }
    }

    struct TOperationEntry {
        const char* Name;
        const char* Support;
        const char* Factory;
    };
    const TOperationEntry entries[] = {
#define OP(name, support, ...) {#name, #support, #__VA_ARGS__},
        SCHEME_OPERATIONS(OP)
#undef OP
    };

    jinja2::ValuesList operations;
    std::unordered_set<std::string> registered;
    for (const auto& entry : entries) {
        if (!d->FindValueByName(entry.Name) || !registered.insert(entry.Name).second) {
            Cerr << "ERROR: unknown or duplicate operation registry entry: " << entry.Name << Endl;
            return 1;
        }
        const std::string support = entry.Support;
        std::string factory = entry.Factory;
        if (support == "Unsupported") {
            factory = "AbortUnimplementedSchemeOperation<NKikimrSchemeOp::" + std::string(entry.Name) + ">();";
        } else if (support == "Deprecated") {
            factory = "Y_ABORT(\"impossible\");";
        } else if (support == "Internal") {
            factory = "Y_ABORT(\"%s\", " + factory + ");";
        } else if (support != "Implemented" && support != "Stub" && support != "Retired") {
            Cerr << "ERROR: unknown support status for " << entry.Name << ": " << support << Endl;
            return 1;
        }
        if (factory.empty()) {
            Cerr << "ERROR: missing factory for " << entry.Name << Endl;
            return 1;
        }
        operations.emplace_back(jinja2::ValuesMap{
            {"name", std::string(entry.Name)},
            {"support", support},
            {"factory", factory},
        });
    }
    for (int i = 0; i < d->value_count(); ++i) {
        if (!registered.contains(d->value(i)->name())) {
            Cerr << "ERROR: missing operation registry entry: " << d->value(i)->name() << Endl;
            return 1;
        }
    }

    jinja2::ValuesList recovery;
#define UNSUPPORTED(txType, opType) \
    recovery.emplace_back(jinja2::ValuesMap{{"type", #txType}, {"operation", #opType}});
#define TRANSIENT(txType) \
    recovery.emplace_back(jinja2::ValuesMap{{"type", #txType}, {"operation", ""}});
    SCHEME_OPERATION_RECOVERY(UNSUPPORTED, TRANSIENT)
#undef UNSUPPORTED
#undef TRANSIENT

    jinja2::TemplateEnv env;
    env.AddGlobal("generator", jinja2::Reflect(std::string(__SOURCE_FILE__)));
    env.AddGlobal("opTypes", jinja2::Reflect(opTypes));
    env.AddGlobal("operations", std::move(operations));
    env.AddGlobal("recovery", std::move(recovery));

    for (int i = 1; i < argc; i += 2) {
        if (!(i + 1 < argc)) {
            Cerr << "ERROR: missing output for " << argv[i] << Endl;
            return 1;
        }

        jinja2::Template t(&env);
        auto loaded = t.Load(TFileInput(argv[i]).ReadAll(), argv[i]);
        if (!loaded) {
            Cerr << "ERROR: " << loaded.error().ToString() << Endl;
            return 1;
        }

        auto rendered = t.RenderAsString({});
        if (!rendered) {
            Cerr << "ERROR: " << rendered.error().ToString() << Endl;
            return 1;
        }

        TFileOutput(argv[i + 1]).Write(rendered.value());
        Cout << "Generated " << argv[i + 1] << " from " << argv[i] << Endl;
    }

    return 0;
}
