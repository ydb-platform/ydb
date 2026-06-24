#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <google/protobuf/descriptor.pb.h>
#include <jinja2cpp/reflected_value.h>
#include <jinja2cpp/template.h>
#include <jinja2cpp/template_env.h>
#include <jinja2cpp/value.h>

#include <yaml-cpp/yaml.h>

#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/src_location.h>

#include <cstdint>
#include <iostream>
#include <set>
#include <string>
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

// Reads op_handler_overrides.yaml and returns the list of migrated op names
// in proto-enum order. Aborts on unknown enum names so a typo in the yaml
// is a build error, not a silent miss.
std::vector<std::string> LoadMigratedOps(
    const std::string& yamlPath,
    const google::protobuf::EnumDescriptor& enumDesc)
{
    std::set<std::string> requested;
    YAML::Node root = YAML::LoadFile(yamlPath);
    if (auto ops = root["ops"]; ops && ops.IsSequence()) {
        for (const auto& entry : ops) {
            requested.insert(entry.as<std::string>());
        }
    }

    std::vector<std::string> ordered;
    for (int i = 0; i < enumDesc.value_count(); ++i) {
        const auto* v = enumDesc.value(i);
        if (requested.count(v->name())) {
            ordered.push_back(v->name());
            requested.erase(v->name());
        }
    }
    if (!requested.empty()) {
        Cerr << "ERROR: " << yamlPath
             << " references unknown EOperationType values:" << Endl;
        for (const auto& name : requested) {
            Cerr << "  " << name << Endl;
        }
        std::exit(1);
    }
    return ordered;
}

int main(int argc, char** argv) {
    if (argc < 4 || std::string(argv[1]).rfind("--yaml=", 0) != 0) {
        Cerr << "Usage: " << argv[0]
             << " --yaml=PATH INPUT OUTPUT [INPUT OUTPUT...]" << Endl;
        return 1;
    }

    const std::string yamlPath = std::string(argv[1]).substr(strlen("--yaml="));

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

    const auto migratedOps = LoadMigratedOps(yamlPath, *d);

    jinja2::TemplateEnv env;
    env.AddGlobal("generator", jinja2::Reflect(std::string(__SOURCE_FILE__)));
    env.AddGlobal("opTypes", jinja2::Reflect(opTypes));
    env.AddGlobal("migratedOps", jinja2::Reflect(migratedOps));

    for (int i = 2; i < argc; i += 2) {
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
