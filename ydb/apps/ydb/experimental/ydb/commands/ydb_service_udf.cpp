#include "ydb_service_udf.h"
#include "udf_package.h"

#include <ydb/public/lib/ydb_cli/common/pretty_table.h>

#include <library/cpp/json/json_writer.h>
#include <ydb/public/lib/udf/manifest/manifest.h>
#include <yaml-cpp/yaml.h>

#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

namespace NYdb {
namespace NConsoleClient {
namespace {

NUdf::EModuleType ParseType(const TString& value) {
    const TString lower = to_lower(value);
    if (lower == "module") {
        return NUdf::EModuleType::Module;
    }
    if (lower == "library") {
        return NUdf::EModuleType::Library;
    }
    throw TMisuseException() << "Unknown module type '" << value << "'. Expected: module, library";
}

NUdf::EModuleKind ParseKind(const TString& value) {
    const TString lower = to_lower(value);
    if (lower == "wasm") {
        return NUdf::EModuleKind::Wasm;
    }
    if (lower == "native") {
        return NUdf::EModuleKind::Native;
    }
    throw TMisuseException() << "Unknown module kind '" << value << "'. Expected: wasm, native";
}

void EmitYaml(YAML::Emitter& emitter, const NJson::TJsonValue& json) {
    switch (json.GetType()) {
        case NJson::JSON_MAP:
            emitter << YAML::BeginMap;
            for (const auto& [key, value] : json.GetMap()) {
                emitter << YAML::Key << YAML::DoubleQuoted << std::string(key.data(), key.size()) << YAML::Value;
                EmitYaml(emitter, value);
            }
            emitter << YAML::EndMap;
            break;
        case NJson::JSON_ARRAY:
            emitter << YAML::BeginSeq;
            for (const auto& value : json.GetArray()) {
                EmitYaml(emitter, value);
            }
            emitter << YAML::EndSeq;
            break;
        case NJson::JSON_STRING: {
            // Quote strings even when their contents look like YAML booleans,
            // numbers or timestamps, preserving the JSON value's type.
            const auto& value = json.GetString();
            emitter << YAML::DoubleQuoted << std::string(value.data(), value.size());
            break;
        }
        case NJson::JSON_INTEGER:
            emitter << json.GetInteger();
            break;
        case NJson::JSON_UINTEGER:
            emitter << json.GetUInteger();
            break;
        case NJson::JSON_DOUBLE:
            emitter << json.GetDouble();
            break;
        case NJson::JSON_BOOLEAN:
            emitter << json.GetBoolean();
            break;
        case NJson::JSON_NULL:
        case NJson::JSON_UNDEFINED:
            emitter << YAML::Null;
            break;
    }
}

void PrintStructured(const NJson::TJsonValue& json, const TString& format) {
    if (format == "yaml") {
        YAML::Emitter emitter;
        EmitYaml(emitter, json);
        Cout << emitter.c_str() << Endl;
    } else {
        NJson::WriteJson(&Cout, &json, true, true);
        Cout << Endl;
    }
}

void CheckFormat(const TString& format, std::initializer_list<TStringBuf> allowed) {
    for (auto item : allowed) {
        if (format == item) {
            return;
        }
    }
    throw TMisuseException() << "Unsupported output format: " << format;
}

TStringBuf TypeToString(NUdf::EModuleType kind) {
    switch (kind) {
        case NUdf::EModuleType::Module:
            return "module";
        case NUdf::EModuleType::Library:
            return "library";
        case NUdf::EModuleType::Unspecified:
            return "unspecified";
    }
    return "unspecified";
}

TStringBuf KindToString(NUdf::EModuleKind kind) {
    switch (kind) {
        case NUdf::EModuleKind::Wasm:
            return "wasm";
        case NUdf::EModuleKind::Native:
            return "native";
        case NUdf::EModuleKind::Unspecified:
            return "unspecified";
    }
    return "unspecified";
}

TStringBuf StatusToString(NUdf::ECompileStatus status) {
    switch (status) {
        case NUdf::ECompileStatus::Pending:
            return "pending";
        case NUdf::ECompileStatus::Compiling:
            return "compiling";
        case NUdf::ECompileStatus::Ready:
            return "ready";
        case NUdf::ECompileStatus::Failed:
            return "failed";
        case NUdf::ECompileStatus::Unspecified:
            return "unspecified";
    }
    return "unspecified";
}

NJson::TJsonValue ModuleToJson(const NUdf::TModuleInfo& module) {
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["name"] = module.Name;
    json["module_type"] = TString(TypeToString(module.Type));
    json["module_kind"] = TString(KindToString(module.Kind));
    json["uid"] = module.Uid;
    json["md5"] = module.Md5;
    json["size"] = module.Size;
    json["version"] = module.Version;
    return json;
}

} // namespace

TCommandUdf::TCommandUdf()
    : TClientCommandTree("udf", {}, "Manage WASM modules and libraries (native is not supported yet)")
{
    AddCommand(std::make_unique<TCommandUdfUpload>());
    AddCommand(std::make_unique<TCommandUdfDelete>());
    AddCommand(std::make_unique<TCommandUdfList>());
    AddCommand(std::make_unique<TCommandUdfDescribe>());
}

TCommandUdfUpload::TCommandUdfUpload()
    : TYdbOperationCommand("upload", {}, "Upload a WASM module or library using its manifest")
{
}

void TCommandUdfUpload::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption('f', "file", "Path to the module body (format is specified in the manifest)")
        .Optional()
        .RequiredArgument("PATH")
        .StoreResult(&FilePath);
    config.Opts->AddLongOption("manifest", "Path to the required module or library manifest.json")
        .Optional()
        .RequiredArgument("PATH")
        .StoreResult(&ManifestPath);
    config.Opts->AddLongOption("package", "Path to a ZIP, TAR, TAR.GZ, or TGZ package containing manifest.json and one binary")
        .Optional()
        .RequiredArgument("PATH")
        .StoreResult(&PackagePath);
    config.Opts->AddLongOption(
            "write-mode",
            "Module write mode: create-or-replace (default) | create-only | replace-only")
        .Optional()
        .RequiredArgument("MODE")
        .ChoicesWithCompletion({
            {"create-or-replace", "Create a module or replace an existing module"},
            {"create-only", "Refuse to replace an existing module"},
            {"replace-only", "Refuse to create a missing module"},
        })
        .StoreResult(&WriteMode);
    config.Opts->AddLongOption("create-only", "Refuse to replace an existing module")
        .StoreTrue(&CreateOnly);
    config.Opts->AddLongOption("replace-only", "Refuse to create a missing module")
        .StoreTrue(&ReplaceOnly);
    config.Opts->AddLongOption("expected-uid", "Abort unless the current uid matches")
        .Optional()
        .RequiredArgument("UID")
        .StoreResult(&ExpectedUid);
    config.Opts->AddLongOption("expected-md5", "Abort unless the uploaded body md5 matches")
        .Optional()
        .RequiredArgument("MD5")
        .StoreResult(&ExpectedMd5);
    config.Opts->AddLongOption("format", "Output format: text (default) | json").RequiredArgument("FORMAT").StoreResult(&Format);
    config.Opts->MutuallyExclusive("create-only", "replace-only");
    config.Opts->MutuallyExclusive("create-only", "write-mode");
    config.Opts->MutuallyExclusive("replace-only", "write-mode");
    config.Opts->MutuallyExclusive("package", "file");
    config.Opts->MutuallyExclusive("package", "manifest");
    config.SetFreeArgsNum(0);
}

void TCommandUdfUpload::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    CheckFormat(Format, {"text", "json"});
    if (config.ParseResult->Has("expected-uid") && ExpectedUid.empty()) {
        throw TMisuseException() << "--expected-uid must not be empty";
    }
    if (config.ParseResult->Has("expected-md5") && ExpectedMd5.empty()) {
        throw TMisuseException() << "--expected-md5 must not be empty";
    }
    const bool hasPackage = config.ParseResult->Has("package");
    const bool hasFile = config.ParseResult->Has("file");
    const bool hasManifest = config.ParseResult->Has("manifest");
    if ((hasPackage && PackagePath.empty()) || (hasFile && FilePath.empty()) ||
        (hasManifest && ManifestPath.empty()))
    {
        throw TMisuseException() << "Upload paths must not be empty";
    }
    if (!hasPackage && (!hasFile || !hasManifest)) {
        throw TMisuseException() << "Specify either --package or both --file and --manifest";
    }
}

int TCommandUdfUpload::Run(TConfig& config) {
    std::string manifest;
    std::string packageBody;
    if (PackagePath) {
        auto package = ReadUdfPackage(PackagePath);
        manifest = std::move(package.Manifest);
        packageBody = std::move(package.Body);
    } else {
        const TString manifestData = TFileInput(ManifestPath).ReadAll();
        manifest.assign(manifestData.data(), manifestData.size());
    }
    NUdfManifest::Parse(TStringBuf(manifest.data(), manifest.size()));
    auto settings = FillSettings(NUdf::TUploadModuleSettings()).ManifestJson(manifest);
    if (CreateOnly) {
        settings.WriteMode(NUdf::EWriteMode::CreateOnly);
    } else if (ReplaceOnly) {
        settings.WriteMode(NUdf::EWriteMode::ReplaceOnly);
    } else if (WriteMode) {
        const TString lower = to_lower(WriteMode);
        if (lower == "create-or-replace") {
            settings.WriteMode(NUdf::EWriteMode::CreateOrReplace);
        } else if (lower == "create-only") {
            settings.WriteMode(NUdf::EWriteMode::CreateOnly);
        } else if (lower == "replace-only") {
            settings.WriteMode(NUdf::EWriteMode::ReplaceOnly);
        } else {
            throw TMisuseException() << "Unknown write mode '" << WriteMode << "'";
        }
    }
    if (ExpectedUid) {
        settings.ExpectedUid(ExpectedUid);
    }
    if (ExpectedMd5) {
        settings.ExpectedMd5(ExpectedMd5);
    }

    auto driver = CreateDriver(config);
    NUdf::TUdfClient client(driver);
    auto result = PackagePath
        ? client.UploadModule(std::move(packageBody), settings).GetValueSync()
        : client.UploadModuleFromFile(FilePath, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    if (Format == "json") {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["name"] = result.GetName();
        json["uid"] = result.GetUid();
        json["md5"] = result.GetMd5();
        json["size"] = result.GetSize();
        json["replaced_existing"] = result.GetReplacedExisting();
        NJson::WriteJson(&Cout, &json, true, true);
        Cout << Endl;
    } else {
        Cout << "name: " << result.GetName() << Endl
             << "uid: " << result.GetUid() << Endl
             << "md5: " << result.GetMd5() << Endl
             << "size: " << result.GetSize() << Endl
             << "replaced_existing: " << (result.GetReplacedExisting() ? "true" : "false") << Endl;
    }
    return EXIT_SUCCESS;
}

TCommandUdfDelete::TCommandUdfDelete()
    : TYdbOperationCommand("delete", {}, "Delete a module from the UDF store")
{
}

void TCommandUdfDelete::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption("name", "Module name")
        .Required()
        .RequiredArgument("NAME")
        .StoreResult(&Name);
    config.Opts->AddLongOption("type", "Module type: module | library")
        .Optional()
        .RequiredArgument("TYPE")
        .StoreResult(&Type);
    config.Opts->AddLongOption("kind", "Optional code kind assert: wasm | native (native unsupported)")
        .Optional()
        .RequiredArgument("KIND")
        .StoreResult(&Kind);
    config.Opts->AddLongOption("expected-uid", "Abort unless the current uid matches")
        .Optional()
        .RequiredArgument("UID")
        .StoreResult(&ExpectedUid);
    config.SetFreeArgsNum(0);
}

void TCommandUdfDelete::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    if (config.ParseResult->Has("expected-uid") && ExpectedUid.empty()) {
        throw TMisuseException() << "--expected-uid must not be empty";
    }
}

int TCommandUdfDelete::Run(TConfig& config) {
    auto settings = FillSettings(NUdf::TDeleteModuleSettings());
    if (Type) {
        settings.Type(ParseType(Type));
    }
    if (Kind) {
        settings.Kind(ParseKind(Kind));
    }
    if (ExpectedUid) {
        settings.ExpectedUid(ExpectedUid);
    }

    auto driver = CreateDriver(config);
    NUdf::TUdfClient client(driver);
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.DeleteModule(Name, settings).GetValueSync());
    return EXIT_SUCCESS;
}

TCommandUdfList::TCommandUdfList()
    : TYdbOperationCommand("list", {}, "List modules in the UDF store")
{
}

void TCommandUdfList::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption("type", "Module type: module | library")
        .Optional()
        .RequiredArgument("TYPE")
        .StoreResult(&Type);
    config.Opts->AddLongOption("kind", "Filter by code kind: wasm | native (native unsupported)")
        .Optional()
        .RequiredArgument("KIND")
        .StoreResult(&Kind);
    config.Opts->AddLongOption("format", "Output format: table (default) | json | yaml").RequiredArgument("FORMAT").StoreResult(&Format);
    config.SetFreeArgsNum(0);
}

void TCommandUdfList::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    CheckFormat(Format, {"table", "json", "yaml"});
}

int TCommandUdfList::Run(TConfig& config) {
    auto settings = FillSettings(NUdf::TListModulesSettings());
    if (Type) {
        settings.TypeFilter(ParseType(Type));
    }
    if (Kind) {
        settings.KindFilter(ParseKind(Kind));
    }
    auto driver = CreateDriver(config);
    NUdf::TUdfClient client(driver);
    NJson::TJsonValue json(NJson::JSON_MAP);
    NJson::TJsonValue modules(NJson::JSON_ARRAY);
    TPrettyTable table({"Name", "ModuleType", "ModuleKind", "Uid"});
    do {
        auto result = client.ListModules(settings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        for (const auto& module : result.GetModules()) {
            modules.AppendValue(ModuleToJson(module));
            auto& row = table.AddRow();
            row.Column(0, module.Name);
            row.Column(1, TypeToString(module.Type));
            row.Column(2, KindToString(module.Kind));
            row.Column(3, module.Uid);
        }
        settings.PageToken(result.GetNextPageToken());
    } while (!settings.PageToken_.empty());
    if (Format == "table") {
        table.Print(Cout);
    } else {
        json["modules"] = std::move(modules);
        PrintStructured(json, Format);
    }
    return EXIT_SUCCESS;
}

TCommandUdfDescribe::TCommandUdfDescribe()
    : TYdbOperationCommand("describe", {}, "Describe a module and per-cpu_spec compile status")
{
}

void TCommandUdfDescribe::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption("name", "Module name")
        .Required()
        .RequiredArgument("NAME")
        .StoreResult(&Name);
    config.Opts->AddLongOption("format", "Output format: json (default) | yaml").RequiredArgument("FORMAT").StoreResult(&Format);
    config.SetFreeArgsNum(0);
}

void TCommandUdfDescribe::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    CheckFormat(Format, {"json", "yaml"});
}

int TCommandUdfDescribe::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NUdf::TUdfClient client(driver);
    auto result = client.DescribeModule(Name, FillSettings(NUdf::TDescribeModuleSettings())).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& moduleInfo = result.GetModule();
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["module"] = ModuleToJson(moduleInfo);
    json["manifest_json"] = result.GetManifestJson();
    NJson::TJsonValue platforms(NJson::JSON_ARRAY);
    for (const auto& platform : result.GetPlatforms()) {
        NJson::TJsonValue item(NJson::JSON_MAP);
        item["cpu_spec"] = platform.CpuSpec;
        item["status"] = TString(StatusToString(platform.Status));
        if (!platform.CompileError.empty()) {
            item["compile_error"] = platform.CompileError;
        }
        platforms.AppendValue(std::move(item));
    }
    json["platforms"] = std::move(platforms);
    PrintStructured(json, Format);

    return EXIT_SUCCESS;
}

} // namespace NConsoleClient
} // namespace NYdb
