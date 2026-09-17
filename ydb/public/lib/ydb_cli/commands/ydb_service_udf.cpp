#include "ydb_service_udf.h"

#include <ydb/public/lib/ydb_cli/common/pretty_table.h>

#include <library/cpp/json/json_writer.h>

#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

namespace NYdb {
namespace NConsoleClient {
namespace {

NUdf::EModuleKind ParseKind(const TString& value) {
    const TString lower = to_lower(value);
    if (lower == "udf") {
        return NUdf::EModuleKind::Udf;
    }
    if (lower == "library") {
        return NUdf::EModuleKind::Library;
    }
    throw TMisuseException() << "Unknown module kind '" << value << "'. Expected: udf, library";
}

NUdf::ECompileStatus ParseCompileStatus(const TString& value) {
    const TString lower = to_lower(value);
    if (lower == "pending") {
        return NUdf::ECompileStatus::Pending;
    }
    if (lower == "compiling") {
        return NUdf::ECompileStatus::Compiling;
    }
    if (lower == "ready") {
        return NUdf::ECompileStatus::Ready;
    }
    if (lower == "failed") {
        return NUdf::ECompileStatus::Failed;
    }
    throw TMisuseException() << "Unknown compile status '" << value
        << "'. Expected: pending, compiling, ready, failed";
}

TStringBuf KindToString(NUdf::EModuleKind kind) {
    switch (kind) {
        case NUdf::EModuleKind::Udf:
            return "udf";
        case NUdf::EModuleKind::Library:
            return "library";
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
    json["kind"] = TString(KindToString(module.Kind));
    json["uid"] = module.Uid;
    json["md5"] = module.Md5;
    json["size"] = module.Size;
    json["version"] = module.Version;
    json["compile_status"] = TString(StatusToString(module.CompileStatus));
    if (!module.CompileError.empty()) {
        json["compile_error"] = module.CompileError;
    }
    return json;
}

} // namespace

TCommandUdf::TCommandUdf()
    : TClientCommandTree("udf", {}, "Manage WASM UDF / LIBRARY modules in the UDF store")
{
    AddCommand(std::make_unique<TCommandUdfUpload>());
    AddCommand(std::make_unique<TCommandUdfDelete>());
    AddCommand(std::make_unique<TCommandUdfList>());
    AddCommand(std::make_unique<TCommandUdfDescribe>());
}

TCommandUdfUpload::TCommandUdfUpload()
    : TYdbOperationCommand("upload", {}, "Upload a WASM UDF or LIBRARY module")
{
}

void TCommandUdfUpload::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption("kind", "Module kind: udf | library")
        .Required().RequiredArgument("KIND").StoreResult(&Kind);
    config.Opts->AddLongOption('f', "file", "Path to the module body (.wasm)")
        .Required().RequiredArgument("PATH").StoreResult(&FilePath);
    config.Opts->AddLongOption("manifest", "Path to manifest.json (required for --kind udf)")
        .Optional().RequiredArgument("PATH").StoreResult(&ManifestPath);
    config.Opts->AddLongOption("name",
            "Library name (required for --kind library; for --kind udf it must match manifest module_name)")
        .Optional().RequiredArgument("NAME").StoreResult(&LibraryName);
    config.Opts->AddLongOption("write-mode", "create-or-replace | create-only | replace-only")
        .Optional().RequiredArgument("MODE").StoreResult(&WriteMode);
    config.Opts->AddLongOption("create-only", "Refuse to replace an existing module")
        .StoreTrue(&CreateOnly);
    config.Opts->AddLongOption("replace-only", "Refuse to create a missing module")
        .StoreTrue(&ReplaceOnly);
    config.Opts->AddLongOption("expected-uid", "Abort unless the current uid matches")
        .Optional().RequiredArgument("UID").StoreResult(&ExpectedUid);
    config.Opts->AddLongOption("expected-md5", "Abort unless the uploaded body md5 matches")
        .Optional().RequiredArgument("MD5").StoreResult(&ExpectedMd5);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::Json });
    config.SetFreeArgsNum(0);
}

void TCommandUdfUpload::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandUdfUpload::Run(TConfig& config) {
    if (CreateOnly && ReplaceOnly) {
        throw TMisuseException() << "--create-only and --replace-only are mutually exclusive";
    }
    if (CreateOnly && WriteMode) {
        throw TMisuseException() << "--create-only conflicts with --write-mode";
    }
    if (ReplaceOnly && WriteMode) {
        throw TMisuseException() << "--replace-only conflicts with --write-mode";
    }

    const auto kind = ParseKind(Kind);
    if (kind == NUdf::EModuleKind::Udf && !ManifestPath) {
        throw TMisuseException() << "--manifest is required for --kind udf";
    }
    if (kind == NUdf::EModuleKind::Library && !LibraryName) {
        throw TMisuseException() << "--name is required for --kind library";
    }

    auto settings = FillSettings(NUdf::TUploadModuleSettings())
        .Kind(kind);
    if (ManifestPath) {
        settings.ManifestJson(TFileInput(ManifestPath).ReadAll());
    }
    if (LibraryName) {
        settings.LibraryName(LibraryName);
    }
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
    auto result = client.UploadModule(TFileInput(FilePath).ReadAll(), settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    if (OutputFormat == EDataFormat::Json) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["name"] = result.GetName();
        json["uid"] = result.GetUid();
        json["md5"] = result.GetMd5();
        json["size"] = result.GetSize();
        json["compile_status"] = TString(StatusToString(result.GetCompileStatus()));
        json["replaced_existing"] = result.GetReplacedExisting();
        NJson::WriteJson(&Cout, &json, true, true);
        Cout << Endl;
    } else {
        Cout << "name: " << result.GetName() << Endl
             << "uid: " << result.GetUid() << Endl
             << "md5: " << result.GetMd5() << Endl
             << "size: " << result.GetSize() << Endl
             << "compile_status: " << StatusToString(result.GetCompileStatus()) << Endl
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
        .Required().RequiredArgument("NAME").StoreResult(&Name);
    config.Opts->AddLongOption("kind", "Optional kind assert: udf | library")
        .Optional().RequiredArgument("KIND").StoreResult(&Kind);
    config.Opts->AddLongOption("expected-uid", "Abort unless the current uid matches")
        .Optional().RequiredArgument("UID").StoreResult(&ExpectedUid);
    config.SetFreeArgsNum(0);
}

int TCommandUdfDelete::Run(TConfig& config) {
    auto settings = FillSettings(NUdf::TDeleteModuleSettings());
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
    config.Opts->AddLongOption("kind", "Filter by kind: udf | library")
        .Optional().RequiredArgument("KIND").StoreResult(&Kind);
    config.Opts->AddLongOption("status", "Filter by compile status")
        .Optional().RequiredArgument("STATUS").StoreResult(&Status);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::Json });
    config.SetFreeArgsNum(0);
}

void TCommandUdfList::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandUdfList::Run(TConfig& config) {
    auto settings = FillSettings(NUdf::TListModulesSettings());
    if (Kind) {
        settings.KindFilter(ParseKind(Kind));
    }
    if (Status) {
        settings.StatusFilter(ParseCompileStatus(Status));
    }

    auto driver = CreateDriver(config);
    NUdf::TUdfClient client(driver);
    auto result = client.ListModules(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    if (OutputFormat == EDataFormat::Json) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        NJson::TJsonValue modules(NJson::JSON_ARRAY);
        for (const auto& module : result.GetModules()) {
            modules.AppendValue(ModuleToJson(module));
        }
        json["modules"] = std::move(modules);
        if (!result.GetNextPageToken().empty()) {
            json["next_page_token"] = result.GetNextPageToken();
        }
        NJson::WriteJson(&Cout, &json, true, true);
        Cout << Endl;
    } else {
        TPrettyTable table({
            "Name",
            "Kind",
            "Uid",
            "Md5",
            "Size",
            "Status",
        });
        for (const auto& module : result.GetModules()) {
            auto& row = table.AddRow();
            row.Column(0, module.Name);
            row.Column(1, KindToString(module.Kind));
            row.Column(2, module.Uid);
            row.Column(3, module.Md5);
            row.Column(4, ToString(module.Size));
            row.Column(5, StatusToString(module.CompileStatus));
        }
        table.Print(Cout);
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
        .Required().RequiredArgument("NAME").StoreResult(&Name);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::Json });
    config.SetFreeArgsNum(0);
}

void TCommandUdfDescribe::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandUdfDescribe::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NUdf::TUdfClient client(driver);
    auto result = client.DescribeModule(Name, FillSettings(NUdf::TDescribeModuleSettings())).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& module = result.GetModule();
    if (OutputFormat == EDataFormat::Json) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["module"] = ModuleToJson(module);
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
        NJson::WriteJson(&Cout, &json, true, true);
        Cout << Endl;
    } else {
        Cout << "name: " << module.Name << Endl
             << "kind: " << KindToString(module.Kind) << Endl
             << "uid: " << module.Uid << Endl
             << "md5: " << module.Md5 << Endl
             << "size: " << module.Size << Endl
             << "version: " << module.Version << Endl
             << "compile_status: " << StatusToString(module.CompileStatus) << Endl;
        if (!module.CompileError.empty()) {
            Cout << "compile_error: " << module.CompileError << Endl;
        }
        if (!result.GetManifestJson().empty()) {
            Cout << "manifest_json: " << result.GetManifestJson() << Endl;
        }
        if (!result.GetPlatforms().empty()) {
            Cout << "platforms:" << Endl;
            for (const auto& platform : result.GetPlatforms()) {
                Cout << "  - cpu_spec: " << platform.CpuSpec
                     << " status: " << StatusToString(platform.Status);
                if (!platform.CompileError.empty()) {
                    Cout << " error: " << platform.CompileError;
                }
                Cout << Endl;
            }
        }
    }
    return EXIT_SUCCESS;
}

} // namespace NConsoleClient
} // namespace NYdb
