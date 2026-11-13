#include "suite.h"

#include <library/cpp/string_utils/quote/quote.h>

#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/system/fstat.h>

namespace NYql::NPureBench {
namespace {

constexpr TStringBuf QueryFileExtension = "yql";
constexpr TStringBuf QueryFileSuffix = ".yql";
constexpr TStringBuf PreparationFileSuffix = ".prepare.yql";
constexpr TStringBuf ConfigurationFileSuffix = ".cfg";
constexpr TStringBuf CaseEngineSeparator = "/engine:";

constexpr TStringBuf LanguageVersionSetting = "langver";
constexpr TStringBuf GenerationSyntaxSetting = "bench_generation_syntax";
constexpr TStringBuf QuerySyntaxSetting = "bench_query_syntax";
constexpr TStringBuf InputRowsSetting = "bench_input_rows";
constexpr TStringBuf LLVMSetting = "bench_llvm_settings";
constexpr TStringBuf BlockEnginesSetting = "bench_block_engines";
constexpr TStringBuf GatewayConfigurationPatchSetting = "gateway_cfg_patch";

TString ReadFile(const TFsPath& path) {
    Y_ENSURE(path.IsFile(), "File is missing or is not a regular file: " << path);
    return TFileInput(path).ReadAll();
}

TString ReadRequiredFile(const TFsPath& path) {
    TString contents = ReadFile(path);
    Y_ENSURE(!StripString(contents).empty(), path << ": File is empty");
    return contents;
}

void FindMeasuredQueries(const TFsPath& directory, const TFsPath& suiteDirectory,
                         THashSet<TString>& ancestors, TVector<TFsPath>& queries) {
    const TString canonicalDirectory = directory.RealPath().GetPath();
    Y_ENSURE(ancestors.insert(canonicalDirectory).second, "Cyclic suite directory link: " << directory);
    Y_DEFER {
        ancestors.erase(canonicalDirectory);
    };
    TVector<TFsPath> children;
    directory.List(children);
    for (const auto& child : children) {
        const TFileStat status(child);
        Y_ENSURE(!status.IsNull() || child.IsSymlink(), "Cannot read suite entry: " << child);
        if (status.IsDir()) {
            FindMeasuredQueries(child, suiteDirectory, ancestors, queries);
        } else if (status.IsFile() && child.GetExtension() == QueryFileExtension && !child.GetName().EndsWith(PreparationFileSuffix)) {
            queries.push_back(child.RelativeTo(suiteDirectory));
        }
    }
}

TVector<TFsPath> ListMeasuredQueries(const TFsPath& suiteDirectory) {
    Y_ENSURE(suiteDirectory.IsDirectory(), "Suite directory is missing or is not a directory: " << suiteDirectory);
    TVector<TFsPath> queries;
    THashSet<TString> ancestors;
    FindMeasuredQueries(suiteDirectory, suiteDirectory, ancestors, queries);
    Sort(queries, [](const TFsPath& left, const TFsPath& right) { return left.GetPath() < right.GetPath(); });
    return queries;
}

TStringBuf SingleValue(const TVector<TStringBuf>& values) {
    Y_ENSURE(values.size() == 1, "Expected exactly one value");
    return values.front();
}

TVector<EBlockEngineMode> ParseBlockEngines(const TVector<TStringBuf>& values) {
    Y_ENSURE(!values.empty(), BlockEnginesSetting << " must not be empty");
    THashSet<EBlockEngineMode> uniqueModes;
    TVector<EBlockEngineMode> modes(Reserve(values.size()));
    for (const TStringBuf value : values) {
        const auto mode = FromString<EBlockEngineMode>(value);
        Y_ENSURE(uniqueModes.insert(mode).second, "Duplicate block engine '" << value << "'");
        modes.push_back(mode);
    }
    return modes;
}

void ExpandBlockEngines(TVector<TBenchmarkCase>& cases, const TVector<TStringBuf>& values) {
    const auto modes = ParseBlockEngines(values);
    TVector<TBenchmarkCase> expandedCases(Reserve(cases.size() * modes.size()));
    for (const auto& benchmarkCase : cases) {
        for (const auto mode : modes) {
            auto expandedCase = benchmarkCase;
            expandedCase.Options.BlockEngineMode = mode;
            expandedCases.push_back(std::move(expandedCase));
        }
    }
    cases = std::move(expandedCases);
}

void ApplyProgramSetting(TBenchmarkProgramOptions& options, TStringBuf key, TStringBuf value) {
    if (key == LanguageVersionSetting) {
        Y_ENSURE(ParseLangVersion(value, options.LanguageVersion) && IsValidLangVersion(options.LanguageVersion),
                 "Invalid fixed langver '" << value << "'");
    } else if (key == GenerationSyntaxSetting) {
        options.GenerationSyntax = FromString<ESyntax>(value);
    } else if (key == QuerySyntaxSetting) {
        options.MeasuredQuerySyntax = FromString<ESyntax>(value);
    } else if (key == InputRowsSetting) {
        options.GeneratorInputRows = FromString<ui64>(value);
    } else if (key == LLVMSetting) {
        options.LLVMSettings = value;
    } else if (key == GatewayConfigurationPatchSetting) {
        Y_ENSURE(false, "Gateway patch пока не поддерживается этим клиентом");
    } else {
        Y_ENSURE(false, "Unknown setting '" << key << "'");
    }
}

void ApplySetting(TVector<TBenchmarkCase>& cases, TStringBuf key, const TVector<TStringBuf>& values) {
    if (key == BlockEnginesSetting) {
        ExpandBlockEngines(cases, values);
    } else {
        const TStringBuf value = SingleValue(values);
        for (auto& benchmarkCase : cases) {
            ApplyProgramSetting(benchmarkCase.Options, key, value);
        }
    }
}

void ParseConfigurationLine(TVector<TBenchmarkCase>& cases, THashSet<TString>& definedSettings, TStringBuf line) {
    TVector<TStringBuf> tokens = StringSplitter(line).SplitBySet(" \t\r").SkipEmpty().ToList<TStringBuf>();
    if (tokens.empty()) {
        return;
    }
    const TString key(tokens.front());
    Y_ENSURE(definedSettings.insert(key).second, "Duplicate setting '" << key << "'");
    tokens.erase(tokens.begin());
    ApplySetting(cases, key, tokens);
}

TVector<TBenchmarkCase> ParseConfiguration(const TFsPath& configurationPath) {
    TVector<TBenchmarkCase> cases{{.Options = {.EnableCalibration = false}}};
    if (!configurationPath.Exists() && !configurationPath.IsSymlink()) {
        return cases;
    }
    const TString contents = ReadFile(configurationPath);
    THashSet<TString> definedSettings;
    for (const TStringBuf line : StringSplitter(contents).Split('\n')) {
        ParseConfigurationLine(cases, definedSettings, line);
    }
    return cases;
}

TVector<TBenchmarkCase> LoadQueryCases(TStringBuf suiteName, const TFsPath& suiteDirectory, const TFsPath& relativeQueryPath) {
    const TFsPath measuredQueryPath = suiteDirectory / relativeQueryPath;
    const TFsPath queryDirectory = measuredQueryPath.Parent();
    const TString basename = measuredQueryPath.GetName().substr(0, measuredQueryPath.GetName().size() - QueryFileSuffix.size());
    auto cases = ParseConfiguration(queryDirectory / (basename + ConfigurationFileSuffix));
    const TFsPath preparationPath = queryDirectory / (basename + PreparationFileSuffix);
    const TMaybe<TString> preparation = preparationPath.Exists() || preparationPath.IsSymlink()
                                            ? ReadRequiredFile(preparationPath)
                                            : TMaybe<TString>();
    const TString measuredQuery = ReadRequiredFile(measuredQueryPath);
    TString queryName = relativeQueryPath.GetPath().substr(0, relativeQueryPath.GetPath().size() - QueryFileSuffix.size());
    TString encodedSuiteName(suiteName);
    Quote(encodedSuiteName, "");
    Quote(queryName);
    for (auto& benchmarkCase : cases) {
        if (preparation) {
            benchmarkCase.Options.GenerationQuery = *preparation;
        }
        benchmarkCase.Options.MeasuredQuery = measuredQuery;
        benchmarkCase.Name = TStringBuilder() << encodedSuiteName << '/' << queryName
                                              << CaseEngineSeparator << ToString(benchmarkCase.Options.BlockEngineMode);
    }
    return cases;
}

} // namespace

TVector<TBenchmarkCase> LoadBenchmarkSuite(TStringBuf suiteName, TStringBuf suiteDataDirectory) {
    Y_ENSURE(!suiteName.empty() && !suiteName.Contains('/'), "Suite name must be a non-empty path component");
    Y_ENSURE(!suiteDataDirectory.empty(), "Suite data directory must not be empty");
    TFsPath suiteDirectory(suiteDataDirectory);
    if (suiteDirectory.IsRelative()) {
        suiteDirectory = TFsPath::Cwd() / suiteDirectory;
    }
    suiteDirectory.Fix();
    TVector<TBenchmarkCase> cases;
    for (const auto& relativeQueryPath : ListMeasuredQueries(suiteDirectory)) {
        for (auto& benchmarkCase : LoadQueryCases(suiteName, suiteDirectory, relativeQueryPath)) {
            cases.push_back(std::move(benchmarkCase));
        }
    }
    Y_ENSURE(!cases.empty(), "Suite contains no main .yql queries: " << suiteDirectory);
    return cases;
}

} // namespace NYql::NPureBench
