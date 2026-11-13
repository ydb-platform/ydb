#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/qplayer/storage/memory/yql_qstorage_memory.h>
#include <yql/essentials/core/yql_library_compiler.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

namespace NYql {
namespace {

class TRecordingModuleResolver: public TModuleResolver {
public:
    TRecordingModuleResolver()
        : TModuleResolver(NSQLTranslation::TTranslators(nullptr, nullptr, nullptr), TModulesTable{}, 0, {}, {})
    {
    }

    IModuleResolver::TPtr CreateMutableChild() const override {
        Child = TModuleResolver::CreateMutableChild();
        return Child;
    }

    mutable IModuleResolver::TPtr Child;
};

TProjectSettings CreateProgramSettings(const TProjectSettings& settings, const TQContext& qContext) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, {}, "test");
    auto modules = std::make_shared<TRecordingModuleResolver>();
    factory.SetModules(modules);
    factory.SetProjectSettings(settings);
    auto program = factory.Create("test", "", "session", EHiddenMode::Disable, qContext);
    return *modules->Child->GetProjectSettings();
}

void AssertProjectPragmasEqual(const TProjectSettings& actual, const TProjectSettings& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.Pragmas.size(), expected.Pragmas.size());
    for (size_t index = 0; index < expected.Pragmas.size(); ++index) {
        UNIT_ASSERT_VALUES_EQUAL(actual.Pragmas[index].Name, expected.Pragmas[index].Name);
        UNIT_ASSERT_VALUES_EQUAL(actual.Pragmas[index].Value, expected.Pragmas[index].Value);
    }
}

TString CompileProjectLibrary(const TProjectSettings& settings) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(/*isAmbiguityError=*/true, /*isAmbiguityDebugging=*/false);
    NSQLTranslation::TTranslators translators(nullptr, NSQLTranslationV1::MakeTranslator(lexers, parsers), nullptr);
    TExprContext ctx;
    TLibraryCohesion library;
    UNIT_ASSERT_C(CompileLibrary(translators, "/lib/project.sql", "$value = 1 / 2; EXPORT $value;", ctx, library, true, settings),
                  ctx.IssueManager.GetIssues().ToString());
    return ConvertToAst(*library.Exports.Symbols().at("$value"), ctx, {}).Root->ToString();
}

} // namespace

Y_UNIT_TEST_SUITE(TProjectSettingsQStorage) {
Y_UNIT_TEST(RestoreSettingsBeforeCompilingLibraries) {
    TProjectSettings settings;
    settings.AddPragma("ClassicDivision", "false");
    const auto expected = CompileProjectLibrary(settings);
    UNIT_ASSERT(expected != CompileProjectLibrary({}));
    auto storage = MakeMemoryQStorage();
    auto writer = storage->MakeWriter("query", {});
    CreateProgramSettings(settings, TQContext(writer));
    writer->Commit().GetValueSync();
    const auto restored = ProjectSettingsFromQContext(TQContext(storage->MakeReader("query", {})));
    UNIT_ASSERT_VALUES_EQUAL(CompileProjectLibrary(restored), expected);
}

Y_UNIT_TEST(CaptureAndReplay) {
    for (const auto mode : {EQPlayerCaptureMode::MetaOnly, EQPlayerCaptureMode::Full}) {
        TProjectSettings settings;
        settings.AddPragma("config.flags", TVector<TString>{"NodesAllocationLimit", "0"});
        settings.AddPragma("config.flags", TVector<TString>{"DisableStrictTableProps"});
        settings.AddPragma("Empty", TVector<TString>{});
        settings.AddPragma("Escaped", TVector<TString>{"", "\"\\\n"});
        auto storage = MakeMemoryQStorage();
        auto writer = storage->MakeWriter("query", {});
        AssertProjectPragmasEqual(CreateProgramSettings(settings, TQContext(writer, mode)), settings);
        writer->Commit().GetValueSync();
        auto reader = storage->MakeReader("query", {});
        const auto item = reader->Get({.Component = "Facade", .Label = "ProjectSettings"}).GetValueSync();
        UNIT_ASSERT(item);
        const auto storedSettings = NYT::NodeFromYsonString(item->Value);
        UNIT_ASSERT(storedSettings.IsMap());
        UNIT_ASSERT_VALUES_EQUAL(storedSettings["Pragmas"].AsList().size(), settings.Pragmas.size());
        TProjectSettings changed;
        changed.AddPragma("Different", "value");
        AssertProjectPragmasEqual(CreateProgramSettings(changed, TQContext(reader, mode)), settings);
    }
}

Y_UNIT_TEST(EmptyCaptureOverridesReplayInput) {
    auto storage = MakeMemoryQStorage();
    auto writer = storage->MakeWriter("query", {});
    UNIT_ASSERT(CreateProgramSettings({}, TQContext(writer)).Pragmas.empty());
    writer->Commit().GetValueSync();
    TProjectSettings changed;
    changed.AddPragma("Different", "value");
    UNIT_ASSERT(CreateProgramSettings(changed, TQContext(storage->MakeReader("query", {}))).Pragmas.empty());
}

Y_UNIT_TEST(OldCaptureHasNoProjectSettings) {
    auto storage = MakeMemoryQStorage();
    storage->MakeWriter("query", {})->Commit().GetValueSync();
    TProjectSettings settings;
    settings.AddPragma("Different", "value");
    UNIT_ASSERT(CreateProgramSettings(settings, TQContext(storage->MakeReader("query", {}))).Pragmas.empty());
}

Y_UNIT_TEST(ReplayIgnoresUnknownFields) {
    for (const auto& data : {"{FutureSetting=42;}", "{Pragmas=[];FutureSetting=42;}"}) {
        auto storage = MakeMemoryQStorage();
        auto writer = storage->MakeWriter("query", {});
        writer->Put({.Component = "Facade", .Label = "ProjectSettings"}, data).GetValueSync();
        writer->Commit().GetValueSync();
        TProjectSettings settings;
        settings.AddPragma("Different", "value");
        UNIT_ASSERT(CreateProgramSettings(settings, TQContext(storage->MakeReader("query", {}))).Pragmas.empty());
    }
}
} // Y_UNIT_TEST_SUITE(TProjectSettingsQStorage)

} // namespace NYql
