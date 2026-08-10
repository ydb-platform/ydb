#include <ydb/public/lib/ydb_cli/commands/ydb_root_common.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>
#include <ydb/public/lib/ydb_cli/common/completion.h>
#include <ydb/public/lib/ydb_cli/common/completion_graph_json.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/getopt/small/completer.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/getopt/small/modchooser.h>
#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/stream/str.h>

#include <functional>

using namespace NYdb::NConsoleClient;

TVector<NYdb::NTopic::ECodec> NYdb::NConsoleClient::InitAllowedCodecs() {
    return {
        NYdb::NTopic::ECodec::RAW,
        NYdb::NTopic::ECodec::ZSTD,
        NYdb::NTopic::ECodec::GZIP,
    };
}

namespace {

constexpr size_t MaxCompletionDescriptionLength = 200;

void CollectCompletionErrors(
    const TModChooser& chooser,
    const TString& path,
    TVector<TString>& errors)
{
    auto modes = chooser.GetUnsortedModes();
    THashMap<TString, TString> descToFirstCmd;

    for (const auto* mode : modes) {
        if (mode->Name.empty() || mode->Hidden) {
            continue;
        }

        const TString fullPath = path + " " + mode->Name;

        if (mode->Description.empty()) {
            errors.push_back(TStringBuilder()
                << fullPath << ": empty completion description");
        }

        if (mode->Description.size() > MaxCompletionDescriptionLength) {
            errors.push_back(TStringBuilder()
                << fullPath << ": completion description is "
                << mode->Description.size() << " chars (max "
                << MaxCompletionDescriptionLength
                << "). Set CompletionDescription field to a shorter text"
                   " in the command constructor");
        }

        if (!mode->Description.empty()) {
            auto result = descToFirstCmd.emplace(mode->Description, fullPath);
            if (!result.second) {
                errors.push_back(TStringBuilder()
                    << fullPath << ": duplicate completion description \""
                    << mode->Description << "\" (same as "
                    << result.first->second << ")");
            }
        }

        if (auto* mainModes = dynamic_cast<TMainClassModes*>(mode->Main)) {
            CollectCompletionErrors(mainModes->GetSubModes(), fullPath, errors);
        }
    }
}

TString FormatErrors(const TVector<TString>& errors) {
    TStringBuilder msg;
    msg << "Completion data invariant violations (" << errors.size() << "):\n";
    for (const auto& err : errors) {
        msg << "  - " << err << "\n";
    }
    return msg;
}

TClientCommand::TConfig MakeDummyConfig() {
    static char arg0[] = "ydb";
    static char* argv[] = {arg0};
    return TClientCommand::TConfig(1, argv);
}

} // namespace

Y_UNIT_TEST_SUITE(CompletionData) {
    Y_UNIT_TEST(RootCommandDescriptionsAreValid) {
        TClientSettings settings;
        settings.EnableSsl = false;
        settings.UseAccessToken = true;
        settings.UseDefaultTokenFile = false;
        settings.UseIamAuth = false;
        settings.UseExportToYt = false;
        settings.UseStaticCredentials = false;
        settings.MentionUserAccount = false;
        settings.UseOauth2TokenExchange = false;
        settings.YdbDir = "ydb";

        TClientCommandRootCommon root("ydb", settings);
        auto config = MakeDummyConfig();

        TModChooser chooser;
        TYdbCommandTreeAutoCompletionWrapper wrapper(&root, config);
        wrapper.RegisterModes(chooser);

        TVector<TString> errors;
        CollectCompletionErrors(chooser, "ydb", errors);

        UNIT_ASSERT_C(errors.empty(), FormatErrors(errors));
    }
}

namespace {

// Minimal leaf command (TMainClassArgs) whose options/free-args are configured by
// a callback, so tests can run GenerateJsonCompletion against a real graph node.
class TConfigurableLeaf : public TMainClassArgs {
public:
    using TSetup = std::function<void(NLastGetopt::TOpts&)>;

    explicit TConfigurableLeaf(TSetup setup)
        : Setup_(std::move(setup))
    {
    }

protected:
    void RegisterOptions(NLastGetopt::TOpts& opts) override {
        if (Setup_) {
            Setup_(opts);
        }
    }

    int DoRun(NLastGetopt::TOptsParseResult&&) override {
        return 0;
    }

private:
    TSetup Setup_;
};

NJson::TJsonValue GenerateAndParse(const TModChooser& chooser, const NLastGetopt::TOpts& rootOpts) {
    TStringStream out;
    GenerateJsonCompletion(chooser, rootOpts, out);
    NJson::TJsonValue root;
    UNIT_ASSERT_C(NJson::ReadJsonTree(out.Str(), &root), "produced invalid JSON: " << out.Str());
    return root;
}

} // namespace

Y_UNIT_TEST_SUITE(CompletionGraphJson) {
    Y_UNIT_TEST(OptionValueKinds) {
        NLastGetopt::TOpts opts;
        opts.AddLongOption("verbose").NoArgument();
        opts.AddLongOption("endpoint").RequiredArgument("ENDPOINT");
        opts.AddLongOption("input").RequiredArgument("FILE").Completer(NLastGetopt::NComp::File());
        opts.AddLongOption("dir").RequiredArgument("DIR").Completer(NLastGetopt::NComp::Directory());
        opts.AddLongOption("default").RequiredArgument("X").Completer(NLastGetopt::NComp::Default());
        opts.AddLongOption("host").RequiredArgument("HOST").Completer(NLastGetopt::NComp::Host());
        opts.AddLongOption("format").RequiredArgument("FORMAT").Choices(TVector<TString>{"json", "csv"});

        TModChooser chooser;
        auto root = GenerateAndParse(chooser, opts);
        const auto& options = root["options"];

        // A flag stays null.
        UNIT_ASSERT(options["--verbose"].IsNull());
        // A value option without a completer defaults to local file completion.
        UNIT_ASSERT_VALUES_EQUAL(options["--endpoint"].GetStringSafe(), "file");
        // An explicit File() completer is also local files.
        UNIT_ASSERT_VALUES_EQUAL(options["--input"].GetStringSafe(), "file");
        // Directory() and Default() also list local filesystem entries (their zsh
        // actions start with _files/_default); guards CompletesLocalFiles() against
        // those action strings changing.
        UNIT_ASSERT_VALUES_EQUAL(options["--dir"].GetStringSafe(), "file");
        UNIT_ASSERT_VALUES_EQUAL(options["--default"].GetStringSafe(), "file");
        // A non-file completer (host) is reported as an opaque value.
        UNIT_ASSERT_VALUES_EQUAL(options["--host"].GetStringSafe(), "value");
        // Choices are listed and sorted.
        UNIT_ASSERT(options["--format"].IsArray());
        UNIT_ASSERT_VALUES_EQUAL(options["--format"].GetArray().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(options["--format"][0].GetStringSafe(), "csv");
        UNIT_ASSERT_VALUES_EQUAL(options["--format"][1].GetStringSafe(), "json");
        // The root behaves as a command group.
        UNIT_ASSERT(root["free_args"].IsNull());
    }

    Y_UNIT_TEST(LeafFreeArgs) {
        TConfigurableLeaf undeclared([](NLastGetopt::TOpts&) {});
        TConfigurableLeaf noArgs([](NLastGetopt::TOpts& o) { o.SetFreeArgsNum(0); });
        TConfigurableLeaf fileArg([](NLastGetopt::TOpts& o) { o.SetFreeArgsNum(1); });
        TConfigurableLeaf customArg([](NLastGetopt::TOpts& o) {
            o.SetFreeArgsNum(1);
            o.GetFreeArgSpec(0).Completer(NLastGetopt::NComp::Host());
        });
        // Unbounded args (the TOpts default max) with only a trailing completer set
        // and no title/help: exercises the trailing-arg branch in
        // WriteFreeArgsValue and the IsDefault()/Completer_ guard -- without that
        // guard this would be misreported as null.
        TConfigurableLeaf trailingCustom([](NLastGetopt::TOpts& o) {
            o.SetFreeArgsMax(NLastGetopt::TOpts::UNLIMITED_ARGS);
            o.GetTrailingArgSpec().Completer(NLastGetopt::NComp::Host());
        });

        TModChooser chooser;
        chooser.AddMode("undeclared", &undeclared, "");
        chooser.AddMode("no-args", &noArgs, "");
        chooser.AddMode("file-arg", &fileArg, "");
        chooser.AddMode("custom-arg", &customArg, "");
        chooser.AddMode("trailing-custom", &trailingCustom, "");

        NLastGetopt::TOpts rootOpts;
        auto root = GenerateAndParse(chooser, rootOpts);
        const auto& handlers = root["handlers"];

        // A command that never restricted/declared its positionals must not make
        // the consumer dump the cwd (regression guard for the "386 files" bug).
        UNIT_ASSERT(handlers["undeclared"]["free_args"].IsNull());
        UNIT_ASSERT(handlers["no-args"]["free_args"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(handlers["file-arg"]["free_args"].GetStringSafe(), "file");
        UNIT_ASSERT_VALUES_EQUAL(handlers["custom-arg"]["free_args"].GetStringSafe(), "value");
        UNIT_ASSERT_VALUES_EQUAL(handlers["trailing-custom"]["free_args"].GetStringSafe(), "value");
        // Leaves expose no subcommands.
        UNIT_ASSERT(handlers["file-arg"]["handlers"].GetMapSafe().empty());
    }
}
