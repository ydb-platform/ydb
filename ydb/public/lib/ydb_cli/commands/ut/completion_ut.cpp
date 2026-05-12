#include <ydb/public/lib/ydb_cli/commands/ydb_root_common.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>
#include <ydb/public/lib/ydb_cli/common/completion.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/getopt/small/modchooser.h>

#include <util/generic/hash.h>

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
