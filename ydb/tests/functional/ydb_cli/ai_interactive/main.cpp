#include <ydb/apps/ydb/commands/ydb_root.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_root_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <util/system/env.h>

#include <filesystem>

namespace NYdb::NConsoleClient {

namespace {

class TTestTokenProvider final : public TAiPresets::ITokenProvider {
public:
    std::optional<TString> GetToken() final {
        return "test_token";
    }
};

void SetupAiPresets() {
    SetupPresetsInitializer(
        [colors = NConsoleClient::AutoColors(Cout)]() -> TAiPresets {
            TAiPresetsBuilder builder;

            builder.AddToken(
                "test_token",
                "Test token",
                std::make_shared<TTestTokenProvider>()
            );

            ui16 port = 8080;
            if (const auto portEnv = GetEnv("YDB_CLI_TEST_AI_PORT")) {
                port = FromString<ui16>(portEnv);
            }

            const TAiPresets::TEndpoint sampleEndpoint = {
                .ApiEndpoint = TStringBuilder() << "http://127.0.0.1:" << port << "/",
                .TokenProvider = "test_token",
            };

            const auto sampleOpenaiEndpoint = builder.AddEndpoint(sampleEndpoint
                .AppendApiPath("openai/v1")
                .SetInfo("Sample OpenAI models")
                .SetApiType(TAiPresets::EApiType::OpenAI));

            builder.AddPreset("sample_openai_model", sampleOpenaiEndpoint
                .SetInfo("Sample Open AI model")
                .SetModelName("sample-openai-model")
            );

            const auto sampleAnthropicEndpoint = builder.AddEndpoint(sampleEndpoint
                .AppendApiPath("anthropic/v1")
                .SetInfo("Sample anthropic models")
                .SetApiType(TAiPresets::EApiType::Anthropic));

            builder.AddPreset("sample_anthropic_model", sampleAnthropicEndpoint
                .SetInfo("Sample Anthropic model")
                .SetModelName("sample-anthropic-model")
            );

            builder.SetMetaInfo({
                .DefaultPreset = "sample_openai_model",
                .TokenDocs = ftxui::text("Test token documentation"),
            });

            return builder.Done();
        }
    );
}

int NewTestClient(int argc, char** argv) {
    NYdb::NConsoleClient::TClientSettings settings;
    settings.EnableSsl = false;
    settings.UseAccessToken = true;
    settings.UseDefaultTokenFile = true;
    settings.UseIamAuth = true;
    settings.UseStaticCredentials = true;
    settings.UseExportToYt = true;
    settings.UseOauth2TokenExchange = true;
    settings.MentionUserAccount = true;
    settings.EnableAiInteractive = true;
    settings.YdbDir = ".ydb";

    SetupAiPresets();

    auto commandsRoot = MakeHolder<TClientCommandRoot>(std::filesystem::path(argv[0]).stem().string(), settings);
    commandsRoot->Opts.SetTitle("YDB client");
    TClientCommand::TConfig config(argc, argv);
    return commandsRoot->Process(config);
}

} // anonymous namespace

TVector<NYdb::NTopic::ECodec> InitAllowedCodecs() {
    return TVector<NYdb::NTopic::ECodec>{
            NYdb::NTopic::ECodec::RAW,
            NYdb::NTopic::ECodec::ZSTD,
            NYdb::NTopic::ECodec::GZIP,
    };
}

} // namespace NYdb::NConsoleClient

int main(int argc, char **argv) {
    try {
        return NYdb::NConsoleClient::NewTestClient(argc, argv);
    } catch (const std::exception& e) {
        Cerr << e.what() << Endl;
        return EXIT_FAILURE;
    }
}
