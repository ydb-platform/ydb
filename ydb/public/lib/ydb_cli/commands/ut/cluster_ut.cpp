#include <ydb/public/lib/ydb_cli/commands/ydb_cluster.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_root_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

namespace NYdb::NConsoleClient {
namespace {

void ValidateCommand(TVector<TString> args, TStringBuf profile = "{}") {
    TTempFileHandle profileFile;
    profileFile.Write(profile.data(), profile.size());
    TVector<TString> fullArgs = {"ydb", "-e", "grpc://localhost:2135", "--profile-file", profileFile.Name()};
    fullArgs.insert(fullArgs.end(), args.begin(), args.end());
    TVector<char*> argv;
    for (auto& arg : fullArgs) {
        argv.push_back(arg.Detach());
    }

    TClientSettings settings;
    settings.EnableSsl = false;
    settings.UseAccessToken = false;
    settings.UseDefaultTokenFile = false;
    settings.UseIamAuth = false;
    settings.UseExportToYt = false;
    settings.UseStaticCredentials = false;
    settings.MentionUserAccount = false;
    settings.UseOauth2TokenExchange = false;
    settings.EnableAiInteractive = false;
    settings.YdbDir = "ydb";

    TClientCommandRootCommon root("ydb", settings);
    TClientCommand::TConfig config(argv.size(), argv.data());
    root.Prepare(config);
    root.ExtractParams(config);
    root.Validate(config);

    // Validate the selected cluster command without running any requests.
    TClientCommand* command = &root;
    for (const auto& token : config.Tokens) {
        command = command->FindNextCommand(token);
        if (auto* cluster = dynamic_cast<NCluster::TCommandCluster*>(command)) {
            cluster->Validate(config);
        }
    }
}

const TVector<TVector<TString>> ClusterCommands = {
    {"bridge", "switchover", "--new-primary", "r1"},
    {"bridge", "list"},
    {"bridge", "set-database-mode", "ordinary", "--target-database", "/Root/db"},
    {"bootstrap", "--uuid", "test-cluster"},
    {"config", "fetch"},
    {"dump"},
    {"restore", "--input", "unused-directory"},
    {"diagnostics", "collect"},
};

constexpr TStringBuf TenantProfile = R"(
profiles:
  tenant:
    database: /Root/db
)";

} // namespace

Y_UNIT_TEST_SUITE(ClusterDatabaseValidation) {
    Y_UNIT_TEST(AcceptsOmittedDatabase) {
        for (const auto& command : ClusterCommands) {
            TVector<TString> args = {"admin", "cluster"};
            args.insert(args.end(), command.begin(), command.end());
            ValidateCommand(std::move(args));
        }
    }

    Y_UNIT_TEST(AcceptsRootDomain) {
        for (const TString& database : {"/Root", "/CustomDomain", "/Root/", "//Root///"}) {
            for (const auto& command : ClusterCommands) {
                TVector<TString> args = {"-d", database, "admin", "cluster"};
                args.insert(args.end(), command.begin(), command.end());
                ValidateCommand(std::move(args));
            }
        }
    }

    Y_UNIT_TEST(RejectsTenantDatabase) {
        for (const TString& database : {"/Root/db", "/Root/db/nested", "//Root//db//"}) {
            for (const auto& command : ClusterCommands) {
                TVector<TString> args = {"-d", database, "admin", "cluster"};
                args.insert(args.end(), command.begin(), command.end());
                UNIT_ASSERT_EXCEPTION_CONTAINS(ValidateCommand(std::move(args)), TMisuseException, "requires a root domain");
            }
        }
    }

    Y_UNIT_TEST(RejectsPathWithoutDomain) {
        for (const TString& database : {"/", "///"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                ValidateCommand({"-d", database, "admin", "cluster", "bridge", "list"}),
                TMisuseException, "requires a root domain");
        }
    }

    Y_UNIT_TEST(RejectsTenantDatabaseFromProfile) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ValidateCommand({"--profile", "tenant", "admin", "cluster", "bridge", "list"}, TenantProfile),
            TMisuseException, "Database '/Root/db' is not a root domain");
    }

    Y_UNIT_TEST(ExplicitDatabaseOverridesTenantProfile) {
        for (const TString& database : {"/Root", ""}) {
            ValidateCommand({"--profile", "tenant", "-d", database, "admin", "cluster", "bridge", "list"}, TenantProfile);
        }
    }

    Y_UNIT_TEST(OtherCommandsAcceptTenantDatabase) {
        ValidateCommand({"-d", "/Root/db", "monitoring", "healthcheck"});
        ValidateCommand({"-d", "/Root/db", "scheme", "ls"});
        ValidateCommand({"-d", "/Root/db", "admin", "database", "dump"});
        ValidateCommand({"-d", "/Root/db", "admin", "cluster", "config", "resolve", "--from-cluster"});
    }

    Y_UNIT_TEST(OfflineCommandsIgnoreDatabase) {
        ValidateCommand({"-d", "/Root/db", "admin", "cluster", "config", "migration", "cleanup-v2", "-f", "unused.yaml"});
    }
}

} // namespace NYdb::NConsoleClient
