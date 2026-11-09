#include <ydb/core/tx/schemeshard/eds_import/restore.h>
#include <ydb/core/tx/schemeshard/eds_import/limits.h>

#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {
namespace {

NKikimrSchemeOp::TModifyScheme PreparedEds(const TString& reference) {
    NKikimrSchemeOp::TModifyScheme scheme;
    scheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalDataSource);
    scheme.SetWorkingDir("/Destination/subtree");
    scheme.SetFailedOnAlreadyExists(false);
    auto& source = *scheme.MutableCreateExternalDataSource();
    source.SetName("renamed`source");
    source.SetSourceType("Ydb");
    source.SetLocation("grpc://localhost:2135");
    auto& auth = *source.MutableAuth()->MutableIam();
    auth.SetServiceAccountId("service-account");
    auth.SetResourceId("resource");
    auth.SetInitialTokenSecretName(reference);
    return scheme;
}

TString Query(TStringBuf database) {
    return TStringBuilder() << "-- database: \"" << database << "\"\n"
        << "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS `repaired` WITH (AUTH_METHOD = 'IAM');";
}

TString Reference(const TImportedSchemeResult& result) {
    UNIT_ASSERT(std::holds_alternative<NKikimrSchemeOp::TModifyScheme>(result));
    return std::get<NKikimrSchemeOp::TModifyScheme>(result).GetCreateExternalDataSource()
        .GetAuth().GetIam().GetInitialTokenSecretName();
}

void CheckFailure(const TString& reference, const TString& query, const TString& destination) {
    const auto compiled = PreparedEds(reference);
    TImportedSchemeResult result = compiled;
    auto status = Ydb::StatusIds::SUCCESS;
    RebindImportedEdsInitialToken(status, result, query, destination);
    UNIT_ASSERT(status == Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT(std::holds_alternative<TString>(result));
    const auto& error = std::get<TString>(result);
    UNIT_ASSERT(!error.empty());
    UNIT_ASSERT(!error.Contains("/Source"));
    UNIT_ASSERT(!error.Contains("diagnostic-canary"));
    UNIT_ASSERT_VALUES_EQUAL(compiled.GetCreateExternalDataSource().GetAuth().GetIam().GetInitialTokenSecretName(), reference);
}

} // anonymous

Y_UNIT_TEST_SUITE(ImportedEdsInitialToken) {
    Y_UNIT_TEST(RebindsTypedReferenceAndPreservesDestinationAndCompiledCache) {
        const auto compiled = PreparedEds("/Source/secrets/token");
        TImportedSchemeResult result = compiled;
        auto status = Ydb::StatusIds::SUCCESS;
        RebindImportedEdsInitialToken(status, result, Query("/Source"), "/Destination");
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Reference(result), "/Destination/secrets/token");
        const auto& mapped = std::get<NKikimrSchemeOp::TModifyScheme>(result);
        UNIT_ASSERT_VALUES_EQUAL(mapped.GetWorkingDir(), "/Destination/subtree");
        UNIT_ASSERT_VALUES_EQUAL(mapped.GetCreateExternalDataSource().GetName(), "renamed`source");
        UNIT_ASSERT_VALUES_EQUAL(mapped.GetCreateExternalDataSource().GetAuth().GetIam().GetServiceAccountId(), "service-account");
        UNIT_ASSERT_VALUES_EQUAL(mapped.GetCreateExternalDataSource().GetAuth().GetIam().GetResourceId(), "resource");
        UNIT_ASSERT_VALUES_EQUAL(compiled.GetCreateExternalDataSource().GetAuth().GetIam().GetInitialTokenSecretName(), "/Source/secrets/token");
    }

    Y_UNIT_TEST(SameDatabaseAndNestedOwningDatabase) {
        for (const auto& [source, destination] : TVector<std::pair<TString, TString>>{
                {"/DB", "/DB"}, {"/Source", "/Parent/TargetDB"}, {"/Source", "/"}})
        {
            TImportedSchemeResult result = PreparedEds(source + "/secrets/token");
            auto status = Ydb::StatusIds::SUCCESS;
            RebindImportedEdsInitialToken(status, result, Query(source), destination);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(Reference(result), (destination == "/" ? TString() : destination) + "/secrets/token");
        }
    }

    Y_UNIT_TEST(LegacySlashSpellingsPreserveDefinitionAndDatabaseBoundary) {
        for (const auto reference : {"/Source//secret", "/Source/secret/", "///Source///dir//secret///"}) {
            auto status = Ydb::StatusIds::SUCCESS;
            TImportedSchemeResult same = PreparedEds(reference);
            const auto original = std::get<NKikimrSchemeOp::TModifyScheme>(same).SerializeAsString();
            RebindImportedEdsInitialToken(status, same, Query("/Source"), "/Source");
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(std::get<NKikimrSchemeOp::TModifyScheme>(same).SerializeAsString(), original);
        }
        for (const auto& [reference, expected] : TVector<std::pair<TString, TString>>{
                {"/Source//secret", "/Destination//secret"},
                {"/Source/secret/", "/Destination/secret/"},
                {"///Source///dir//secret///", "/Destination///dir//secret///"}})
        {
            auto status = Ydb::StatusIds::SUCCESS;
            TImportedSchemeResult result = PreparedEds(reference);
            RebindImportedEdsInitialToken(status, result, Query("/Source"), "/Destination");
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(Reference(result), expected);
        }
        auto status = Ydb::StatusIds::SUCCESS;
        TImportedSchemeResult result = PreparedEds("//Parent///Source//secret/");
        RebindImportedEdsInitialToken(status, result, Query("/Parent/Source"), "/Destination");
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Reference(result), "/Destination//secret/");
        for (const auto reference : {"/Source///", "//SourceOther//secret", "/Source//../Other/secret",
                "/Source/./secret", "/Source//secret/..", "///Other/Source/secret"})
        {
            CheckFailure(reference, Query("/Source"), "/Source");
            CheckFailure(reference, Query("/Source"), "/Destination");
        }
    }

    Y_UNIT_TEST(NamedAndNonIamResultsAreExactNoOps) {
        auto status = Ydb::StatusIds::SUCCESS;
        TImportedSchemeResult result = PreparedEds("named-token");
        const auto named = std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString();
        RebindImportedEdsInitialToken(status, result, "not metadata", "");
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString(), named);
        auto noAuth = PreparedEds("/Source/token");
        noAuth.MutableCreateExternalDataSource()->MutableAuth()->MutableNone();
        result = noAuth;
        const auto none = noAuth.SerializeAsString();
        RebindImportedEdsInitialToken(status, result, "not metadata", "");
        UNIT_ASSERT_VALUES_EQUAL(std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString(), none);
        noAuth.MutableCreateExternalDataSource()->ClearAuth();
        result = noAuth;
        const auto absent = noAuth.SerializeAsString();
        RebindImportedEdsInitialToken(status, result, "not metadata", "");
        UNIT_ASSERT_VALUES_EQUAL(std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString(), absent);
        noAuth.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTransfer);
        result = noAuth;
        const auto other = noAuth.SerializeAsString();
        RebindImportedEdsInitialToken(status, result, "not metadata", "");
        UNIT_ASSERT_VALUES_EQUAL(std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString(), other);
    }

    Y_UNIT_TEST(FailedStatusesAndUpstreamErrorsRemainUnchanged) {
        for (auto status : {Ydb::StatusIds::BAD_REQUEST, Ydb::StatusIds::SCHEME_ERROR,
                Ydb::StatusIds::UNAUTHORIZED, Ydb::StatusIds::UNAVAILABLE,
                Ydb::StatusIds::OVERLOADED, Ydb::StatusIds::TIMEOUT,
                Ydb::StatusIds::CANCELLED, Ydb::StatusIds::INTERNAL_ERROR})
        {
            const auto originalStatus = status;
            TImportedSchemeResult result = PreparedEds("/Source/token");
            const auto before = std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString();
            RebindImportedEdsInitialToken(status, result, "bad metadata", "");
            UNIT_ASSERT(status == originalStatus);
            UNIT_ASSERT_VALUES_EQUAL(std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString(), before);
            result = TString("upstream error");
            RebindImportedEdsInitialToken(status, result, "bad metadata", "");
            UNIT_ASSERT(status == originalStatus);
            UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(result), "upstream error");
        }
    }

    Y_UNIT_TEST(RejectsMissingAmbiguousAndForeignMetadata) {
        for (const auto& query : TVector<TString>{
                "CREATE EXTERNAL DATA SOURCE `source`; -- database: \"/Source\"",
                "-- database: \"/SourceX\nCREATE EXTERNAL DATA SOURCE `source`;",
                "-- database: \"/Source\"\n-- database: \"/Other\"\nCREATE EXTERNAL DATA SOURCE `source`;",
                "/* -- database: \"/Source\" */\nCREATE EXTERNAL DATA SOURCE `source`;",
                Query("/SourceOther"), Query("/Source/..")})
        {
            CheckFailure("/Source/token", query, "/Destination");
        }
        for (const auto reference : {"/Other/token", "/SourceOther/token", "/Source/../Other/token", "/Source"}) {
            CheckFailure(reference, Query("/Source"), "/Destination");
        }
        for (const auto destination : {"", "relative", "/Destination/.."}) {
            CheckFailure("/Source/token", Query("/Source"), destination);
        }
    }

    Y_UNIT_TEST(BomAndFlagsDoNotReinterpretDecodedReference) {
        const TString reference = "/Source/my'secret\\path/путь😀";
        for (const auto prefix : {"", "--!ansi_lexer\n", "\xEF\xBB\xBF", "\xEF\xBB\xBF--!ansi_lexer\n"}) {
            TImportedSchemeResult result = PreparedEds(reference);
            auto status = Ydb::StatusIds::SUCCESS;
            const TString query = TStringBuilder() << prefix << Query("/Source");
            RebindImportedEdsInitialToken(status, result, query, "/Destination");
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(Reference(result), "/Destination/my'secret\\path/путь😀");
        }
    }

    Y_UNIT_TEST(PersistedPreparedPayloadDoesNotNeedRebindingOnRetry) {
        TImportedSchemeResult result = PreparedEds("/Source/token");
        auto status = Ydb::StatusIds::SUCCESS;
        RebindImportedEdsInitialToken(status, result, Query("/Source"), "/Destination");
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        NKikimrSchemeOp::TModifyScheme persisted;
        UNIT_ASSERT(persisted.ParseFromString(std::get<NKikimrSchemeOp::TModifyScheme>(result).SerializeAsString()));
        UNIT_ASSERT_VALUES_EQUAL(persisted.GetCreateExternalDataSource().GetAuth().GetIam().GetInitialTokenSecretName(), "/Destination/token");
        // Each fresh preparation starts from original SQL; existing prepared retries reuse the persisted payload.
        TImportedSchemeResult fresh = PreparedEds("/Source/token");
        RebindImportedEdsInitialToken(status, fresh, Query("/Source"), "/Destination");
        UNIT_ASSERT_VALUES_EQUAL(Reference(fresh), "/Destination/token");
        result = persisted;
        RebindImportedEdsInitialToken(status, result, Query("/Source"), "/Destination");
        UNIT_ASSERT(status == Ydb::StatusIds::BAD_REQUEST); // Accidental repeat never adds another prefix.
        UNIT_ASSERT_VALUES_EQUAL(persisted.GetCreateExternalDataSource().GetAuth().GetIam().GetInitialTokenSecretName(), "/Destination/token");
    }

    Y_UNIT_TEST(ProducerDerivedBoundsDoNotTruncate) {
        const size_t limit = NExternalDataSource::MAX_PROTOBUF_SIZE;
        const TString reference = "/Source/" + TString(limit - 128, 'x');
        const auto compiled = PreparedEds(reference);
        UNIT_ASSERT(compiled.GetCreateExternalDataSource().GetAuth().ByteSizeLong() <= limit);
        TImportedSchemeResult result = compiled;
        auto status = Ydb::StatusIds::SUCCESS;
        RebindImportedEdsInitialToken(status, result, Query("/Source"), "/Dest");
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(Reference(result) == "/Dest/" + TString(limit - 128, 'x'));
        CheckFailure("/Source/" + TString(limit, 'x'), Query("/Source"), "/Destination");
        CheckFailure("/Source/token", Query("/Source"), "/" + TString(limit, 'x'));
        CheckFailure("/Source/token", "-- " + TString(limit + 1024, 'x') + "\n" + Query("/Source"), "/Destination");
        const TString largeDatabase = "/" + TString(limit - 256, 'x');
        result = PreparedEds(largeDatabase + "/token");
        status = Ydb::StatusIds::SUCCESS;
        RebindImportedEdsInitialToken(status, result, Query(largeDatabase), "/Dest");
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Reference(result), "/Dest/token");
    }

    Y_UNIT_TEST(SqlBodyIsNotTokenizedOrParsedAgain) {
        TImportedSchemeResult result = PreparedEds("/Source/token");
        auto status = Ydb::StatusIds::SUCCESS;
        const TString query = Query("/Source") + TString(4 * NExternalDataSource::MAX_PROTOBUF_SIZE, 'x');
        RebindImportedEdsInitialToken(status, result, query, "/Destination");
        UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Reference(result), "/Destination/token");
    }
}

} // namespace NKikimr::NSchemeShard
