#include "rpc_common.h"
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/yaml_config/public/yaml_config.h>
#include <util/stream/str.h>

namespace NKikimr::NGRpcService {

    bool ResolveDatabaseConfigMetadata(IRequestCtxBase& request, TString& config) {
        if (!request.HasActivePathRewriting()) {
            return true;
        }
        try {
            const auto metadata = NYamlConfig::GetGenericMetadata(config);
            const auto* databaseMetadata = std::get_if<NYamlConfig::TDatabaseMetadata>(&metadata);
            if (!databaseMetadata || !databaseMetadata->Database) {
                // Preserve the existing config validator and main/cluster metadata.
                return true;
            }
            TString database;
            if (!ResolveRootSchemaPath(request, *databaseMetadata->Database, database)) {
                return false;
            }
            if (database == *databaseMetadata->Database) {
                return true;
            }
            auto document = NFyaml::TDocument::Parse(config);
            auto metadataMap = document.Root().Map().at("metadata").Map();
            metadataMap.pair_at("database").SetValue(document.CreateScalar(database));
            TStringStream output;
            output << document;
            config = output.Str();
            return true;
        } catch (const std::exception& error) {
            request.RaiseIssue(NYql::ExceptionToIssue(error));
            return false;
        }
    }

    TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveTopicSchemaPath(
        const IRequestCtxBaseMtSafe& request, const TString& path)
    {
        using NPathAliasing::EPathRewriteOutcome;
        using NPathAliasing::TResolvedSchemaPath;
        if (!request.HasActivePathRewriting() || path.empty()) {
            return TResolvedSchemaPath{path, EPathRewriteOutcome::NoMatch};
        }

        return ResolveTopicSchemaPath(*request.GetPathRewriteSettings().Context,
                                      request.GetLogicalDatabaseName().GetOrElse(""), request.GetDatabaseName().GetOrElse(""), path);
    }

    TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveConvertedTopicSchemaPath(
        const NPathAliasing::TPathContext& context, const TString& path,
        const TString& completeLegacyCandidate)
    {
        using NPathAliasing::EPathRewriteOutcome;
        using NPathAliasing::TResolvedSchemaPath;
        if (context.Empty() || path.empty()) {
            return TResolvedSchemaPath{path, EPathRewriteOutcome::NoMatch};
        }
        auto absoluteCandidate = CanonizePath(path);
        if (absoluteCandidate.empty()) {
            absoluteCandidate = "/";
        }
        auto legacyCandidate = CanonizePath(completeLegacyCandidate);
        if (legacyCandidate.empty() && !completeLegacyCandidate.empty()) {
            legacyCandidate = "/";
        }
        auto result = context.NormalizePath(path.StartsWith('/') ? absoluteCandidate : legacyCandidate);
        if (result.IsFail()) {
            return result;
        }
        if (path.StartsWith('/') && result->Outcome == EPathRewriteOutcome::NoMatch && legacyCandidate != absoluteCandidate) {
            result = context.NormalizePath(legacyCandidate);
            if (result.IsFail()) {
                return result;
            }
        }
        if (result->Outcome != EPathRewriteOutcome::Rewritten) {
            // Identity stops alias matching, not the owner's legacy resolution.
            result->Path = completeLegacyCandidate;
        }
        return result;
    }

    namespace {

        TConclusion<NPathAliasing::TResolvedSchemaPath> CheckTopicDatabase(
            TConclusion<NPathAliasing::TResolvedSchemaPath> result, const TString& rawPhysicalDatabase)
        {
            if (result.IsFail()) {
                return result;
            }
            const auto database = CanonizePath(rawPhysicalDatabase);
            const auto path = CanonizePath(result->Path);
            // Do not let the downstream parser reinterpret a resolved operand
            // outside this owner's database as a new relative topic spelling.
            if (!database.empty() && path != database && (path.size() <= database.size() || !path.StartsWith(database) || path[database.size()] != '/')) {
                return TConclusionStatus::Fail("Rewritten topic path is outside the request database");
            }
            return result;
        }

    } // namespace

    TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveTopicSchemaPath(
        const NPathAliasing::TPathContext& context, const TString& rawLogicalDatabase,
        const TString& rawPhysicalDatabase, const TString& path)
    {
        if (context.Empty() || path.empty()) {
            return NPathAliasing::TResolvedSchemaPath{path, NPathAliasing::EPathRewriteOutcome::NoMatch};
        }
        auto canonicalPath = CanonizePath(path);
        if (canonicalPath.empty()) {
            canonicalPath = "/";
        }
        const auto candidate = NKikimr::NormalizePath(CanonizePath(rawLogicalDatabase), canonicalPath);
        return CheckTopicDatabase(ResolveConvertedTopicSchemaPath(context, path, candidate), rawPhysicalDatabase);
    }

    TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveFstClassTopicSchemaPath(
        const IRequestCtxBaseMtSafe& request, const TString& path, const TString& defaultDatabase)
    {
        if (!request.HasActivePathRewriting() || path.empty()) {
            return NPathAliasing::TResolvedSchemaPath{path, NPathAliasing::EPathRewriteOutcome::NoMatch};
        }
        return ResolveFstClassTopicSchemaPath(*request.GetPathRewriteSettings().Context,
                                              request.GetLogicalDatabaseName().GetOrElse(defaultDatabase), request.GetDatabaseName().GetOrElse(defaultDatabase), path);
    }

    TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveFullTopicSchemaPath(
        const IRequestCtxBaseMtSafe& request, const TString& path)
    {
        using NPathAliasing::EPathRewriteOutcome;
        using NPathAliasing::TResolvedSchemaPath;
        if (!request.HasActivePathRewriting() || path.empty()) {
            return TResolvedSchemaPath{path, EPathRewriteOutcome::NoMatch};
        }
        const auto logicalDatabase = request.GetLogicalDatabaseName();
        const auto physicalDatabase = request.GetDatabaseName();
        const auto legacyPath = NPersQueue::GetFullTopicPath(logicalDatabase, path);
        auto candidate = CanonizePath(legacyPath);
        if (candidate.empty() && !legacyPath.empty()) {
            candidate = "/";
        }
        auto result = ResolveConvertedTopicSchemaPath(*request.GetPathRewriteSettings().Context, path, candidate);
        if (result.IsFail()) {
            return result;
        }
        if (result->Outcome != EPathRewriteOutcome::Rewritten && logicalDatabase == physicalDatabase) {
            // Preserve this owner's lexical behavior for misses and identity rules.
            result->Path = legacyPath;
        } else if (CanonizePath(NPersQueue::GetFullTopicPath(physicalDatabase, result->Path)) != result->Path) {
            return TConclusionStatus::Fail("Rewritten topic path is changed by the owner's path resolution");
        }
        return result;
    }

    TConclusion<NPathAliasing::TResolvedSchemaPath> ResolveFstClassTopicSchemaPath(
        const NPathAliasing::TPathContext& context, const TString& logicalDatabase,
        const TString& physicalDatabase, const TString& path)
    {
        if (context.Empty() || path.empty()) {
            return NPathAliasing::TResolvedSchemaPath{path, NPathAliasing::EPathRewriteOutcome::NoMatch};
        }
        NPersQueue::TTopicNamesConverterFactory factory(true, {}, {});
        const auto converter = factory.MakeDiscoveryConverter(path, Nothing(), {}, logicalDatabase);
        if (!converter->IsValid()) {
            return TConclusionStatus::Fail(converter->GetReason());
        }
        auto result = CheckTopicDatabase(
            ResolveConvertedTopicSchemaPath(context, path, converter->GetPrimaryPath()), physicalDatabase);
        if (result.IsFail()) {
            return result;
        }
        if (result->Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten || logicalDatabase != physicalDatabase) {
            const auto resolvedConverter = factory.MakeDiscoveryConverter(result->Path, Nothing(), {}, physicalDatabase);
            if (!resolvedConverter->IsValid() || CanonizePath(resolvedConverter->GetPrimaryPath()) != CanonizePath(result->Path)) {
                return TConclusionStatus::Fail("Rewritten topic path is changed by the owner's path resolution");
            }
        }
        return result;
    }

} // namespace NKikimr::NGRpcService
