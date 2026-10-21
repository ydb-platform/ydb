#include "nameresolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public/topic_parser.h>

#include <util/string/builder.h>
#include <util/system/yassert.h>

namespace NKikimr::NPQ::NNameResolver {

namespace {

// Error reasons end with '.' for stable client-visible messages.
std::unexpected<TString> Fail(const TString& reason) {
    return std::unexpected(reason + ".");
}

TStringBuf StripLeadingSlash(TStringBuf value) {
    value.SkipPrefix("/");
    return value;
}

TStringBuf StripSlashes(TStringBuf value) {
    value.SkipPrefix("/");
    value.ChopSuffix("/");
    return value;
}

bool IsPathPrefix(TStringBuf normPath, TStringBuf prefix) {
    auto res = normPath.SkipPrefix(prefix);
    if (!res) {
        return false;
    }
    if (normPath.empty()) {
        return true;
    }
    return normPath.SkipPrefix("/");
}

void SkipPathPrefix(TStringBuf& path, TStringBuf prefix) {
    // prefix is always slash-stripped by callers.
    // Exact match (path == prefix) clears path — same as IsPathPrefix.
    auto copy = path;
    if (!path.SkipPrefix(prefix)) {
        path = copy;
        return;
    }
    if (path.empty()) {
        return;
    }
    if (!path.SkipPrefix("/")) {
        path = copy;
    }
}

bool BasicNameChecks(TStringBuf name) {
    return !name.empty() && !name.Contains("//");
}

bool HasModernPathSeparator(TStringBuf topic) {
    // True for account/topic; false for bare names and short legacy with only '//'.
    for (size_t i = 0; i + 1 < topic.size(); ++i) {
        if (topic[i] == '/' && topic[i + 1] != '/' && (i == 0 || topic[i - 1] != '/')) {
            return true;
        }
    }
    return false;
}

bool IsExplicitLegacyName(TStringBuf topic) {
    // Unlike bare names, these are never relative modern paths inside a user DB.
    return !HasModernPathSeparator(topic)
        && (topic.StartsWith("rt3.") || topic.Contains("--") || topic.Contains("@"));
}

bool IsCleanRelativePath(TStringBuf path) {
    return !path.empty()
        && !path.StartsWith('/')
        && !path.EndsWith('/')
        && !path.Contains("//");
}

// Join non-empty slash-stripped absolute root with a relative modern path.
// Caller (JoinWithRoot) only invokes this when rootStripped is non-empty.
TString JoinStrippedRoot(TStringBuf rootStripped, TString modernPath) {
    Y_ABORT_UNLESS(!rootStripped.empty());
    if (modernPath.empty()) {
        return TStringBuilder() << '/' << rootStripped;
    }
    if (IsCleanRelativePath(modernPath)) {
        return TStringBuilder() << '/' << rootStripped << '/' << modernPath;
    }
    return NormalizePath(TStringBuilder() << '/' << rootStripped, modernPath);
}

// Prefer LbRoot (slash-stripped), else original database; empty root leaves modernPath as-is.
TString JoinWithRoot(TStringBuf lbRootStripped, TStringBuf database, TString modernPath) {
    if (!lbRootStripped.empty()) {
        return JoinStrippedRoot(lbRootStripped, std::move(modernPath));
    }
    if (!database.empty()) {
        const TStringBuf databaseNorm = StripSlashes(database);
        if (!databaseNorm.empty() && IsCleanRelativePath(modernPath)) {
            return TStringBuilder() << '/' << databaseNorm << '/' << modernPath;
        }
        return NormalizePath(database, modernPath);
    }
    return modernPath;
}

// Join request database with a relative topic path (user-DB / FCC modern paths).
TString JoinWithDatabase(TStringBuf database, TStringBuf databaseNorm, TStringBuf relative) {
    if (databaseNorm.empty()) {
        // Empty request database: relative is never empty here (legacy bare "" goes via
        // JoinWithRoot; modern paths always contain '/').
        if (IsCleanRelativePath(relative)) {
            return TStringBuilder() << '/' << relative;
        }
        return NormalizePath(TStringBuf{}, relative);
    }
    if (relative.empty()) {
        // name equal to database → absolute database path.
        if (!database.empty() && database.StartsWith('/') && !database.EndsWith('/') && !database.Contains("//")) {
            return TString{database};
        }
        return TStringBuilder() << '/' << databaseNorm;
    }
    if (IsCleanRelativePath(relative)) {
        return TStringBuilder() << '/' << databaseNorm << '/' << relative;
    }
    return NormalizePath(database, relative);
}

// Append -mirrored-from-<dc> to the leaf topic name when dc != localDc.
void ApplyMirrorSuffix(TString& modernPath, TStringBuf dc, TStringBuf localDc) {
    if (localDc.empty() || dc.empty() || dc == localDc) {
        return;
    }

    TStringBuf dirs;
    TStringBuf topicName;
    if (TStringBuf{modernPath}.TryRSplit("/", dirs, topicName)) {
        modernPath = TStringBuilder() << dirs << "/" << topicName << "-mirrored-from-" << dc;
    } else {
        modernPath = TStringBuilder() << modernPath << "-mirrored-from-" << dc;
    }
}

// Legacy rt3. / short / bare → modern path (via CorrectName / GetTopicPath / ConvertOldTopicName).
std::expected<TString, TString> TryParseLegacyToModernPath(
    TStringBuf topic,
    TStringBuf localDc,
    TStringBuf dc
) {
    TStringBuf topicDc = dc;

    if (topic.StartsWith("rt3.")) {
        TStringBuf nameWithoutPrefix = topic;
        nameWithoutPrefix.SkipPrefix("rt3.");
        TStringBuf nameDc;
        TStringBuf shortLegacy;
        if (!nameWithoutPrefix.TrySplit("--", nameDc, shortLegacy)) {
            return Fail("Malformed legacy style topic name: contains 'rt3.', but no '--'");
        }
        if (!topicDc.empty() && topicDc != nameDc) {
            return Fail("DC specified both in topic name and separate option and they mismatch");
        }
        topicDc = nameDc;
        if (topicDc.empty()) {
            return Fail(TStringBuilder() << "Internal error: Could not determine DC for topic: " << topic);
        }

        const std::string original{topic};
        TString modernPath;
        if (NPersQueue::CorrectName(original)) {
            modernPath = TString{NPersQueue::GetTopicPath(original)};
        } else {
            modernPath = TString{NPersQueue::ConvertOldTopicName(std::string{shortLegacy})};
        }
        ApplyMirrorSuffix(modernPath, topicDc, localDc);
        return modernPath;
    }

    // Short name without rt3.: dc from argument or localDc.
    // Empty dc and localDc → local path without -mirrored-from- (same as modern paths).
    if (topicDc.empty()) {
        topicDc = localDc;
    }

    TString modernPath = TString{NPersQueue::ConvertOldTopicName(std::string{topic})};
    if (!topicDc.empty()) {
        ApplyMirrorSuffix(modernPath, topicDc, localDc);
    }
    return modernPath;
}

// true = already mirrored (use path as-is), false = not a mirrored name.
std::expected<bool, TString> IsAlreadyMirroredModernPath(TStringBuf path, TStringBuf localDc) {
    if (!path.Contains("-mirrored-from-")) {
        if (path.Contains("mirrored-from")) {
            return Fail(
                "Federation topics cannot contain 'mirrored-from' in name unless this is a mirrored topic");
        }
        return false;
    }

    TStringBuf withoutSuffix;
    TStringBuf cluster;
    // Do not put TryRSplit inside Y_DEBUG_ABORT_UNLESS: in release builds the
    // expression is not evaluated and cluster stays empty.
    if (!path.TryRSplit("-mirrored-from-", withoutSuffix, cluster) || cluster.empty()) {
        return Fail("Malformed mirrored topic path - expected to end with valid cluster name");
    }
    if (localDc == cluster) {
        return Fail("Local topic cannot contain '-mirrored-from' part");
    }
    return true;
}

// Modern path that is not already mirrored: apply mirror suffix when needed.
// Empty dc and localDc means "local" — no -mirrored-from- suffix (describe without DC).
TString BuildModernTopicPath(TStringBuf topic, TStringBuf localDc, TStringBuf dc) {
    TString modernPath{topic};
    const TStringBuf topicDc = !dc.empty() ? dc : localDc;
    if (!topicDc.empty()) {
        ApplyMirrorSuffix(modernPath, topicDc, localDc);
    }
    return modernPath;
}

struct TFederationContext {
    bool IsRootDb = false;
    TStringBuf Topic;
};

// Classify topic relative to PQ root / LbRoot vs user database.
// database is a prefix of lbRoot (e.g. /Root vs /Root/Federation) → root-like.
// Caller already stripped the database prefix from topic when present.
TFederationContext ClassifyFederationTopic(
    TStringBuf database,
    TStringBuf topic,
    TStringBuf pqPrefix,
    TStringBuf lbRoot
) {
    TFederationContext ctx;
    ctx.Topic = topic;
    ctx.IsRootDb = database.empty();

    if (!database.empty()) {
        if (IsPathPrefix(pqPrefix, database)) {
            ctx.IsRootDb = true;
            SkipPathPrefix(ctx.Topic, pqPrefix);
        } else if (!lbRoot.empty() && IsPathPrefix(lbRoot, database)) {
            ctx.IsRootDb = true;
        }
    } else if (IsPathPrefix(ctx.Topic, pqPrefix)) {
        ctx.IsRootDb = true;
        SkipPathPrefix(ctx.Topic, pqPrefix);
    }
    return ctx;
}

TString AbsoluteDatabase(TStringBuf database, TStringBuf databaseNorm) {
    if (databaseNorm.empty()) {
        return {};
    }
    if (!database.empty() && database.StartsWith('/') && !database.EndsWith('/') && !database.Contains("//")) {
        return TString{database};
    }
    return TStringBuilder() << '/' << databaseNorm;
}

TString NavigateDatabaseFor(
    const TString& path,
    TStringBuf database,
    TStringBuf databaseNorm,
    TStringBuf lbRoot,
    bool isFederation
) {
    // Federation account topics live in the account tenant; FCC keeps the request database.
    if (isFederation) {
        // Prefer LbRoot; if unset, request database can be the federation/domain root
        // (e.g. describe account1/topic with Database=/Root and empty LbUserDatabaseRoot).
        const TStringBuf federationRoot = !lbRoot.empty() ? lbRoot : databaseNorm;
        if (!federationRoot.empty()) {
            TStringBuf rest = StripLeadingSlash(path);
            if (IsPathPrefix(rest, federationRoot)) {
                SkipPathPrefix(rest, federationRoot);
                TStringBuf account;
                TStringBuf topicRest;
                if (rest.TrySplit("/", account, topicRest) && !account.empty() && !topicRest.empty()) {
                    return TStringBuilder() << '/' << federationRoot << '/' << account;
                }
            }
        }
    }
    return AbsoluteDatabase(database, databaseNorm);
}

std::expected<TResolvedName, TString> MakeResolved(
    TString path,
    TStringBuf database,
    TStringBuf databaseNorm,
    TStringBuf lbRoot,
    bool isFederation
) {
    TResolvedName resolved;
    resolved.NavigateDatabase = NavigateDatabaseFor(path, database, databaseNorm, lbRoot, isFederation);
    resolved.Path = std::move(path);
    return resolved;
}

} // namespace

std::expected<TResolvedName, TString> ResolveName(
    TStringBuf database,
    TStringBuf name,
    TStringBuf localDc,
    TStringBuf dc
) {
    const auto& pqConfig = AppData()->PQConfig;
    const bool isFederation = !pqConfig.GetTopicsAreFirstClassCitizen();

    // Absolute paths may contain accidental '//' (e.g. JoinPath({"/Root/PQ/", name})).
    // Canonize those before BasicNameChecks; keep relative '//' rejected as before.
    TString canonName;
    if (name.StartsWith('/') && name.Contains("//")) {
        canonName = CanonizePath(TString{name});
        name = canonName;
    }

    TStringBuf topicName = StripLeadingSlash(name);
    const TStringBuf databaseNorm = StripSlashes(database);
    const TStringBuf pqPrefix = StripSlashes(pqConfig.GetRoot());
    const TStringBuf lbRoot = StripSlashes(pqConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot());

    // Reject trailing '/' before stripping PQ/database prefixes (exact PQ root is "/").
    if (isFederation && topicName.EndsWith("/")) {
        return Fail("Invalid topic path or trailing '/'");
    }

    // Absolute under PQ root first (more specific than request database).
    // Only in federation for classic PQ leaves (e.g. /Root/PQ/rt3.*). Never strip when the
    // request database is a proper child of the PQ/domain root — otherwise
    // /Root/test_db/topic with PQ Root=/Root becomes test_db/topic and rejoins as
    // /Root/test_db/test_db/topic.
    bool strippedPqPrefix = false;
    const bool underPq = isFederation && !pqPrefix.empty() && IsPathPrefix(topicName, pqPrefix);
    const bool databaseUnderPq = !databaseNorm.empty()
        && databaseNorm != pqPrefix
        && IsPathPrefix(databaseNorm, pqPrefix);
    if (underPq && !databaseUnderPq) {
        SkipPathPrefix(topicName, pqPrefix);
        strippedPqPrefix = true;
    } else if (!databaseNorm.empty()) {
        // Full path under database: /Root/account/topic + database /Root → account/topic.
        // Callers (e.g. describer) may pass database-prefixed absolute paths as-is.
        SkipPathPrefix(topicName, databaseNorm);
    }

    // Absolute under LbRoot (possibly after stripping a parent database) must not be
    // re-joined as LbRoot + "LbRootSuffix/...". E.g. db=/Root, LbRoot=/Root/LbAccount,
    // name=/Root/LbAccount/account/topic → account/topic (not LbAccount/account/topic).
    if (!lbRoot.empty()) {
        if (IsPathPrefix(topicName, lbRoot)) {
            SkipPathPrefix(topicName, lbRoot);
        } else if (!databaseNorm.empty()
            && databaseNorm != lbRoot
            && IsPathPrefix(lbRoot, databaseNorm)) {
            TStringBuf lbSuffix = lbRoot;
            SkipPathPrefix(lbSuffix, databaseNorm);
            if (!lbSuffix.empty() && IsPathPrefix(topicName, lbSuffix)) {
                SkipPathPrefix(topicName, lbSuffix);
            }
        }
    }

    auto wrap = [&](TString path) {
        return MakeResolved(std::move(path), database, databaseNorm, lbRoot, isFederation);
    };

    if (!isFederation) {
        // FCC: never interpret rt3. / -- / @ as a legacy name. A leaf like
        // TestSchemeList--test-topic-1 is a literal topic under the database.
        return wrap(JoinWithDatabase(database, databaseNorm, topicName));
    }

    // Federation mode.
    if (!BasicNameChecks(name)) {
        return std::unexpected(TStringBuilder() << "Bad topic name for federation: " << name);
    }
    if (topicName.empty()) {
        // Exact PQ root strip → same wording as ClassifyFederationTopic empty topic.
        return Fail(strippedPqPrefix
            ? "Bad topic name (only account provided?)"
            : "Invalid topic path (only account provided?)");
    }
    if (topicName.EndsWith("/")) {
        return Fail("Invalid topic path or trailing '/'");
    }

    const auto ctx = ClassifyFederationTopic(databaseNorm, topicName, pqPrefix, lbRoot);
    if (ctx.Topic.empty()) {
        return Fail("Bad topic name (only account provided?)");
    }

    if (!ctx.IsRootDb) {
        // Relative modern path inside user database.
        // Explicit legacy (rt3 / -- / @) still converts via LbRoot.
        if (IsExplicitLegacyName(ctx.Topic)) {
            auto parsed = TryParseLegacyToModernPath(ctx.Topic, localDc, dc);
            if (!parsed) {
                return std::unexpected(parsed.error());
            }
            return wrap(JoinWithRoot(lbRoot, database, std::move(*parsed)));
        }
        // database is non-empty here: empty database is classified as root DB.
        auto mirrored = IsAlreadyMirroredModernPath(ctx.Topic, localDc);
        if (!mirrored) {
            return std::unexpected(mirrored.error());
        }
        if (*mirrored) {
            return wrap(JoinWithDatabase(database, databaseNorm, ctx.Topic));
        }
        return wrap(JoinWithDatabase(database, databaseNorm, BuildModernTopicPath(ctx.Topic, localDc, dc)));
    }

    // Root-like database: path with '/' is federation account/topic; otherwise legacy name.
    // Absolute path under PQ with empty LbRoot → classic PrimaryPath under PQ (leaf as-is).
    // With LbRoot → modern path under LbRoot (federation, no miss→retry).
    if (strippedPqPrefix && lbRoot.empty() && !pqPrefix.empty()) {
        return wrap(JoinStrippedRoot(pqPrefix, TString{ctx.Topic}));
    }

    if (ctx.Topic.Contains("/")) {
        // EndsWith('/') and StripLeadingSlash already rejected empty account/rest forms
        // like "account/" and "/topic"; Contains('/') ⇒ TrySplit succeeds.
        TStringBuf account;
        TStringBuf rest;
        Y_DEBUG_ABORT_UNLESS(ctx.Topic.TrySplit("/", account, rest));
        Y_DEBUG_ABORT_UNLESS(!account.empty() && !rest.empty());

        auto mirrored = IsAlreadyMirroredModernPath(ctx.Topic, localDc);
        if (!mirrored) {
            return std::unexpected(mirrored.error());
        }
        if (*mirrored) {
            return wrap(JoinWithRoot(lbRoot, database, TString{ctx.Topic}));
        }
        return wrap(JoinWithRoot(lbRoot, database, BuildModernTopicPath(ctx.Topic, localDc, dc)));
    }

    auto parsed = TryParseLegacyToModernPath(ctx.Topic, localDc, dc);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    // Bare names with a non-empty root-like request database stay under that database
    // (e.g. /Root/local) instead of LbRoot/local.
    if (!IsExplicitLegacyName(ctx.Topic) && !database.empty()) {
        return wrap(JoinWithDatabase(database, databaseNorm, *parsed));
    }
    return wrap(JoinWithRoot(lbRoot, database, std::move(*parsed)));
}

std::optional<TFederationAccountTarget> TryFederationAccountTarget(
    TStringBuf path,
    TStringBuf federationRoot
) {
    if (federationRoot.empty() || path.empty()) {
        return std::nullopt;
    }

    // Prefer slash-stripped prefix checks over CanonizePath/JoinPath allocations.
    const TStringBuf root = StripSlashes(federationRoot);
    TStringBuf rest = StripLeadingSlash(path);
    if (root.empty() || !IsPathPrefix(rest, root)) {
        return std::nullopt;
    }
    SkipPathPrefix(rest, root);
    if (rest.empty()) {
        return std::nullopt;
    }

    TStringBuf account;
    TStringBuf topicRest;
    if (!rest.TrySplit("/", account, topicRest) || account.empty() || topicRest.empty()) {
        return std::nullopt;
    }

    TString canonPath;
    if (path.StartsWith('/') && !path.EndsWith('/') && !path.Contains("//")) {
        canonPath = TString{path};
    } else {
        canonPath = CanonizePath(TString{path});
    }

    return TFederationAccountTarget{
        .Path = std::move(canonPath),
        .AccountDatabase = TStringBuilder() << '/' << root << '/' << account,
    };
}

} // namespace NKikimr::NPQ::NNameResolver
