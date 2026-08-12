#include "nameresolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public/topic_parser.h>

#include <util/string/builder.h>
#include <util/system/yassert.h>

namespace NKikimr::NPQ::NNameResolver {

namespace {

// Error reasons end with ". " for multi-part messages.
std::unexpected<TString> Fail(const TString& reason) {
    return std::unexpected(reason + ". ");
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

bool IsLegacyStyleName(TStringBuf topic) {
    // Full: rt3.<dc>--...
    // Short without rt3.: account--topic / account@dir--topic
    // Bare topic without path separators: "topic"
    if (topic.StartsWith("rt3.") || topic.Contains("--") || topic.Contains("@")) {
        return true;
    }
    return !topic.Contains("/");
}

bool IsExplicitLegacyName(TStringBuf topic) {
    // Unlike bare names, these are never relative modern paths inside a user DB.
    return topic.StartsWith("rt3.") || topic.Contains("--") || topic.Contains("@");
}

// Prefer LbRoot, else database; empty root leaves modernPath unchanged (no leading '/').
TString JoinWithRoot(TStringBuf lbRoot, TStringBuf database, TStringBuf modernPath) {
    if (!lbRoot.empty()) {
        return NormalizePath(lbRoot, modernPath);
    }
    if (!database.empty()) {
        return NormalizePath(database, modernPath);
    }
    return TString{modernPath};
}

TString JoinWithRoot(TStringBuf lbRoot, TStringBuf database, TString&& modernPath) {
    if (!lbRoot.empty()) {
        return NormalizePath(lbRoot, modernPath);
    }
    if (!database.empty()) {
        return NormalizePath(database, modernPath);
    }
    return std::move(modernPath);
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
    // Contains("-mirrored-from-") above ⇒ RSplit must succeed.
    Y_DEBUG_ABORT_UNLESS(path.TryRSplit("-mirrored-from-", withoutSuffix, cluster));
    if (cluster.empty()) {
        return Fail("Malformed mirrored topic path - expected to end with valid cluster name");
    }
    if (localDc == cluster) {
        return Fail("Local topic cannot contain '-mirrored-from' part");
    }
    return true;
}

// Modern path that is not already mirrored: apply mirror suffix when needed.
// Empty dc and localDc means "local" — no -mirrored-from- suffix (describe without DC).
std::expected<TString, TString> BuildModernTopicPath(
    TStringBuf topic,
    TStringBuf localDc,
    TStringBuf dc
) {
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

    if (!ctx.IsRootDb) {
        SkipPathPrefix(ctx.Topic, database);
    }
    return ctx;
}

} // namespace

std::expected<TString, TString> ResolveName(
    TStringBuf database,
    TStringBuf name,
    TStringBuf localDc,
    TStringBuf dc
) {
    const auto& pqConfig = AppData()->PQConfig;
    TStringBuf topicName = StripLeadingSlash(name);
    const TStringBuf databaseNorm = StripSlashes(database);
    const TStringBuf pqPrefix = StripSlashes(pqConfig.GetRoot());
    const TStringBuf lbRoot = StripSlashes(pqConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot());
    const TString& lbRootRaw = pqConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot();

    // Full path under database: /Root/account/topic + database /Root → account/topic.
    // Callers (e.g. describer) may pass database-prefixed absolute paths as-is.
    if (!databaseNorm.empty()) {
        SkipPathPrefix(topicName, databaseNorm);
    }

    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        if (!IsLegacyStyleName(topicName)) {
            return NormalizePath(database, topicName);
        }
        auto parsed = TryParseLegacyToModernPath(topicName, localDc, dc);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        return JoinWithRoot(lbRootRaw, database, std::move(*parsed));
    }

    // Federation mode.
    if (!BasicNameChecks(name)) {
        return std::unexpected(TStringBuilder() << "Bad topic name for federation: " << name);
    }
    if (topicName.empty()) {
        return Fail("Invalid topic path (only account provided?)");
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
            return JoinWithRoot(lbRootRaw, database, std::move(*parsed));
        }
        // database is non-empty here: empty database is classified as root DB.
        auto mirrored = IsAlreadyMirroredModernPath(ctx.Topic, localDc);
        if (!mirrored) {
            return std::unexpected(mirrored.error());
        }
        if (*mirrored) {
            // Keep topic as TStringBuf until the final join — no intermediate copy.
            return NormalizePath(database, ctx.Topic);
        }
        auto parsed = BuildModernTopicPath(ctx.Topic, localDc, dc);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        return NormalizePath(database, *parsed);
    }

    // Root-like database: path with '/' is federation account/topic; otherwise legacy name.
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
            return JoinWithRoot(lbRootRaw, database, ctx.Topic);
        }
        auto parsed = BuildModernTopicPath(ctx.Topic, localDc, dc);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        return JoinWithRoot(lbRootRaw, database, std::move(*parsed));
    }

    auto parsed = TryParseLegacyToModernPath(ctx.Topic, localDc, dc);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    // Bare names with a non-empty root-like request database stay under that database
    // (e.g. /Root/local) instead of LbRoot/local.
    if (!IsExplicitLegacyName(ctx.Topic) && !database.empty()) {
        return NormalizePath(database, *parsed);
    }
    return JoinWithRoot(lbRootRaw, database, std::move(*parsed));
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
