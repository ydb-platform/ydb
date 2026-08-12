#include "nameresolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public/topic_parser.h>

#include <util/string/builder.h>

namespace NKikimr::NPQ::NNameResolver {

namespace {

// Matches CHECK_SET_VALID: Reason = Reason + (reason) + ". ";
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
    // Bare topic without path separators: "topic" (MinimalName in BuildFromLegacyName)
    if (topic.StartsWith("rt3.") || topic.Contains("--") || topic.Contains("@")) {
        return true;
    }
    return !topic.Contains("/");
}

// Append -mirrored-from-<dc> to the leaf topic name when Dc != LocalDc.
// Mirrors TDiscoveryConverter::BuildFromLegacyName / ParseModernPath.
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

TString JoinWithRoot(const TString& lbRoot, TStringBuf database, const TString& modernPath) {
    if (!lbRoot.empty()) {
        return CanonizePath(JoinPath({lbRoot, modernPath}));
    }
    if (!database.empty()) {
        return CanonizePath(JoinPath({TString{database}, modernPath}));
    }
    return modernPath;
}

// Mirrors TDiscoveryConverter::BuildFromLegacyName + public GetTopicPath / ConvertOldTopicName.
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
            // Same structural split as BuildFromLegacyName; ConvertOldTopicName on short part.
            modernPath = TString{NPersQueue::ConvertOldTopicName(std::string{shortLegacy})};
        }
        ApplyMirrorSuffix(modernPath, topicDc, localDc);
        return modernPath;
    }

    // Short name without rt3. — mirrors BuildFromLegacyName DC resolution.
    if (topicDc.empty()) {
        if (localDc.empty()) {
            return Fail(
                "Cannot determine DC: should specify either in topic name, Dc option or LocalDc option");
        }
        topicDc = localDc;
    }

    TString modernPath = TString{NPersQueue::ConvertOldTopicName(std::string{topic})};
    ApplyMirrorSuffix(modernPath, topicDc, localDc);
    return modernPath;
}

// Mirrors TDiscoveryConverter::ParseModernPath + BuildFromShortModernName DC check.
std::expected<TString, TString> TryParseModernTopicPath(
    TStringBuf topic,
    TStringBuf localDc,
    TStringBuf dc
) {
    TStringBuf topicDc = !dc.empty() ? dc : localDc;
    if (topicDc.empty()) {
        // BuildFromShortModernName when Dc falls back to LocalDc.
        return Fail("Cannot determine DC: should specify either with Dc option or LocalDc option");
    }

    TString modernPath{topic};
    ApplyMirrorSuffix(modernPath, topicDc, localDc);
    // BasicNameChecks for modern path is done in ResolveName (ForFederation) before classify;
    // pathAfterAccount in ParseModernPath cannot introduce '//' via mirror suffix alone.
    return modernPath;
}

struct TFederationContext {
    bool IsRootDb = false;
    TStringBuf Topic;
};

// Mirrors TDiscoveryConverter::BuildForFederation database/PQ-root classification.
TFederationContext ClassifyFederationTopic(TStringBuf database, TStringBuf topic, TStringBuf pqPrefix) {
    TFederationContext ctx;
    ctx.Topic = topic;
    ctx.IsRootDb = database.empty();

    if (!database.empty()) {
        if (IsPathPrefix(pqPrefix, database)) {
            ctx.IsRootDb = true;
            SkipPathPrefix(ctx.Topic, pqPrefix);
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
    const TStringBuf topicName = StripLeadingSlash(name);
    const TStringBuf databaseNorm = StripSlashes(database);
    const TStringBuf pqPrefix = StripSlashes(pqConfig.GetRoot());
    const TString& lbRoot = pqConfig.GetPQDiscoveryConfig().GetLbUserDatabaseRoot();

    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        if (!IsLegacyStyleName(topicName)) {
            return NormalizePath(TString{database}, TString{name});
        }
        auto parsed = TryParseLegacyToModernPath(topicName, localDc, dc);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        return JoinWithRoot(lbRoot, database, *parsed);
    }

    // Federation mode — same checks as TDiscoveryConverter::ForFederation / BuildForFederation.
    if (!BasicNameChecks(name)) {
        return std::unexpected(TStringBuilder() << "Bad topic name for federation: " << name);
    }
    if (topicName.empty()) {
        return Fail("Invalid topic path (only account provided?)");
    }
    if (topicName.EndsWith("/")) {
        return Fail("Invalid topic path or trailing '/'");
    }

    const auto ctx = ClassifyFederationTopic(databaseNorm, topicName, pqPrefix);
    if (ctx.Topic.empty()) {
        return Fail("Bad topic name (only account provided?)");
    }

    if (!ctx.IsRootDb) {
        // Relative modern path inside user database.
        // database is non-empty here: empty database is classified as root DB.
        auto parsed = TryParseModernTopicPath(ctx.Topic, localDc, dc);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        return CanonizePath(JoinPath({TString{database}, *parsed}));
    }

    // Root / PQ database: path with '/' is federation account/topic; otherwise legacy name.
    if (ctx.Topic.Contains("/")) {
        // EndsWith('/') and StripLeadingSlash already rejected empty account/rest forms
        // like "account/" and "/topic"; Contains('/') ⇒ TrySplit succeeds.
        TStringBuf account;
        TStringBuf rest;
        AFL_ENSURE(ctx.Topic.TrySplit("/", account, rest))("topic", ctx.Topic);
        AFL_ENSURE(!account.empty() && !rest.empty())("topic", ctx.Topic);
        auto parsed = TryParseModernTopicPath(ctx.Topic, localDc, dc);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        return JoinWithRoot(lbRoot, database, *parsed);
    }

    auto parsed = TryParseLegacyToModernPath(ctx.Topic, localDc, dc);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    return JoinWithRoot(lbRoot, database, *parsed);
}

} // namespace NKikimr::NPQ::NNameResolver
