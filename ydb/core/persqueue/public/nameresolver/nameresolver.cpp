#include "nameresolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public/topic_parser.h>

#include <util/generic/maybe.h>
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
    // Empty LbRoot: classic discovery PrimaryPath is PQ root + full legacy leaf
    // (TDiscoveryConverter::BuildFromLegacyName / BuildFromFederationPath).
    // Already-stripped /Root/PQ/rt3... paths and rt3.* names stay as that leaf.
    // With LbRoot → modern path under LbRoot (federation, no miss→retry).
    if (lbRoot.empty() && !pqPrefix.empty()) {
        if (strippedPqPrefix || ctx.Topic.StartsWith("rt3.")) {
            return wrap(JoinStrippedRoot(pqPrefix, TString{ctx.Topic}));
        }
        const TStringBuf topicDc = !dc.empty() ? dc : localDc;
        if (!topicDc.empty()) {
            return wrap(JoinStrippedRoot(
                pqPrefix,
                TString{NPersQueue::BuildFullTopicName(TString{ctx.Topic}, TString{topicDc})}));
        }
        if (IsExplicitLegacyName(ctx.Topic)) {
            return wrap(JoinStrippedRoot(pqPrefix, TString{ctx.Topic}));
        }
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

namespace {

#define CHECK_SET_VALID(cond, reason, statement) \
    if (!(cond)) {                               \
        Valid = false;                           \
        Reason = Reason + (reason) + ". ";       \
        statement;                               \
    }

TString StripLeadSlash(const TString& path) {
    if (!path.StartsWith("/")) {
        return path;
    }
    return path.substr(1);
}

void NormalizeAsFullPath(TString& path) {
    if (!path.empty() && !path.StartsWith("/")) {
        path = TString("/") + path;
    }
}

void ConverterSkipPathPrefix(TStringBuf& path, const TStringBuf& prefix) {
    auto copy = path;
    if (prefix.EndsWith('/')) {
        path.SkipPrefix(prefix);
    } else {
        const bool skip = path.SkipPrefix(prefix) && path.SkipPrefix("/");
        if (!skip) {
            path = copy;
        }
    }
}

TString NormalizePqPrefix(const TString& pqRoot) {
    TStringBuf prefix(pqRoot);
    prefix.SkipPrefix("/");
    prefix.ChopSuffix("/");
    return TString{prefix};
}

// Port of TDiscoveryConverter + TTopicNameConverter (BuildFrom*, BuildInternals, ForFederation create).
struct TNameBuilder {
    bool FstClass = false;
    bool Valid = true;
    TString Reason;

    TString OriginalTopic;
    TString Dc;
    TString LocalDc;
    TMaybe<TString> Database;
    TString PQPrefix;

    TString PrimaryPath;
    TString FullModernPath;
    TString ModernName;
    TString FullModernName;
    TString ShortLegacyName;
    TString FullLegacyName;
    TString LegacyProducer;
    TString LegacyLogtype;
    TMaybe<TString> LbPath;
    TMaybe<TString> Account_;
    TMaybe<TString> SecondaryPath;

    TString ClientsideName;
    TString ShortClientsideName;
    TString Account;
    TString InternalName;

    bool BuildFromShortModernName() {
        CHECK_SET_VALID(
            !ModernName.empty(), TStringBuilder() << "Could not parse topic name: " << OriginalTopic, return false);

        TStringBuf pathBuf(ModernName);
        TStringBuilder legacyName;
        TString lbPath;
        legacyName << Account_.GetOrElse("undef-account");
        if (Account_.Defined()) {
            lbPath = NKikimr::JoinPath({*Account_, ModernName});
        }

        TStringBuf fst, snd, logtype;
        auto res = pathBuf.TryRSplit("/", fst, logtype);
        if (!res) {
            logtype = pathBuf;
        } else {
            pathBuf = fst;
            while (true) {
                res = pathBuf.TrySplit("/", fst, snd);
                if (res) {
                    legacyName << "@" << fst;
                    pathBuf = snd;
                } else {
                    legacyName << "@" << pathBuf;
                    break;
                }
            }
        }
        const TString legacyProducer = legacyName;
        legacyName << "--" << logtype;
        ShortLegacyName = legacyName;
        if (Dc.empty()) {
            Dc = LocalDc;
            CHECK_SET_VALID(!LocalDc.empty(),
                "Cannot determine DC: should specify either with Dc option or LocalDc option",
                return false);
        }
        LbPath = lbPath;
        FullLegacyName = TStringBuilder() << "rt3." << Dc << "--" << ShortLegacyName;
        LegacyProducer = legacyProducer;
        LegacyLogtype = logtype;
        return true;
    }

    bool ParseModernPath(const TStringBuf& path) {
        TStringBuilder pathAfterAccount;
        if (!Dc.empty() && !LocalDc.empty() && Dc != LocalDc) {
            TStringBuf directories, topicName;
            if (path.TrySplit("/", directories, topicName)) {
                pathAfterAccount << directories << "/" << topicName << "-mirrored-from-" << Dc;
            } else {
                pathAfterAccount << path << "-mirrored-from-" << Dc;
            }
        } else {
            pathAfterAccount << path;
        }
        CHECK_SET_VALID(BasicNameChecks(pathAfterAccount), "Bad topic name", return false);
        ModernName = path;
        FullModernName = pathAfterAccount;
        if (Account_.Defined()) {
            return BuildFromShortModernName();
        }
        return true;
    }

    bool TryParseModernMirroredPath(TStringBuf path) {
        if (!path.Contains("-mirrored-from-")) {
            CHECK_SET_VALID(!path.Contains("mirrored-from"),
                "Federation topics cannot contain 'mirrored-from' in name unless this is a mirrored topic",
                return false);
            return false;
        }
        TStringBuf fst, snd;
        auto res = path.TryRSplit("-mirrored-from-", fst, snd);
        CHECK_SET_VALID(res, "Malformed mirrored topic path - expected to end with '-mirrored-from-<cluster>'",
            return false);
        CHECK_SET_VALID(!snd.empty(), "Malformed mirrored topic path - expected to end with valid cluster name",
            return false);
        Dc = snd;
        CHECK_SET_VALID(LocalDc != Dc, "Local topic cannot contain '-mirrored-from' part", return false);
        FullModernName = path;
        ModernName = fst;
        if (Account_.Defined()) {
            return BuildFromShortModernName();
        }
        return true;
    }

    bool BuildFromFederationPath(const TString& rootPrefix) {
        TStringBuf topic(OriginalTopic);
        LbPath = OriginalTopic;
        TStringBuf fst, snd;
        auto res = topic.TrySplit("/", fst, snd);
        CHECK_SET_VALID(res, TStringBuilder() << "Could not split federation path: " << OriginalTopic, return false);
        Account_ = fst;

        if (!ParseModernPath(snd)) {
            return false;
        }
        if (!BuildFromShortModernName()) {
            return false;
        }
        CHECK_SET_VALID(
            !FullLegacyName.empty(),
            TStringBuilder() << "Internal error: couldn't build legacy-style name for topic " << OriginalTopic,
            return false);

        PrimaryPath = NKikimr::JoinPath({rootPrefix, FullLegacyName});
        NormalizeAsFullPath(PrimaryPath);
        return true;
    }

    bool BuildFromLegacyName(const TString& rootPrefix, bool forceFullName = false) {
        TStringBuf topic(OriginalTopic);
        const bool hasDcInName = topic.Contains("rt3.");
        TStringBuf fst, snd;
        Account_ = Nothing();
        TString shortLegacyName, fullLegacyName;
        if (forceFullName) {
            CHECK_SET_VALID(hasDcInName,
                TStringBuilder() << "Invalid topic name - " << OriginalTopic
                                 << " - expected legacy-style name like rt3.<dc>--<account>--<topic>",
                return false);
        }
        if (Dc.empty() && !hasDcInName) {
            CHECK_SET_VALID(!FstClass,
                TStringBuilder() << "Internal error: FirstClass mode enabled, but trying to parse Legacy-style name: "
                                 << OriginalTopic,
                return false);
            CHECK_SET_VALID(!LocalDc.empty(),
                "Cannot determine DC: should specify either in topic name, Dc option or LocalDc option",
                return false);
            Dc = LocalDc;
        }

        if (hasDcInName) {
            fullLegacyName = topic;
            auto res = topic.SkipPrefix("rt3.");
            CHECK_SET_VALID(res, "Malformed full legacy topic name", return false);
            res = topic.TrySplit("--", fst, snd);
            CHECK_SET_VALID(res, "Malformed legacy style topic name: contains 'rt3.', but no '--'.", return false);
            CHECK_SET_VALID(Dc.empty() || Dc == fst,
                "DC specified both in topic name and separate option and they mismatch", return false);
            Dc = fst;
            topic = snd;
        } else {
            CHECK_SET_VALID(!Dc.empty(),
                TStringBuilder() << "Internal error: Could not determine DC (despite beleiving the name contins one) for topic "
                                 << OriginalTopic,
                return false);
            fullLegacyName = TStringBuilder() << "rt3." << Dc << "--" << topic;
        }
        shortLegacyName = topic;
        TStringBuilder modernName, fullModernName;
        auto res = topic.TryRSplit("--", fst, snd);
        if (res) {
            LegacyProducer = fst;
            LegacyLogtype = snd;
        } else {
            LegacyProducer = "unknown";
            LegacyLogtype = topic;
        }
        while (true) {
            auto splitRes = topic.TrySplit("@", fst, snd);
            if (!splitRes) {
                break;
            }
            if (!Account_.Defined()) {
                Account_ = fst;
            } else {
                modernName << fst << "/";
            }
            topic = snd;
        }
        fullModernName << modernName;
        TString topicName;
        res = topic.TrySplit("--", fst, snd);
        if (res) {
            if (!Account_.Defined()) {
                Account_ = fst;
            } else {
                modernName << fst << "/";
                fullModernName << fst << "/";
            }
            topicName = snd;
        } else {
            if (!Account_.Defined()) {
                Account_ = "";
            }
            topicName = topic;
        }
        modernName << topicName;
        CHECK_SET_VALID(!Dc.empty(),
            TStringBuilder() << "Internal error: Could not determine DC for topic: " << OriginalTopic,
            return false);

        const bool isMirrored = (!LocalDc.empty() && Dc != LocalDc);
        if (isMirrored) {
            fullModernName << topicName << "-mirrored-from-" << Dc;
        } else {
            fullModernName << topicName;
        }
        CHECK_SET_VALID(!fullLegacyName.empty(),
            TStringBuilder() << "Could not form a full legacy name for topic: " << OriginalTopic,
            return false);

        ShortLegacyName = shortLegacyName;
        FullLegacyName = fullLegacyName;
        PrimaryPath = NKikimr::JoinPath({rootPrefix, fullLegacyName});
        NormalizeAsFullPath(PrimaryPath);
        FullModernName = fullModernName;
        ModernName = modernName;
        LbPath = NKikimr::JoinPath({*Account_, modernName});
        return true;
    }

    void BuildFstClassNames() {
        TStringBuf normTopic(OriginalTopic);
        normTopic.SkipPrefix("/");
        if (Database.Defined()) {
            TStringBuf normDb(*Database);
            normDb.SkipPrefix("/");
            normDb.ChopSuffix("/");
            normTopic.SkipPrefix(normDb);
            normTopic.SkipPrefix("/");
            PrimaryPath = NKikimr::JoinPath({TString(normDb), TString(normTopic)});
        } else {
            PrimaryPath = TString(normTopic);
            Database = "";
        }
        NormalizeAsFullPath(PrimaryPath);
        FullModernPath = PrimaryPath;
        CHECK_SET_VALID(
            !FullModernPath.empty(),
            TStringBuilder() << "Internal error: could not build modern name for first class topic: " << OriginalTopic,
            return);
    }

    void BuildForFederation(const TStringBuf& databaseBuf, TStringBuf topicPath) {
        topicPath.SkipPrefix("/");
        CHECK_SET_VALID(!topicPath.empty(), "Invalid topic path (only account provided?)", return);
        CHECK_SET_VALID(!topicPath.EndsWith("/"), "Invalid topic path or trailing '/'", return);
        if (FstClass) {
            OriginalTopic = topicPath;
            Database = databaseBuf;
            BuildFstClassNames();
            return;
        }
        bool isRootDb = databaseBuf.empty();
        TString root;
        if (!databaseBuf.empty()) {
            if (IsPathPrefix(PQPrefix, databaseBuf)) {
                isRootDb = true;
                root = PQPrefix;
                ConverterSkipPathPrefix(topicPath, PQPrefix);
            }
        } else if (IsPathPrefix(topicPath, PQPrefix)) {
            isRootDb = true;
            ConverterSkipPathPrefix(topicPath, PQPrefix);
            root = PQPrefix;
        }
        if (!isRootDb) {
            ConverterSkipPathPrefix(topicPath, databaseBuf);
            Database = databaseBuf;
        }
        CHECK_SET_VALID(!topicPath.empty(), "Bad topic name (only account provided?)", return);

        OriginalTopic = topicPath;
        if (!isRootDb && Database.Defined()) {
            auto parsed = TryParseModernMirroredPath(topicPath);
            if (!Valid) {
                return;
            }
            if (!parsed) {
                if (!ParseModernPath(topicPath)) {
                    return;
                }
            }
            CHECK_SET_VALID(
                !FullModernName.empty(),
                TStringBuilder() << "Internal error: Could not parse topic name (federation path was assumed)" << OriginalTopic,
                return);

            PrimaryPath = NKikimr::JoinPath({*Database, FullModernName});
            NormalizeAsFullPath(PrimaryPath);
            if (!FullLegacyName.empty()) {
                SecondaryPath = NKikimr::JoinPath({PQPrefix, FullLegacyName});
                NormalizeAsFullPath(SecondaryPath.GetRef());
            }
            if (!BuildFromShortModernName()) {
                return;
            }
        } else {
            if (root.empty()) {
                root = PQPrefix;
            }
            if (topicPath.find("/") != TString::npos) {
                Y_UNUSED(BuildFromFederationPath(root));
            } else {
                Y_UNUSED(BuildFromLegacyName(root));
            }
        }
    }

    void SetDatabaseFromConfig(const TString& database) {
        if (database.empty()) {
            return;
        }
        AFL_ENSURE(!FullModernName.empty())("database", database)("original_topic", OriginalTopic);
        if (!SecondaryPath.Defined()) {
            SecondaryPath = NKikimr::JoinPath({database, FullModernName});
            NormalizeAsFullPath(SecondaryPath.GetRef());
        }
        FullModernPath = SecondaryPath.GetRef();
    }

    void BuildInternals(const NKikimrPQ::TPQTabletConfig& config) {
        if (!config.GetFederationAccount().empty()) {
            Account = config.GetFederationAccount();
        } else {
            Account = Account_.GetOrElse("");
        }
        TStringBuf path = config.GetTopicPath();
        TStringBuf db = config.GetYdbDatabasePath();
        path.SkipPrefix("/");
        db.SkipPrefix("/");
        db.ChopSuffix("/");
        Database = db;
        if (FstClass) {
            AFL_ENSURE(!path.empty())("topic_path", config.GetTopicPath())("database", db);
            path.SkipPrefix(db);
            path.SkipPrefix("/");
            ClientsideName = path;
            ShortClientsideName = path;
            FullModernName = path;
            InternalName = PrimaryPath;
        } else {
            SetDatabaseFromConfig(*Database);
            AFL_ENSURE(!FullLegacyName.empty())("topic_path", config.GetTopicPath())("database", db);
            ClientsideName = FullLegacyName;
            ShortClientsideName = ShortLegacyName;
            const auto& producer = config.GetProducer();
            if (!producer.empty()) {
                LegacyProducer = producer;
                LegacyLogtype = config.GetTopic();
            }
            if (LegacyProducer.empty()) {
                LegacyProducer = Account;
            }
            AFL_ENSURE(!FullModernName.empty())("topic_path", config.GetTopicPath())("database", db);
            InternalName = FullLegacyName;
        }
    }

    void InitFromTabletConfig(
        bool firstClass,
        const TString& pqNormalizedPrefix,
        const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
        const TString& ydbDatabaseRootOverride)
    {
        PQPrefix = pqNormalizedPrefix;
        auto name = pqTabletConfig.GetTopicName();
        auto path = pqTabletConfig.GetTopicPath();
        if (name.empty()) {
            AFL_ENSURE(!path.empty())("topic_path", path)("topic_name", name);
            TStringBuf pathBuf(path), fst, snd;
            auto res = pathBuf.TryRSplit("/", fst, snd);
            AFL_ENSURE(res)("topic_path", path);
            name = snd;
        } else if (path.empty()) {
            path = name;
        }
        Y_UNUSED(name);
        if (!ydbDatabaseRootOverride.empty()) {
            TStringBuf pathBuf(path);
            TStringBuf dbRoot(ydbDatabaseRootOverride);
            auto res_ = pathBuf.SkipPrefix(dbRoot);
            if (res_) {
                dbRoot.SkipPrefix("/");
                pathBuf.SkipPrefix("/");
                TStringBuf acc, rest;
                if (pathBuf.TrySplit("/", acc, rest)) {
                    Database = NKikimr::JoinPath({TString(dbRoot), TString(acc)});
                } else {
                    Database = TString(dbRoot);
                }
            }
        }
        if (!Database.Defined()) {
            TStringBuf dbPath = pqTabletConfig.GetYdbDatabasePath();
            dbPath.SkipPrefix("/");
            dbPath.ChopSuffix("/");
            Database = dbPath;
        }
        FstClass = firstClass;
        Dc = pqTabletConfig.GetDC();
        const auto& acc = pqTabletConfig.GetFederationAccount();
        if (!acc.empty()) {
            Account_ = acc;
        }
        if (FstClass) {
            OriginalTopic = pqTabletConfig.GetTopicPath();
            BuildFstClassNames();
        } else {
            BuildForFederation(*Database, path);
        }
        if (Valid) {
            BuildInternals(pqTabletConfig);
        }
    }

    void InitForCreate(
        const TString& pqRoot,
        const TString& ydbTestDatabaseRoot,
        const TString& schemeName,
        const TString& schemeDir,
        const TString& database,
        bool isLocal,
        const TString& localDc,
        const TString& federationAccount)
    {
        bool isRoot = false;
        TStringBuf normDb(database);
        TStringBuf normRoot(pqRoot);
        TStringBuf normDir(schemeDir);

        normDb.ChopSuffix("/");
        normRoot.SkipPrefix("/");
        normDir.SkipPrefix("/");
        normDb.SkipPrefix("/");

        if (!ydbTestDatabaseRoot.empty()) {
            TStringBuf dbRoot(ydbTestDatabaseRoot);
            dbRoot.SkipPrefix("/");
            if (normDir.StartsWith(dbRoot)) {
                normDb = dbRoot;
            }
        }

        if (normDb.empty()) {
            isRoot = IsPathPrefix(normDir, normRoot);
        } else if (!normRoot.empty() && IsPathPrefix(normRoot, normDb)) {
            isRoot = true;
        }

        Database = normDb;

        if (isRoot) {
            if (normDir != normRoot) {
                Valid = false;
                Reason = TStringBuilder() << "Topics with database '" << database << "' should be created in pqRoot: "
                                           << pqRoot;
                return;
            }

            OriginalTopic = schemeName;
            if (!BuildFromLegacyName(TString(normRoot), true)) {
                return;
            }
            if (Valid && !isLocal && Dc == localDc) {
                Valid = false;
                Reason = TStringBuilder() << "Topic '" << schemeName << "' created as non-local in local cluster";
            }
        } else {
            if (federationAccount.empty()) {
                Valid = false;
                Reason = "Should specify federation account for modern-style topics";
                return;
            }
            Account_ = federationAccount;
            normDir.SkipPrefix(normDb);
            normDir.SkipPrefix("/");
            TString fullPath = NKikimr::JoinPath({TString(normDir), schemeName});
            auto parsed = TryParseModernMirroredPath(fullPath);
            if (!Valid) {
                return;
            }
            if (isLocal) {
                if (localDc.empty()) {
                    Valid = false;
                    Reason = "Local DC option is mandatory when creating local modern-style topic";
                    return;
                }
                Dc = localDc;
                if (!ParseModernPath(fullPath)) {
                    return;
                }
            } else {
                if (!parsed) {
                    Valid = false;
                    Reason = TStringBuilder() << "Topic in modern style with non-mirrored-name: " << schemeName
                                               << ", created as non-local";
                    return;
                }
            }
            if (FullModernName.empty()) {
                Valid = false;
                Reason = TStringBuilder()
                    << "Internal error: FullModernName empty in TopicConverter(for schema) for topic: "
                    << schemeName;
                return;
            }
            PrimaryPath = NKikimr::JoinPath({*Database, FullModernName});
            NormalizeAsFullPath(PrimaryPath);
        }
        if (Valid) {
            AFL_ENSURE(Account_.Defined())("scheme_name", schemeName)("database", database);
            AFL_ENSURE(!LegacyProducer.empty())("scheme_name", schemeName)("database", database);
            AFL_ENSURE(!LegacyLogtype.empty())("scheme_name", schemeName)("database", database);
            AFL_ENSURE(!Dc.empty())("scheme_name", schemeName)("database", database);
            AFL_ENSURE(!FullLegacyName.empty())("scheme_name", schemeName)("database", database);
            Account = *Account_;
            InternalName = FullLegacyName;
        }
    }

    TTopicNames ToTopicNames(bool fromConfig) const {
        TTopicNames names;
        names.Valid = Valid;
        names.Reason = Reason;
        if (!names.Valid) {
            return names;
        }
        names.Path = PrimaryPath;
        names.Account = Account;
        names.Cluster = Dc;
        names.LegacyProducer = LegacyProducer;
        names.LegacyLogtype = LegacyLogtype;
        names.ModernName = FullModernName;
        if (FstClass) {
            names.FederationPath = ClientsideName;
            names.FederationPathWithDC = ClientsideName;
        } else {
            names.FederationPath = LbPath.GetOrElse("");
            names.FederationPathWithDC = Account_.Defined()
                ? (*Account_ + "/" + FullModernName)
                : LbPath.GetOrElse("");
        }
        if (fromConfig) {
            AFL_ENSURE(!FullModernName.empty())("original_topic", OriginalTopic)("primary_path", PrimaryPath);
            names.InternalName = InternalName;
            AFL_ENSURE(!ClientsideName.empty())("original_topic", OriginalTopic)("primary_path", PrimaryPath);
            names.ClientsideName = ClientsideName;
            names.ShortClientsideName = ShortClientsideName;
            if (FstClass) {
                names.TopicForSrcIdHash = StripLeadSlash(FullModernPath);
            } else {
                names.TopicForSrcIdHash = ShortLegacyName;
            }
        } else {
            names.InternalName = names.Path;
        }
        return names;
    }
};

#undef CHECK_SET_VALID

} // namespace

TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, bool firstClassCitizen) {
    TNameBuilder builder;
    builder.InitFromTabletConfig(firstClassCitizen, {}, config, "");
    return builder.ToTopicNames(true);
}

TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config) {
    const auto& pqConfig = AppData()->PQConfig;
    const bool firstClassCitizen = pqConfig.GetTopicsAreFirstClassCitizen() || !pqConfig.GetEnabled();
    TNameBuilder builder;
    builder.InitFromTabletConfig(
        firstClassCitizen, NormalizePqPrefix(pqConfig.GetRoot()), config, pqConfig.GetTestDatabaseRoot());
    auto names = builder.ToTopicNames(true);
    // Request-side FCC converters used to keep names valid when AppData FCC is off
    // for a first-class tablet config (kafka BalanceScenarioForFederation).
    if (!names.IsValid() && !firstClassCitizen) {
        return NamesFromConfig(config, true);
    }
    return names;
}

TTopicNames WithClientsideNameOverride(TTopicNames names, const TString& clientsideName) {
    if (names.Valid && !clientsideName.empty()) {
        names.ClientsideName = clientsideName;
    }
    return names;
}

TTopicNames NamesForCreate(
    const TString& pqRoot,
    const TString& ydbTestDatabaseRoot,
    const TString& schemeName,
    const TString& schemeDir,
    const TString& database,
    bool isLocal,
    const TString& localDc,
    const TString& federationAccount
) {
    TNameBuilder builder;
    builder.InitForCreate(
        pqRoot, ydbTestDatabaseRoot, schemeName, schemeDir, database, isLocal, localDc, federationAccount);
    return builder.ToTopicNames(false);
}

TExpandReadTopicsResult ExpandReadTopics(
    TStringBuf database,
    const THashSet<TString>& clientTopics,
    bool onlyLocal,
    TStringBuf localDc,
    const TVector<TString>& clusters
) {
    TExpandReadTopicsResult result;
    const bool firstClass = AppData()->PQConfig.GetTopicsAreFirstClassCitizen();

    auto putTopic = [&](const TString& topic, TStringBuf dc) {
        auto resolved = ResolveName(database, topic, localDc, dc);
        if (!resolved) {
            result.IsValid = false;
            result.Reason = TStringBuilder() << "Invalid topic format in init request: '" << topic
                                             << "': " << resolved.error();
            return;
        }
        result.ClientTopics[topic].push_back(resolved->Path);
        result.Paths.insert(resolved->Path);
    };

    for (const auto& topic : clientTopics) {
        if (onlyLocal || firstClass) {
            putTopic(topic, firstClass ? TStringBuf{} : localDc);
        } else {
            for (const auto& cluster : clusters) {
                putTopic(topic, cluster);
                if (!result.IsValid) {
                    break;
                }
            }
        }
        if (!result.IsValid) {
            break;
        }
    }
    return result;
}

} // namespace NKikimr::NPQ::NNameResolver
