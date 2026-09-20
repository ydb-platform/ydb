#pragma once

#include "nameresolver_fwd.h"

#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <expected>
#include <optional>

namespace NKikimr::NPQ::NNameResolver {

struct TResolvedName {
    TString Path;              // absolute topic path
    TString NavigateDatabase;  // SchemeCache DatabaseName hint (may be empty)
};

/**
 * Converts a topic name to its full path and SchemeCache database hint.
 * Also converts a full/legacy topic name to the full path used for reading from mirrored topics.
 *
 * Returns resolved path + navigate database on success, or error reason on failure.
 *
 * NavigateDatabase:
 *   - FCC (non-federation): request database (absolute)
 *   - Federation + path under LbRoot/account/...: LbRoot/account
 *   - Otherwise: request database (absolute), or empty if request database is empty
 *
 * First-class citizen (FCC / non-federation) mode:
 *   Names are joined with the request database as-is. Legacy forms (rt3.*, --, @)
 *   are not converted: a leaf like TestSchemeList--test-topic-1 is a literal name.
 *
 * Federation mode (!TopicsAreFirstClassCitizen):
 *   - Root-like database (empty, prefixes PQ Root, or prefixes LbUserDatabaseRoot):
 *     path with '/' is federation account/topic under LbRoot; explicit legacy (rt3/--/@)
 *     also under LbRoot; bare names stay under the request database.
 *   - User database (under LbRoot, e.g. …/account): relative modern path inside that database
 *     (already mirrored …-mirrored-from-<dc> names are kept as-is).
 *
 * Root for resolved modern paths is LbUserDatabaseRoot from PQConfig, or database if Lb root is empty
 * (for user-database federation paths the request database is preferred).
 *
 * Full paths under database are accepted: the database prefix is stripped before resolving
 * (e.g. database "/Root" + name "/Root/account/topic" → same as name "account/topic").
 *
 * If dc is empty (default), it is taken from an rt3.<dc>--... name when present;
 * for short / modern paths without rt3., empty dc falls back to localDc.
 * Empty dc and localDc (both default) means local (no -mirrored-from- suffix)
 * for short and modern names.
 *
 * Examples (LbRoot = "/Root/LbCommunal", PQ Root = "/Root/PQ"):
 *
 *   // FCC: literal names under the request database (-- is not a path separator)
 *   ResolveName(db, "TestSchemeList--test-topic-1")
 *     -> Path=db+"/TestSchemeList--test-topic-1", NavigateDatabase=db
 *   ResolveName(db, "account/topic")
 *     -> Path=db+"/account/topic", NavigateDatabase=db
 *
 *   // Federation: without localDc/dc (defaults) — local path, no -mirrored-from- suffix
 *   ResolveName("/Root", "account/topic")
 *     -> Path="/Root/LbCommunal/account/topic", NavigateDatabase="/Root/LbCommunal/account"
 *   ResolveName("/Root/LbCommunal/account", "dir/topic")
 *     -> Path="/Root/LbCommunal/account/dir/topic", NavigateDatabase="/Root/LbCommunal/account"
 *
 *   // Federation: with localDc (mirroring / DC-aware resolve)
 *   ResolveName("/Root", "rt3.dc1--account--topic", "dc1")
 *     -> Path="/Root/LbCommunal/account/topic", NavigateDatabase="/Root/LbCommunal/account"
 *   ResolveName("/Root", "account/topic", "dc1", "dc2")
 *     -> Path="/Root/LbCommunal/account/topic-mirrored-from-dc2", NavigateDatabase="/Root/LbCommunal/account"
 */
std::expected<TResolvedName, TString> ResolveName(
    TStringBuf database,
    TStringBuf name,
    TStringBuf localDc = {},
    TStringBuf dc = {}
);

struct TFederationAccountTarget {
    TString Path;            // absolute under federationRoot
    TString AccountDatabase; // federationRoot/account
};

/**
 * If path is under federationRoot and has account/... shape, returns SchemeCache target
 * (same path + account database). Otherwise nullopt.
 */
std::optional<TFederationAccountTarget> TryFederationAccountTarget(
    TStringBuf path,
    TStringBuf federationRoot
);

/**
 * All names derived from a tablet config.
 * Value type; pass TTopicNamesPtr where a shared handle is needed.
 *
 * Field comments use two federation topics and one FCC topic:
 *   Fed:     "/Root/PQ/rt3.dc1--account--topic"
 *   Fed dir: "/lb/account-database/path/topic"  (legacy leaf rt3.dc1--account@path--topic)
 *   FCC:     "/lb/database/my-stream"
 */
struct TTopicNames {
    using TPtr = TTopicNamesPtr;

    // False if parsing failed; then only Reason is meaningful.
    bool Valid = false;
    TString Reason;

    // Parse mode used to fill the fields below. True after FCC parse, including a
    // first-class tablet on a federation node (kafka BalanceScenarioForFederation).
    bool FirstClassCitizen = false;

    // Absolute scheme path of the topic object.
    // Fed: "/Root/PQ/rt3.dc1--account--topic"
    // Fed dir: "/lb/account-database/path/topic"
    // FCC: "/lb/database/my-stream"
    TString Path;

    // Name the client uses (legacy rt3 form in federation).
    // Fed: "rt3.dc1--account--topic"
    // Fed dir: "rt3.dc1--account@path--topic"
    // FCC: "my-stream"
    TString ClientsideName;

    // ClientsideName without "rt3.<dc>--". Solomon Topic label in federation.
    // Fed: "account--topic"
    // Fed dir: "account@path--topic"
    // FCC: "my-stream"
    TString ShortClientsideName;

    // Modern path relative to the account; includes "-mirrored-from-<dc>" when remote.
    // Fed local: "topic"; Fed dir local: "path/topic"; remote: "path/topic-mirrored-from-dc2"
    // FCC: "my-stream"
    TString ModernName;

    // Logbroker path account[/dir]/topic without DC suffix.
    // Fed: "account/topic"
    // Fed dir: "account/path/topic"
    // FCC: "my-stream" (same as ClientsideName)
    TString FederationPath;

    // FederationPath with "-mirrored-from-<dc>" when the topic is remote.
    // Fed local: "account/topic"
    // Fed dir remote: "account/path/topic-mirrored-from-dc2"
    // FCC: "my-stream"
    TString FederationPathWithDC;

    // Federation account from tablet config.
    // Fed / Fed dir: "account"
    // FCC: empty
    TString Account;

    // Origin DC copied from tablet config (PQTabletConfig.DC). Empty if DC is unset.
    // Fed: "dc1"
    // FCC: empty unless the tablet config has DC
    TString Cluster;

    // Logbroker producer: account or account@dir. Solomon Producer label.
    // Fed: "account"
    // Fed dir: "account@path"
    // FCC: empty
    TString LegacyProducer;

    // Last component of the modern path (logtype).
    // Fed / Fed dir: "topic"
    // FCC: empty
    TString LegacyLogtype;

    // Identity key: FullLegacyName in federation, scheme Path in FCC.
    // Fed: "rt3.dc1--account--topic"
    // Fed dir: "rt3.dc1--account@path--topic"
    // FCC: "/lb/database/my-stream"
    TString InternalName;

    // Key mixed into source-id hash (SrcIdMeta2). ShortLegacyName in federation.
    // Fed: "account--topic"
    // Fed dir: "account@path--topic"
    // FCC: "lb/database/my-stream" (scheme path without leading slash)
    TString TopicForSrcIdHash;

    // Alternate scheme path. Empty in FCC.
    // User-database federation: legacy PQ-root leaf
    //   Fed dir: "/Root/PQ/rt3.dc1--account@path--topic"
    // PQ-root federation: JoinPath({YdbDatabasePath, ModernName}) when YdbDatabasePath is set,
    // otherwise empty.
    TString SecondaryPath;
};

inline TTopicNames::TPtr MakeTopicNamesPtr(TTopicNames names) {
    return std::make_shared<const TTopicNames>(std::move(names));
}

/**
 * Tablet's only name entry. Reads firstClassCitizen, PQ Root and TestDatabaseRoot from AppData()->PQConfig.
 * SchemeCache fills TPQGroupInfo::Names with NamesFromConfig(config, schemePath).
 * Never aborts or throws on malformed config: Valid=false and Reason is set.
 */
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config);

/**
 * Same as NamesFromConfig(config), but uses topicPath instead of config.GetTopicPath().
 * Pass the scheme path when the stored tablet config has no TopicPath.
 * SchemeCache calls this overload (via AppData PQConfig).
 */
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, const TString& topicPath);

/**
 * Same as NamesFromConfig(config, topicPath), but reads FCC / PQ Root / TestDatabaseRoot
 * from pqConfig instead of AppData. Use in tests that have PQConfig but no actor TLS.
 */
TTopicNames NamesFromConfig(
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& topicPath,
    const NKikimrPQ::TPQConfig& pqConfig);

/**
 * Same formation as NamesFromConfig, but firstClassCitizen is passed explicitly and AppData is not read.
 */
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, bool firstClassCitizen);

TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, const TString& topicPath, bool firstClassCitizen);

/**
 * Same as NamesFromConfig(config, topicPath, firstClassCitizen), but PQ prefix and
 * TestDatabaseRoot override are passed explicitly (AppData is not read).
 * pqNormalizedPrefix is slash-stripped PQ Root, matching TTopicNamesConverterFactory.
 */
TTopicNames NamesFromConfig(
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& topicPath,
    bool firstClassCitizen,
    const TString& pqNormalizedPrefix,
    const TString& ydbDatabaseRootOverride);

// const char* would convert to bool and skip topicPath. Pass TString.
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, const char*) = delete;

} // namespace NKikimr::NPQ::NNameResolver
