#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/persqueue/topic_parser/type_definitions.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <expected>
#include <memory>

namespace NKikimr::NPQ::NNameResolver {

struct TResolvedName {
    // Absolute topic path to navigate in SchemeCache.
    // Federation: "/Root/LbCommunal/account/topic"
    //   (mirrored: "/Root/LbCommunal/account/topic-mirrored-from-dc2").
    // Classic empty-LbRoot: "/Root/PQ/rt3.dc1--account--topic".
    // FCC: "/Root/account/topic".
    TString Path;
    // SchemeCache DatabaseName hint. Empty if the request database was empty.
    // Federation under LbRoot/account/...: "/Root/LbCommunal/account".
    // FCC / otherwise: request database, e.g. "/Root".
    TString NavigateDatabase;
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
 *     also under LbRoot. With empty LbRoot and a known DC, bare and modern names map
 *     to PQ root as rt3.<dc>--... (classic TTestServer / TDiscoveryConverter).
 *     With LbRoot, bare names stay under the request database.
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

/**
 * All names derived from a tablet config (or create-time scheme args).
 * Value type; pass TTopicNamesPtr where a shared handle is needed.
 *
 * Federation example (local DC, TopicPath "/Root/PQ/rt3.dc1--account--topic"):
 *   Path="/Root/PQ/rt3.dc1--account--topic"
 *   ClientsideName="rt3.dc1--account--topic", ShortClientsideName="account--topic"
 *   ModernName="topic"
 *   FederationPath="account/topic", FederationPathWithDC="account/topic"
 *   Account="account", Cluster="dc1", LegacyProducer="account", LegacyLogtype="topic"
 *   InternalName="rt3.dc1--account--topic"
 *   TopicForSrcIdHash="account--topic"
 *
 * Federation mirrored (TopicPath "/lb/account-database/path/topic-mirrored-from-dc2"):
 *   ModernName="path/topic-mirrored-from-dc2"
 *   FederationPath="account/path/topic"
 *   FederationPathWithDC="account/path/topic-mirrored-from-dc2"
 *   ClientsideName="rt3.dc2--account@path--topic"
 *
 * FCC example (TopicPath "/lb/database/my-stream"):
 *   Path="/lb/database/my-stream", ClientsideName="my-stream"
 *   FederationPath=FederationPathWithDC="my-stream"
 *   TopicForSrcIdHash="lb/database/my-stream", InternalName="/lb/database/my-stream"
 */
struct TTopicNames {
    // False if parsing failed; then only Reason is meaningful.
    bool Valid = false;
    // Human-readable parse error. Empty when Valid. Example:
    // "Invalid topic path or trailing '/'".
    TString Reason;

    // Absolute scheme path (GetPrimaryPath).
    // Federation: "/Root/PQ/rt3.dc1--account--topic"
    //   or "/lb/account-database/path/topic".
    // FCC: "/lb/database/my-stream".
    TString Path;
    // Name clients send in PQ API. Federation: "rt3.dc1--account--topic".
    // FCC: "my-stream".
    TString ClientsideName;
    // ClientsideName without the rt3.<dc>-- prefix. Federation: "account--topic"
    // (or "account@path--topic"). FCC: same as ClientsideName ("my-stream").
    TString ShortClientsideName;
    // Path inside the account database, including -mirrored-from-<dc> when remote.
    // Local: "topic" or "path/topic". Remote: "path/topic-mirrored-from-dc2".
    // FCC: "my-stream".
    TString ModernName;
    // account[/dir]/topic without DC suffix. Federation: "account/topic" or
    // "account/path/topic". FCC: same as ClientsideName ("my-stream").
    TString FederationPath;
    // FederationPath with -mirrored-from-<dc> when the topic is remote.
    // Local: "account/path/topic". Remote: "account/path/topic-mirrored-from-dc2".
    // FCC: "my-stream".
    TString FederationPathWithDC;
    // Federation account (Lb account / producer). Example: "account". Empty in FCC.
    TString Account;
    // DC from the name or tablet config. Example: "dc1". Empty in FCC.
    TString Cluster;
    // Legacy producer (account or account@dir). Example: "account" or "account@path".
    // Empty in FCC.
    TString LegacyProducer;
    // Legacy logtype (topic leaf). Example: "topic". Empty in FCC.
    TString LegacyLogtype;
    // Name used internally / as balancer topic. Federation: full legacy
    // "rt3.dc1--account--topic". FCC: absolute path "/lb/database/my-stream".
    TString InternalName;
    // Hash key for SourceId. Federation: short legacy leaf ("account--topic").
    // FCC: Path without leading slash ("lb/database/my-stream").
    TString TopicForSrcIdHash;

    bool IsValid() const { return Valid; }
    const TString& GetReason() const { return Reason; }

    const TString& GetClientsideName() const { return ClientsideName; }
    TString GetPrimaryPath() const { return Path; }
    TString GetFederationPath() const { return FederationPath; }
    TString GetFederationPathWithDC() const { return FederationPathWithDC; }
    const TString& GetAccount() const { return Account; }
    const TString& GetCluster() const { return Cluster; }
    const TString& GetLegacyProducer() const { return LegacyProducer; }
    const TString& GetLegacyLogtype() const { return LegacyLogtype; }
    const TString& GetModernName() const { return ModernName; }
    TString GetInternalName() const { return InternalName; }
    TString GetTopicForSrcIdHash() const { return TopicForSrcIdHash; }

    // Logs: absolute modern path. Account is already in Path.
    TString GetPrintableString() const { return Path; }

    NPersQueue::TTopicCounterNames CounterNames() const {
        return NPersQueue::TTopicCounterNames{
            .Account = Account,
            .LegacyProducer = LegacyProducer,
            .ShortClientsideName = ShortClientsideName,
            .FederationPath = FederationPath,
            .ClientsideName = ClientsideName,
            .Cluster = Cluster,
        };
    }
};

using TTopicNamesPtr = std::shared_ptr<const TTopicNames>;

inline TTopicNamesPtr MakeTopicNamesPtr(TTopicNames names) {
    return std::make_shared<const TTopicNames>(std::move(names));
}

/**
 * Tablet's only name entry. Reads firstClassCitizen, PQ Root and TestDatabaseRoot from AppData()->PQConfig.
 * Prefer this overload wherever an actor context is available and the config already has TopicPath.
 */
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config);

/**
 * Same as NamesFromConfig(config), but uses topicPath instead of config.GetTopicPath().
 * Does not modify config. Pass the scheme path when the stored tablet config has no TopicPath.
 */
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, const TString& topicPath);

/**
 * Same formation as NamesFromConfig, but firstClassCitizen is passed explicitly and AppData is not read.
 * Use when there is no actor TLS (unit tests) or when formation must not follow AppData.
 */
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, bool firstClassCitizen);

/**
 * NamesFromConfig(config, firstClassCitizen) with an explicit topicPath (does not modify config).
 */
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, const TString& topicPath, bool firstClassCitizen);

// const char* would convert to bool and skip topicPath. Pass TString.
TTopicNames NamesFromConfig(const NKikimrPQ::TPQTabletConfig& config, const char*) = delete;

/** CDC: override ClientsideName with the stream path (not streamImpl). */
TTopicNames WithClientsideNameOverride(TTopicNames names, const TString& clientsideName);

/**
 * Analog of TTopicNameConverter::ForFederation for schema create.
 * Reads PQ Root and TestDatabaseRoot from AppData()->PQConfig.
 */
TTopicNames NamesForCreate(
    const TString& schemeName,
    const TString& schemeDir,
    const TString& database,
    bool isLocal,
    const TString& localDc = {},
    const TString& federationAccount = {}
);

struct TExpandReadTopicsResult {
    // False if any client name failed ResolveName.
    bool IsValid = true;
    // Set when !IsValid. Example:
    // "Invalid topic format in init request: 'account/': Invalid topic path or trailing '/'".
    TString Reason;
    // Original client name → one resolved path per DC (FCC / onlyLocal: one entry).
    // Example key "account/topic" →
    //   {"/Root/LbCommunal/account/topic",
    //    "/Root/LbCommunal/account/topic-mirrored-from-dc2"}.
    THashMap<TString, TVector<TString>> ClientTopics;
    // Unique resolved paths from ClientTopics.
    // Example: {"/Root/LbCommunal/account/topic",
    //           "/Root/LbCommunal/account/topic-mirrored-from-dc2"}.
    THashSet<TString> Paths;
};

/**
 * Analog of GetReadTopicsList: ResolveName × DC (FCC: one name; onlyLocal: local DC;
 * otherwise one path per cluster). Does not touch SchemeCache.
 */
TExpandReadTopicsResult ExpandReadTopics(
    TStringBuf database,
    const THashSet<TString>& clientTopics,
    bool onlyLocal,
    TStringBuf localDc,
    const TVector<TString>& clusters
);

struct TReadTopicsContext {
    // Local DC from cluster tracker. Example: "dc1".
    TString LocalCluster;
    // Enabled cluster names for DC expansion. Example: {"dc1", "dc2", "dc3"}.
    TVector<TString> Clusters;

    TExpandReadTopicsResult ExpandRead(
        TStringBuf database,
        const THashSet<TString>& topics,
        bool onlyLocal
    ) const {
        return ExpandReadTopics(database, topics, onlyLocal, LocalCluster, Clusters);
    }
};

} // namespace NKikimr::NPQ::NNameResolver
