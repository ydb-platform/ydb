#include "topic_parser.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/folder/path.h>

namespace NPersQueue {

bool IsPathPrefix(TStringBuf normPath, TStringBuf prefix) {
    auto res = normPath.SkipPrefix(prefix);
    if (!res) {
        return false;
    } else if (normPath.empty()) {
        return true;
    } else {
        return normPath.SkipPrefix("/");
    }
}


void SkipPathPrefix(TStringBuf& path, const TStringBuf& prefix) {
    auto copy = path;
    if (prefix.EndsWith('/')) {
        path.SkipPrefix(prefix);
    } else {
        auto skip = path.SkipPrefix(prefix) && path.SkipPrefix("/");
        if (!skip) {
            path = copy;
        }
    }
}

namespace {
    TString FullPath(const TMaybe<TString>& database, const TString& path) {
        if (database.Defined() && !path.StartsWith(*database) && !path.Contains('\0')) {
            try {
                return (TFsPath(*database) / path).GetPath();
            } catch(...) {
                return path;
            }
        } else {
            return path;
        }
    }
}

void NormalizeAsFullPath(TString& path) {
    if (!path.Empty() && !path.StartsWith("/")) {
        path = TString("/") + path;
    }
}

TString StripLeadSlash(const TString& path) {
    if (!path.StartsWith("/")) {
        return path;
    } else {
        return path.substr(1);
    }
}

TString NormalizeFullPath(const TString& fullPath) {
    if (!fullPath.Empty() && !fullPath.StartsWith("/")) {
        return TString("/") + fullPath;
    } else {
        return fullPath;
    }
}

TString GetFullTopicPath(const NActors::TActorContext& ctx, const TMaybe<TString>& database, const TString& topicPath) {
    if (NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return FullPath(database, topicPath);
    } else {
        return topicPath;
    }
}

TString ConvertNewConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig) {
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertNewConsumerName(consumer);
    }
}

TString ConvertNewConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    return ConvertNewConsumerName(consumer, NKikimr::AppData(ctx)->PQConfig);
}

TString ConvertOldConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig) {
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertOldConsumerName(consumer);
    }
}

TString ConvertOldConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    return ConvertOldConsumerName(consumer, NKikimr::AppData(ctx)->PQConfig);
}


TString MakeConsumerPath(const TString& consumer) {
    TStringBuilder res;
    res.reserve(consumer.size());
    for (ui32 i = 0; i < consumer.size(); ++i) {
        if (consumer[i] == '@') {
            res << "/";
        } else {
            res << consumer[i];
        }
    }
    if (res.find("/") == TString::npos) {
        return TStringBuilder() << "shared/" << res;
    }
    return res;
}

TDiscoveryConverterPtr TDiscoveryConverter::ForFstClass(const TString& topic, const TString& database) {
    auto* res = new TDiscoveryConverter();
    res->FstClass = true;
    res->Database = database;
    res->OriginalTopic = topic;
    res->BuildFstClassNames();
    return TDiscoveryConverterPtr(res);
}

bool BasicNameChecks(const TStringBuf& name) {
    return (!name.empty() && !name.Contains("//"));
}

TDiscoveryConverterPtr TDiscoveryConverter::ForFederation(
        const TString& topic, const TString& dc, const TString& localDc, const TString& database,
        const TString& pqNormalizedPrefix
) {

    auto* res = new TDiscoveryConverter();
    res->PQPrefix = pqNormalizedPrefix;
    res->FstClass = false;
    res->Dc = dc;
    res->LocalDc = localDc;
    TStringBuf topicBuf{topic};
    TStringBuf dbBuf{database};
    if (!BasicNameChecks(topicBuf)) {
        res->Valid = false;
        res->Reason = TStringBuilder() << "Bad topic name for federation: " << topic;
        return NPersQueue::TDiscoveryConverterPtr(res);
    }

    topicBuf.SkipPrefix("/");
    dbBuf.SkipPrefix("/");
    dbBuf.ChopSuffix("/");

    res->BuildForFederation(dbBuf, topicBuf);
    return NPersQueue::TDiscoveryConverterPtr(res);
}

TDiscoveryConverter::TDiscoveryConverter(bool firstClass,
                                         const TString& pqNormalizedPrefix,
                                         const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
                                         const TString& ydbDatabaseRootOverride
)
    : PQPrefix(pqNormalizedPrefix)
{
    auto name = pqTabletConfig.GetTopicName();
    auto path = pqTabletConfig.GetTopicPath();
    if (name.empty()) {
        Y_ABORT_UNLESS(!path.empty());
        TStringBuf pathBuf(path), fst, snd;
        auto res = pathBuf.TryRSplit("/", fst, snd);
        Y_ABORT_UNLESS(res);
        name = snd;
    } else if (path.empty()) {
        path = name;
    }
    if (!ydbDatabaseRootOverride.empty()) {
        TStringBuf pathBuf(path);
        TStringBuf dbRoot(ydbDatabaseRootOverride);
        auto res_ = pathBuf.SkipPrefix(dbRoot);
        if (res_) {
            dbRoot.SkipPrefix("/");
            res_ = pathBuf.SkipPrefix("/");
            TStringBuf acc, rest;
            res_ = pathBuf.TrySplit("/", acc, rest);
            if (res_) {
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
    auto& acc = pqTabletConfig.GetFederationAccount();
    if (!acc.empty()) {
        Account_ = acc;
    }
    if (FstClass) {
        // No legacy names required;
        OriginalTopic = pqTabletConfig.GetTopicPath();
        BuildFstClassNames();
        return;
    } else {
        BuildForFederation(*Database, path);
    }
}

void TDiscoveryConverter::BuildForFederation(const TStringBuf& databaseBuf, TStringBuf topicPath
                                             //, const TVector<TString>& rootDatabases
) {
    topicPath.SkipPrefix("/");
    CHECK_SET_VALID(!topicPath.empty(), "Invalid topic path (only account provided?)", return);
    CHECK_SET_VALID(!topicPath.EndsWith("/"), "Invalid topic path 0 triling '/'", return);
    if (FstClass) {
        // No legacy names required;
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
            SkipPathPrefix(topicPath, PQPrefix);
        }
    } else if (IsPathPrefix(topicPath, PQPrefix)) {
        isRootDb = true;
        SkipPathPrefix(topicPath, PQPrefix);
        root = PQPrefix;
    }
    if (!isRootDb) {
        SkipPathPrefix(topicPath, databaseBuf);
        Database = databaseBuf;
    }
    CHECK_SET_VALID(!topicPath.empty(), "Bad topic name (only account provided?)", return);

    OriginalTopic = topicPath;
    if (!isRootDb && Database.Defined()) {
        // Topic with valid non-root database. Parse as 'modern' name. Primary path is path in database.
        auto parsed = TryParseModernMirroredPath(topicPath);
        if (!Valid) {
            return;
        }
        if (!parsed) {
            if(!ParseModernPath(topicPath))
                return;
        }
        CHECK_SET_VALID(
                !FullModernName.empty(),
                TStringBuilder() << "Internal error: Could not parse topic name (federation path was assumed)"  << OriginalTopic,
                return
        );

        PrimaryPath = NKikimr::JoinPath({*Database, FullModernName});
        NormalizeAsFullPath(PrimaryPath);
        if (!FullLegacyName.empty()) {
            SecondaryPath = NKikimr::JoinPath({PQPrefix, FullLegacyName});
            NormalizeAsFullPath(SecondaryPath.GetRef());
        }
        if (!BuildFromShortModernName())
            return;
    } else {
        if (root.empty()) {
            root = PQPrefix;
        }
        // Topic from PQ root - this is either federation path (account/topic)
        if (topicPath.find("/") != TString::npos) {
            auto ok = BuildFromFederationPath(root);
            Y_UNUSED(ok);
        } else {
            // OR legacy name (rt3.sas--account--topic)
            auto ok = BuildFromLegacyName(root); // Sets primary path;
            Y_UNUSED(ok);
        }
    }
}

TTopicConverterPtr TDiscoveryConverter::UpgradeToFullConverter(
        const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
        const TString& ydbDatabaseRootOverride,
        const TMaybe<TString>& clientsideNameOverride
) {
    Y_VERIFY_S(Valid, Reason.c_str());
    auto* res = new TTopicNameConverter(FstClass, PQPrefix, pqTabletConfig,
        ydbDatabaseRootOverride, clientsideNameOverride);
    return TTopicConverterPtr(res);
}

void TDiscoveryConverter::BuildFstClassNames() {
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
        return;
    );
};

bool TDiscoveryConverter::BuildFromFederationPath(const TString& rootPrefix) {
    // This is federation (not first class) and we have topic specified as a path;
    // So only convention supported is 'account/dir/topic' and account in path matches LB account;
    TStringBuf topic(OriginalTopic);
    LbPath = OriginalTopic;
    TStringBuf fst, snd;
    auto res = topic.TrySplit("/", fst, snd);
    CHECK_SET_VALID(res, TStringBuilder() << "Could not split federation path: " << OriginalTopic, return false);
    Account_ = fst;

    if (!ParseModernPath(snd))
        return false;
    if (!BuildFromShortModernName()) {
        return false;
    }
    CHECK_SET_VALID(
        !FullLegacyName.empty(),
        TStringBuilder() << "Internal error: couldn't build legacy-style name for topic " << OriginalTopic,
        return false
    );

    PrimaryPath = NKikimr::JoinPath({rootPrefix, FullLegacyName});
    NormalizeAsFullPath(PrimaryPath);

    PendingDatabase = true;
    return true;
}

bool TDiscoveryConverter::TryParseModernMirroredPath(TStringBuf path) {
    if (!path.Contains("-mirrored-from-")) {
        CHECK_SET_VALID(!path.Contains("mirrored-from"), "Federation topics cannot contain 'mirrored-from' in name unless this is a mirrored topic", return false);
        return false;
    }
    TStringBuf fst, snd;
    auto res = path.TryRSplit("-mirrored-from-", fst, snd);
    CHECK_SET_VALID(res, "Malformed mirrored topic path - expected to end with '-mirrored-from-<cluster>'", return false);
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

bool TDiscoveryConverter::ParseModernPath(const TStringBuf& path) {
    // This is federation (not first class) and we have topic specified as a path;
    // So only convention supported is 'account/dir/topic' and account in path matches LB account;
    TStringBuilder pathAfterAccount;

    // Path after account would contain 'dir/topic' OR dir/topic-mirrored-from-...
    if (!Dc.empty() && !LocalDc.empty() && Dc != LocalDc) {
        TStringBuf directories, topicName;
        auto res = path.TrySplit("/", directories, topicName);
        if (res) {
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

bool TDiscoveryConverter::BuildFromShortModernName() {
    CHECK_SET_VALID(
        !ModernName.empty(), TStringBuilder() << "Could not parse topic name: " << OriginalTopic, return false
    );

    TStringBuf pathBuf(ModernName);
    TStringBuilder legacyName;
    TString legacyProducer;
    TString lbPath;
    legacyName << Account_.GetOrElse("undef-account"); // Add account;
    if (Account_.Defined()) {
        lbPath = NKikimr::JoinPath({*Account_, ModernName});
    }

    TStringBuf fst, snd, logtype;
    // logtype = topic for path above
    auto res = pathBuf.TryRSplit("/", fst, logtype);
    if (!res) {
        logtype = pathBuf;
    } else {
        pathBuf = fst;
        // topic now contains only directories
        while(true) {
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
    legacyProducer = legacyName;
    legacyName << "--" << logtype;
    ShortLegacyName = legacyName;
    if (Dc.empty()) {
        Dc = LocalDc;
        CHECK_SET_VALID(!LocalDc.empty(), "Cannot determine DC: should specify either with Dc option or LocalDc option",
                        return false);
    }
    LbPath = lbPath;
    FullLegacyName = TStringBuilder() << "rt3." << Dc << "--" << ShortLegacyName;
    LegacyProducer = legacyProducer;
    LegacyLogtype = logtype;
    return true;
}

bool TDiscoveryConverter::BuildFromLegacyName(const TString& rootPrefix, bool forceFullName) {
    TStringBuf topic (OriginalTopic);
    bool hasDcInName = topic.Contains("rt3.");
    TStringBuf fst, snd;
    Account_ = Nothing(); //Account must be parsed out of legacy topic name
    TString shortLegacyName, fullLegacyName;
    if (forceFullName) {
        CHECK_SET_VALID(hasDcInName,
                        TStringBuilder() << "Invalid topic name - " << OriginalTopic
                                         << " - expected legacy-style name like rt3.<dc>--<account>--<topic>",
                        return false);
    }
    if (Dc.empty() && !hasDcInName) {
        CHECK_SET_VALID(!FstClass, TStringBuilder() << "Internal error: FirstClass mode enabled, but trying to parse Legacy-style name: "
                                                    << OriginalTopic, return false;);
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
        CHECK_SET_VALID(Dc.empty() || Dc == fst, "DC specified both in topic name and separate option and they mismatch", return false);
        Dc = fst;
        topic = snd;
    } else {
        CHECK_SET_VALID(!Dc.empty(), TStringBuilder() << "Internal error: Could not determine DC (despite beleiving the name contins one) for topic "
                                                    << OriginalTopic, return false;);
        TStringBuilder builder;
        builder << "rt3." << Dc << "--" << topic;
        fullLegacyName = builder;
    }
    // Now topic is supposed to contain short legacy style name ('topic' OR 'account--topic' OR 'account@dir--topic');
    shortLegacyName = topic;
    TStringBuilder modernName, fullModernName;
    TStringBuilder producer;
    auto res = topic.TryRSplit("--", fst, snd);
    if (res) {
        LegacyProducer = fst;
        LegacyLogtype = snd;
    } else {
        LegacyProducer = "unknown";
        LegacyLogtype = topic;
    }
    while(true) {
        auto res = topic.TrySplit("@", fst, snd);
        if (!res)
            break;
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
    CHECK_SET_VALID(!Dc.empty(), TStringBuilder() << "Internal error: Could not determine DC for topic: "
                                                    << OriginalTopic, return false);

    bool isMirrored = (!LocalDc.empty() && Dc != LocalDc);
    if (isMirrored) {
        fullModernName << topicName << "-mirrored-from-" << Dc;
    } else {
        fullModernName << topicName;
    }
    CHECK_SET_VALID(!fullLegacyName.empty(), TStringBuilder() << "Could not form a full legacy name for topic: "
                                                              << OriginalTopic, return false);

    ShortLegacyName = shortLegacyName;
    FullLegacyName = fullLegacyName;
    PrimaryPath = NKikimr::JoinPath({rootPrefix, fullLegacyName});
    NormalizeAsFullPath(PrimaryPath);
    FullModernName = fullModernName;
    ModernName = modernName;
    LbPath = NKikimr::JoinPath({*Account_, modernName});


    if (!Database.Defined()) {
        PendingDatabase = true;
    } else {
        SetDatabase("");
    }
    return true;
}

bool TDiscoveryConverter::IsValid() const {
    return Valid;
}

const TString& TDiscoveryConverter::GetReason() const {
    return Reason;
}

TString TDiscoveryConverter::GetPrintableString() const {
    TStringBuilder res;
    res << "Topic " << OriginalTopic;
    if (!Dc.empty()) {
        res << " in dc " << Dc;
    }
    if (!Database.GetOrElse(TString()).empty()) {
        res << " in database: " << Database.GetOrElse(TString());
    }
    return res;
}

TString TDiscoveryConverter::GetPrimaryPath() const {
    CHECK_VALID_AND_RETURN(PrimaryPath);
}

TString TDiscoveryConverter::GetOriginalPath() const {
    if (!OriginalPath.empty()) {
        return OriginalPath;
    } else {
        return GetPrimaryPath();
    }
}

const TMaybe<TString>& TDiscoveryConverter::GetSecondaryPath(const TString& database) {
    if (!database.empty()) {
        SetDatabase(database);
    }
    Y_ABORT_UNLESS(!PendingDatabase);
    Y_ABORT_UNLESS(SecondaryPath.Defined());
    return SecondaryPath;
}

const TMaybe<TString>& TDiscoveryConverter::GetAccount_() const {
    return Account_;
}

void TDiscoveryConverter::SetDatabase(const TString& database) {
    if (database.empty()) {
        return;
    }
    if (!Database.Defined()) {
        Database = NormalizeFullPath(database);
    }
    Y_ABORT_UNLESS(!FullModernName.empty());
    if (!SecondaryPath.Defined()) {
        SecondaryPath = NKikimr::JoinPath({*Database, FullModernName});
        NormalizeAsFullPath(SecondaryPath.GetRef());
    }
    FullModernPath = SecondaryPath.GetRef();
    PendingDatabase = false;
}

const TString& TDiscoveryConverter::GetOriginalTopic() const {
    return OriginalTopic;
}

TTopicConverterPtr TTopicNameConverter::ForFederation(
        const TString& pqRoot, const TString& ydbTestDatabaseRoot, const TString& schemeName, const TString& schemeDir,
        const TString& database, bool isLocal, const TString& localDc, const TString& federationAccount
) {
    auto res = TTopicConverterPtr(new TTopicNameConverter());

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

    res->Database = normDb;

    if (isRoot) {
        if (normDir != normRoot) {
            res->Valid = false;
            res->Reason = TStringBuilder() << "Topics with database '" << database << "' should be created in pqRoot: "
                                           << pqRoot;
            return res;
        }

        res->OriginalTopic = schemeName;
        auto buildOk = res->BuildFromLegacyName(TString(normRoot), true);
        if (!buildOk)
            return res;
        if (res->Valid && !isLocal && res->Dc == localDc) {
            res->Valid = false;
            res->Reason = TStringBuilder() << "Topic '" << schemeName << "' created as non-local in local cluster";
        }
    } else {
        if (federationAccount.empty()) {
            res->Valid = false;
            res->Reason = "Should specify federation account for modern-style topics";
            return res;
        }
        res->Account_ = federationAccount;
        normDir.SkipPrefix(normDb);
        normDir.SkipPrefix("/");
        TString fullPath = NKikimr::JoinPath({TString(normDir), schemeName});
        auto parsed = res->TryParseModernMirroredPath(fullPath);
        if (!res->IsValid()) {
            return res;
        }
        if (isLocal) {
            if (localDc.empty()) {
                res->Valid = false;
                res->Reason = "Local DC option is mandatory when creating local modern-style topic";
                return res;
            }
            res->Dc = localDc;
            auto ok = res->ParseModernPath(fullPath);
            if (!ok) {
                return res;
            }
        }
        else {
            if (!parsed) {
                res->Valid = false;
                res->Reason = TStringBuilder() << "Topic in modern style with non-mirrored-name: " << schemeName
                                               << ", created as non-local";

                return res;
            }
        }
        if (res->FullModernName.empty()) {
            res->Valid = false;
            res->Reason = TStringBuilder() << "Internal error: FullModernName empty in TopicConverter(for schema) for topic: "
                                           << schemeName;

            return res;
        }
        res->PrimaryPath = NKikimr::JoinPath({*res->Database, res->FullModernName});
        NormalizeAsFullPath(res->PrimaryPath);
    }
    if (res->IsValid()) {
        Y_ABORT_UNLESS(res->Account_.Defined());
        Y_ABORT_UNLESS(!res->LegacyProducer.empty());
        Y_ABORT_UNLESS(!res->LegacyLogtype.empty());
        Y_ABORT_UNLESS(!res->Dc.empty());
        Y_ABORT_UNLESS(!res->FullLegacyName.empty());
        res->Account = *res->Account_;
        res->InternalName = res->FullLegacyName;
    }
    return res;
}

TTopicNameConverter::TTopicNameConverter(
        bool firstClass, const TString& pqPrefix,
        const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
        const TString& ydbDatabaseRootOverride,
        const TMaybe<TString>& clientsideNameOverride
)
    : TDiscoveryConverter(firstClass, pqPrefix, pqTabletConfig, ydbDatabaseRootOverride)
{
    if (Valid) {
        BuildInternals(pqTabletConfig);
        if (clientsideNameOverride) {
            ClientsideName = *clientsideNameOverride;
        }
    }
}

TTopicConverterPtr TTopicNameConverter::ForFirstClass(const NKikimrPQ::TPQTabletConfig& pqTabletConfig) {
    auto* converter = new TTopicNameConverter{true, {}, pqTabletConfig, ""};
    return TTopicConverterPtr(converter);
}

TTopicConverterPtr TTopicNameConverter::ForFederation(const TString& pqPrefix,
                                                      const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
                                                      const TString& ydbDatabaseRootOverride) {
    auto* converter = new TTopicNameConverter{false, pqPrefix, pqTabletConfig, ydbDatabaseRootOverride};
    return TTopicConverterPtr(converter);
}

void TTopicNameConverter::BuildInternals(const NKikimrPQ::TPQTabletConfig& config) {
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
        Y_ABORT_UNLESS(!path.empty());
        path.SkipPrefix(db);
        path.SkipPrefix("/");
        ClientsideName = path;
        ShortClientsideName = path;
        FullModernName = path;
        InternalName = PrimaryPath;
    } else {
        SetDatabase(*Database);
        Y_ABORT_UNLESS(!FullLegacyName.empty());
        ClientsideName = FullLegacyName;
        ShortClientsideName = ShortLegacyName;
        auto& producer = config.GetProducer();
        if (!producer.empty()) {
            LegacyProducer = producer;
            LegacyLogtype = config.GetTopic();
        }
        if (LegacyProducer.empty()) {
            LegacyProducer = Account;
        }
        Y_ABORT_UNLESS(!FullModernName.empty());
        InternalName = FullLegacyName;
    }
}

const TString& TTopicNameConverter::GetAccount() const {
    return Account;
}

const TString& TTopicNameConverter::GetModernName() const {
    return FullModernName;
}

TString TTopicNameConverter::GetShortLegacyName() const {
    CHECK_VALID_AND_RETURN(ShortLegacyName);
}

TString TTopicNameConverter::GetInternalName() const {
    CHECK_VALID_AND_RETURN(InternalName);
}

const TString& TTopicNameConverter::GetClientsideName() const {
    Y_VERIFY_S(Valid, Reason.c_str());
    Y_ABORT_UNLESS(!ClientsideName.empty());
    return ClientsideName;
}

const TString& TTopicNameConverter::GetShortClientsideName() const {
    Y_ABORT_UNLESS(!ShortClientsideName.empty());
    return ShortClientsideName;
}

const TString& TTopicNameConverter::GetLegacyProducer() const {
    return LegacyProducer;
}

const TString& TTopicNameConverter::GetLegacyLogtype() const {
    return LegacyLogtype;
}


TString TTopicNameConverter::GetFederationPath() const {
    if (FstClass) {
        return ClientsideName;
    }

    return LbPath.GetOrElse("");
}


TString TTopicNameConverter::GetFederationPathWithDC() const {
    if (FstClass) {
        return ClientsideName;
    }

    return Account_ ? (*Account_ + "/" + FullModernName) : LbPath.GetOrElse("");
}


const TString& TTopicNameConverter::GetCluster() const {
    return Dc;
}

TString TTopicNameConverter::GetTopicForSrcId() const {
    if (!IsValid())
        return {};
    if (FstClass) {
        return StripLeadSlash(FullModernPath);
    } else {
        return GetClientsideName();
    }
}

TString TTopicNameConverter::GetTopicForSrcIdHash() const {
    if (!IsValid())
        return {};
    if (FstClass) {
        return StripLeadSlash(FullModernPath);
    } else {
        return ShortLegacyName;
    }
}

TString TTopicNameConverter::GetSecondaryPath() const {
    Y_VERIFY_S(Valid, Reason.c_str());
    if (!FstClass) {
        Y_ABORT_UNLESS(SecondaryPath.Defined());
        return *SecondaryPath;
    } else {
        return TString();
    }
}

bool TTopicNameConverter::IsFirstClass() const {
    return FstClass;
}

TTopicsListController::TTopicsListController(
        const std::shared_ptr<TTopicNamesConverterFactory>& converterFactory,
        const TVector<TString>& clusters
)
    : ConverterFactory(converterFactory)
{
    UpdateClusters(clusters);
}

void TTopicsListController::UpdateClusters(const TVector<TString>& clusters) {
    if (ConverterFactory->GetNoDCMode())
        return;

    Clusters = clusters;
}

TTopicsToConverter TTopicsListController::GetReadTopicsList(
        const THashSet<TString>& clientTopics, bool onlyLocal, const TString& database) const
{
    TTopicsToConverter result;
    auto PutTopic = [&] (const TString& topic, const TString& dc) {
        auto converter = ConverterFactory->MakeDiscoveryConverter(topic, {}, dc, database);
        if (!converter->IsValid()) {
            result.IsValid = false;
            result.Reason = TStringBuilder() << "Invalid topic format in init request: '" << converter->GetOriginalTopic()
                                             << "': " << converter->GetReason();
            return;
        }
        result.Topics[converter->GetOriginalPath()] = converter;
        result.ClientTopics[topic].push_back(converter);
    };

    for (const auto& t : clientTopics) {
        if (onlyLocal) {
            PutTopic(t, ConverterFactory->GetLocalCluster());
        } else if (!ConverterFactory->GetNoDCMode()){
            for(const auto& c : Clusters) {
                PutTopic(t, c);
            }
        } else {
            PutTopic(t, TString());
        }
        if (!result.IsValid)
            break;
    }
    return result;
}

TDiscoveryConverterPtr TTopicsListController::GetWriteTopicConverter(
        const TString& clientName, const TString& database
) {
    return ConverterFactory->MakeDiscoveryConverter(clientName, true,
                                                    ConverterFactory->GetLocalCluster(), database);
}

TConverterFactoryPtr TTopicsListController::GetConverterFactory() const {
    return ConverterFactory;
};

} // namespace NPersQueue
