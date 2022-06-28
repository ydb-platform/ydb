#include "topic_parser.h"

#include <ydb/core/base/appdata.h>

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

TString GetFullTopicPath(const NActors::TActorContext& ctx, const TMaybe<TString>& database, const TString& topicPath) {
    if (NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return FullPath(database, topicPath);
    } else {
        return topicPath;
    }
}

TString ConvertNewConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    if (NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertNewConsumerName(consumer);
    }
}

TString ConvertOldConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    if (NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertOldConsumerName(consumer);
    }
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

TString NormalizeFullPath(const TString& fullPath) {
    if (fullPath.StartsWith("/"))
        return fullPath.substr(1);
    else {
        return fullPath;
    }
}

TDiscoveryConverterPtr TDiscoveryConverter::ForFstClass(const TString& topic, const TString& database) {
    auto* res = new TDiscoveryConverter();
    res->FstClass = true;
    res->Database = database;
    res->OriginalTopic = topic;
    res->BuildFstClassNames();
    return TDiscoveryConverterPtr(res);
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
    if (name.empty()) {
        name = pqTabletConfig.GetTopic();
    }
    Y_VERIFY(!name.empty());
    auto path = pqTabletConfig.GetTopicPath();
    if (path.empty()) {
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
            Y_VERIFY(res_);
            Database = NKikimr::JoinPath({TString(dbRoot), TString(acc)});
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
    } else {
        BuildForFederation(*Database, path); //, rootDatabases);
    }
}

void TDiscoveryConverter::BuildForFederation(const TStringBuf& databaseBuf, TStringBuf topicPath
                                             //, const TVector<TString>& rootDatabases
) {
    topicPath.SkipPrefix("/");
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
            topicPath.SkipPrefix(PQPrefix);
            topicPath.SkipPrefix("/");
        } else {
            topicPath.SkipPrefix(databaseBuf);
            topicPath.SkipPrefix("/");
        }
    } else if (IsPathPrefix(topicPath, PQPrefix)) {
        isRootDb = true;
        topicPath.SkipPrefix(PQPrefix);
        topicPath.SkipPrefix("/");
        root = PQPrefix;
    }
    if (!isRootDb) {
        topicPath.SkipPrefix(databaseBuf);
        topicPath.SkipPrefix("/");
        Database = databaseBuf;
    }

    OriginalTopic = topicPath;
    if (!isRootDb && Database.Defined()) {
        // Topic with valid non-root database. Parse as 'modern' name. Primary path is path in database.
        auto parsed = TryParseModernMirroredPath(topicPath);
        if (!Valid) {
            return;
        }
        if (!parsed) {
            ParseModernPath(topicPath);
        }
        if (!Valid)
            return;
        Y_VERIFY_DEBUG(!FullModernName.empty());
        PrimaryPath = NKikimr::JoinPath({*Database, FullModernName});
        if (!FullLegacyName.empty()) {
            SecondaryPath = NKikimr::JoinPath({PQPrefix, FullLegacyName});
        }
        BuildFromShortModernName();
    } else {
        if (root.empty()) {
            root = PQPrefix;
        }
        // Topic from PQ root - this is either federation path (account/topic)
        if (topicPath.find("/") != TString::npos) {
            BuildFromFederationPath(root);
        } else {
            // OR legacy name (rt3.sas--account--topic)
            BuildFromLegacyName(root); // Sets primary path;
        }
    }
}

TTopicConverterPtr TDiscoveryConverter::UpgradeToFullConverter(
        const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& ydbDatabaseRootOverride
) {
    Y_VERIFY_S(Valid, Reason.c_str());
    auto* res = new TTopicNameConverter(FstClass, PQPrefix, pqTabletConfig, ydbDatabaseRootOverride);
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
    FullModernPath = PrimaryPath;
};

void TDiscoveryConverter::BuildFromFederationPath(const TString& rootPrefix) {
    // This is federation (not first class) and we have topic specified as a path;
    // So only convention supported is 'account/dir/topic' and account in path matches LB account;
    TStringBuf topic(OriginalTopic);
    LbPath = OriginalTopic;
    TStringBuf fst, snd;
    auto res = topic.TrySplit("/", fst, snd);
    Y_VERIFY(res);
    Account_ = fst;
    ParseModernPath(snd);
    BuildFromShortModernName();
    Y_VERIFY_DEBUG(!FullLegacyName.empty());
    PrimaryPath = NKikimr::JoinPath({rootPrefix, FullLegacyName});

    PendingDatabase = true;
}

bool TDiscoveryConverter::TryParseModernMirroredPath(TStringBuf path) {
    if (!path.Contains(".") || !path.Contains("/mirrored-from-")) {
        return false;
    }
    TStringBuf fst, snd;
    auto res = path.TryRSplit("/", fst, snd);
    CHECK_SET_VALID(res, "Malformed mirrored topic path - expected to end with '/mirrored-from-<cluster>'", return false);
    CHECK_SET_VALID(!snd.empty(), "Malformed mirrored topic path - expected to end with '/mirrored-from-<cluster>",
                    return false);
    res = snd.SkipPrefix("mirrored-from-");
    CHECK_SET_VALID(res, "Malformed mirrored topic path - invalid '/mirrored-from-<cluster>; part" , return false);
    CHECK_SET_VALID(Dc.empty() || Dc == "unknown" || Dc == snd,
                    "Bad mirrored topic path - cluster in name mismatches with cluster provided",
                    return false);
    Dc = snd;
    FullModernName = path;
    path = fst;
    res = path.TryRSplit("/", fst, snd);
    if (res) {
        res = snd.SkipPrefix(".");
        CHECK_SET_VALID(res, "Malformed mirrored topic path - topic name is expected to start with '.'", return false);
        ModernName = NKikimr::JoinPath({TString(fst), TString(snd)});
    } else {
        res = path.SkipPrefix(".");
        CHECK_SET_VALID(res, "Malformed mirrored topic path - topic name is expected to start with '.'", return false);
        ModernName = path;
    }
    if (Account_.Defined()) {
        BuildFromShortModernName();
    }
    return true;
}

void TDiscoveryConverter::ParseModernPath(const TStringBuf& path) {
    // This is federation (not first class) and we have topic specified as a path;
    // So only convention supported is 'account/dir/topic' and account in path matches LB account;
    TStringBuilder pathAfterAccount;

    // Path after account would contain 'dir/topic' OR dir/.topic/mirrored-from-..
    if (!Dc.empty() && !LocalDc.empty() && Dc != LocalDc) {
        TStringBuf directories, topicName;
        auto res = path.TrySplit("/", directories, topicName);
        if (res) {
            pathAfterAccount << directories << "/." << topicName << "/mirrored-from-" << Dc;
        } else {
            pathAfterAccount << "." << path << "/mirrored-from-" << Dc;
        }
    } else {
        pathAfterAccount << path;
    }
    ModernName = path;
    FullModernName = pathAfterAccount;
    if (Account_.Defined()) {
        BuildFromShortModernName();
    }
}

void TDiscoveryConverter::BuildFromShortModernName() {
    Y_VERIFY(!ModernName.empty());
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
                        return);
    }
    LbPath = lbPath;
    FullLegacyName = TStringBuilder() << "rt3." << Dc << "--" << ShortLegacyName;
    LegacyProducer = legacyProducer;
    LegacyLogtype = logtype;
}

void TDiscoveryConverter::BuildFromLegacyName(const TString& rootPrefix, bool forceFullName) {
    TStringBuf topic (OriginalTopic);
    bool hasDcInName = topic.Contains("rt3.");
    TStringBuf fst, snd;
    Account_ = Nothing(); //Account must be parsed out of legacy topic name
    TString shortLegacyName, fullLegacyName;
    if (forceFullName) {
        CHECK_SET_VALID(hasDcInName,
                        TStringBuilder() << "Invalid topic name - " << OriginalTopic
                                         << " - expected legacy-style name like rt3.<dc>--<account>--<topic>",
                        return);
    }
    if (Dc.empty() && !hasDcInName) {
        Y_VERIFY(!FstClass);
        CHECK_SET_VALID(!LocalDc.empty(),
                        "Cannot determine DC: should specify either in topic name, Dc option or LocalDc option",
                        return);

        Dc = LocalDc;
    }

    if (hasDcInName) {
        fullLegacyName = topic;
        auto res = topic.SkipPrefix("rt3.");
        CHECK_SET_VALID(res, "Malformed full legacy topic name", return);
        res = topic.TrySplit("--", fst, snd);
        CHECK_SET_VALID(res, "Malformed legacy style topic name: contains 'rt3.', but no '--'.", return);
        CHECK_SET_VALID(Dc.empty() || Dc == fst, "DC specified both in topic name and separate option and they mismatch", return);
        Dc = fst;
        topic = snd;
    } else {
        Y_VERIFY(!Dc.empty());
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
        } else if (*Account_ != fst){
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
    if (!modernName.empty()) {
        modernName << topicName;
    }
    modernName << topicName;
    Y_VERIFY(!Dc.empty());
    bool isMirrored = (!LocalDc.empty() && Dc != LocalDc);
    if (isMirrored) {
        fullModernName << "." << topicName << "/mirrored-from-" << Dc;
    } else {
        fullModernName << topicName;
    }
    Y_VERIFY(!fullLegacyName.empty());

    ShortLegacyName = shortLegacyName;
    FullLegacyName = fullLegacyName;
    PrimaryPath = NKikimr::JoinPath({rootPrefix, fullLegacyName});

    FullModernName = fullModernName;
    ModernName = modernName;
    LbPath = NKikimr::JoinPath({*Account_, modernName});


    if (!Database.Defined()) {
        PendingDatabase = true;
    } else {
        SetDatabase("");
    }
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
    Y_VERIFY(!PendingDatabase);
    Y_VERIFY(SecondaryPath.Defined());
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
    Y_VERIFY(!FullModernName.empty());
    if (!SecondaryPath.Defined()) {
        SecondaryPath = NKikimr::JoinPath({*Database, FullModernName});
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
        res->BuildFromLegacyName(TString(normRoot), true);
        if (res->Valid && !isLocal && res->Dc == localDc) {
            res->Valid = false;
            res->Reason = TStringBuilder() << "Topic '" << schemeName << "' created as non-local in local cluster";
        }
    } else {
        if (schemeName.Contains("rt3.")) {
            res->Valid = false;
            res->Reason = "Legacy style topic should not be created outside of PQ root";
            return res;
        }
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
        if (parsed) {
            Y_VERIFY(!res->Dc.empty());
            if (!localDc.empty() && localDc == res->Dc) {
                res->Valid = false;
                res->Reason = TStringBuilder() << "Topic in modern mirrored-like style: " << schemeName
                                               << " cannot be created in the same cluster " << res->Dc;
                return res;
            }
        }
        if (isLocal) {
            if(parsed) {
                res->Valid = false;
                res->Reason = TStringBuilder() << "Topic in modern mirrored-like style: " << schemeName << ", created as local";
                return res;
            }
            if (localDc.empty()) {
                res->Valid = false;
                res->Reason = "Local DC option is mandatory when creating local modern-style topic";
                return res;
            }
            res->Dc = localDc;
            res->ParseModernPath(fullPath);
        } else {
            if (!parsed) {
                res->Valid = false;
                res->Reason = TStringBuilder() << "Topic in modern style with non-mirrored-name: " << schemeName
                                               << ", created as non-local";

                return res;
            }
        }
        Y_VERIFY(!res->FullModernName.empty());
        res->PrimaryPath = NKikimr::JoinPath({*res->Database, res->FullModernName});
    }
    if (res->IsValid()) {
        Y_VERIFY(res->Account_.Defined());
        Y_VERIFY(!res->LegacyProducer.empty());
        Y_VERIFY(!res->LegacyLogtype.empty());
        Y_VERIFY(!res->Dc.empty());
        Y_VERIFY(!res->FullLegacyName.empty());
        res->Account = *res->Account_;
        res->InternalName = res->FullLegacyName;
    }
    return res;
}

TTopicNameConverter::TTopicNameConverter(
        bool firstClass, const TString& pqPrefix,
        const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
        const TString& ydbDatabaseRootOverride
)
    : TDiscoveryConverter(firstClass, pqPrefix, pqTabletConfig, ydbDatabaseRootOverride)
{
    if (Valid) {
        BuildInternals(pqTabletConfig);
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
        Y_VERIFY(!path.empty());
        path.SkipPrefix(db);
        path.SkipPrefix("/");
        ClientsideName = path;
        ShortClientsideName = path;
        FullModernName = path;
        InternalName = PrimaryPath;
    } else {
        SetDatabase(*Database);
        Y_VERIFY(!FullLegacyName.empty());
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
        Y_VERIFY(!FullModernName.empty());
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
    Y_VERIFY(!ClientsideName.empty());
    return ClientsideName;
}

const TString& TTopicNameConverter::GetShortClientsideName() const {
    Y_VERIFY(!ShortClientsideName.empty());
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

const TString& TTopicNameConverter::GetCluster() const {
    return Dc;
}

TString TTopicNameConverter::GetTopicForSrcId() const {
    if (!IsValid())
        return {};
    if (FstClass) {
        return FullModernPath;
    } else {
        return GetClientsideName();
    }
}

TString TTopicNameConverter::GetTopicForSrcIdHash() const {
    if (!IsValid())
        return {};
    if (FstClass) {
        return FullModernPath;
    } else {
        return ShortLegacyName;
    }
}

TString TTopicNameConverter::GetSecondaryPath() const {
    Y_VERIFY_S(Valid, Reason.c_str());
    if (!FstClass) {
        Y_VERIFY(SecondaryPath.Defined());
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

