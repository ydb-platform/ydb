#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <ydb/library/persqueue/topic_parser_public/topic_parser.h>


namespace NKikimr::NMsgBusProxy::NPqMetaCacheV2 {
    class TPersQueueMetaCacheActor;
}

namespace NPersQueue {

TString GetFullTopicPath(const NActors::TActorContext& ctx, const TMaybe<TString>& database, const TString& topicPath);
TString ConvertNewConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig);
TString ConvertNewConsumerName(const TString& consumer, const NActors::TActorContext& ctx);
TString ConvertOldConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig);
TString ConvertOldConsumerName(const TString& consumer, const NActors::TActorContext& ctx);
TString MakeConsumerPath(const TString& consumer);


#define CHECK_SET_VALID(cond, reason, statement) \
    if (!(cond)) {                               \
        Valid = false;                           \
        Reason = Reason + (reason) + ". ";       \
        statement;                               \
    }


#define CHECK_VALID_AND_RETURN(result)    \
    if (!Valid) {                         \
        return TString();                 \
    } else {                              \
        Y_VERIFY_S(!result.empty(), OriginalTopic.c_str());        \
        return result;                    \
    }

class TTopicNameConverter;
class TTopicNamesConverterFactory;

namespace NTests {
    class TConverterTestWrapper;
};

class TDiscoveryConverter {
    friend class TTopicNamesConverterFactory;
    friend class NKikimr::NMsgBusProxy::NPqMetaCacheV2::TPersQueueMetaCacheActor;
    friend class NTests::TConverterTestWrapper;

    using TDiscoveryConverterPtr = std::shared_ptr<TDiscoveryConverter>;
    using TTopicConverterPtr = std::shared_ptr<TTopicNameConverter>;

private:
    void BuildForFederation(const TStringBuf& databaseBuf, TStringBuf topicPath);
    void BuildFstClassNames();
    [[nodiscard]] bool BuildFromFederationPath(const TString& rootPrefix);
    [[nodiscard]] bool BuildFromShortModernName();

protected:
    TDiscoveryConverter() = default;
    static TDiscoveryConverterPtr ForFstClass(const TString& topic, const TString& database);
    static TDiscoveryConverterPtr ForFederation(const TString& topic, const TString& dc, const TString& localDc,
                                                const TString& database, const TString& pqNormalizedPrefix);


    TDiscoveryConverter(bool firstClass, const TString& pqNormalizedPrefix,
                        const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const TString& ydbDatabaseRootOverride);

    [[nodiscard]] bool BuildFromLegacyName(const TString& rootPrefix, bool forceFullname = false);
    [[nodiscard]] bool TryParseModernMirroredPath(TStringBuf path);
    [[nodiscard]] bool ParseModernPath(const TStringBuf& path);

public:
    bool IsValid() const;
    const TString& GetReason() const;
    const TString& GetOriginalTopic() const;
    TTopicConverterPtr UpgradeToFullConverter(const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
                                              const TString& ydbDatabaseRootOverride,
                                              const TMaybe<TString>& clientsideNameOverride = {});

    TString GetPrintableString() const;


    /** For first class:
     * /database/dir/my-stream
     *
     * For federation (currently) - full legacy path:
     * /Root/PQ/rt3.dc--account@dir--topic
     * Except for topics with explicitly specified database:
     * /user-database/dir/my-topic
     * OR
     * /user-database/dir/.my-topic/mirrored-from-dc
     */
    TString GetPrimaryPath() const;

    TString GetOriginalPath() const;

    /** Second path if applicable:
     * Nothing for first class
     * Nothing for topic with explicitly specified non-root Database
     *
     * Full modern path for topics requested from /Root (/Root/PQ) or in legacy style:
     * /user-database/dir/my-topic
     * OR
     * /user-database/dir/.my-topic/mirrored-from-dc
     */
    const TMaybe<TString>& GetSecondaryPath(const TString& databasePath);

    /**
    Only for special cases.
    */
    void SetPrimaryPath(const TString& path) {
        OriginalPath = PrimaryPath;
        PrimaryPath = path;
    }

    void RestorePrimaryPath() {
        Y_ABORT_UNLESS(!OriginalPath.empty());
        PrimaryPath = OriginalPath;
        OriginalPath.clear();
    }

    // Only for control plane
    const TString& GetFullModernName() const {
        Y_ABORT_UNLESS(!FullModernName.empty());
        return FullModernName;
    }


protected:
    /** Account will be set for federation topics without database only;
     * Generally used only for discovery purposes
     */
    const TMaybe<TString>& GetAccount_() const;

    /**
     * Set database for topics specified via account-in-directory mode. Used for discovery purpose in metacache
     */
    void SetDatabase(const TString& database);
    const TMaybe<TString>& GetDatabase() const { return Database;}


protected:
    TString OriginalTopic;
    TString Dc;
    bool MayUseLocalDc = true;
    TMaybe<TString> Database;
    TString LocalDc;

    TString PQPrefix;
    //TVector<TString> RootDatabases;

    TString OriginalPath;
    TString PrimaryPath;
    bool PendingDatabase = false;
    TMaybe<TString> SecondaryPath;
    TMaybe<TString> Account_;
    TMaybe<TString> LbPath;
    TMaybe<TString> Topic;


protected:
    bool FstClass;

    bool Valid = true;
    TString Reason;

    TString FullModernPath;
    TString ModernName;
    TString FullModernName;
    TString ShortLegacyName;
    TString FullLegacyName;
    TString LegacyProducer;
    TString LegacyLogtype;
};

class TTopicNameConverter : public TDiscoveryConverter {
    using TTopicConverterPtr = std::shared_ptr<TTopicNameConverter>;
    friend class TDiscoveryConverter;
protected:
    TTopicNameConverter() = default;
    TTopicNameConverter(bool firstClass,
                        const TString& pqPrefix,
                        //const TVector<TString>& rootDatabases,
                        const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
                        const TString& ydbDatabaseRootOverride,
                        const TMaybe<TString>& clientsideNameOverride = {});
public:

    static TTopicConverterPtr ForFirstClass(const NKikimrPQ::TPQTabletConfig& pqTabletConfig);

    /** For federation only. legacyBasedApi flag used to indicate thad protocol used prefers legacy style topics
     * (i.e. legacy and PQv0)*/
    static TTopicConverterPtr ForFederation(const TString& pqPrefix,
                                            //const TVector<TString>& rootDatabases,
                                            const NKikimrPQ::TPQTabletConfig& pqTabletConfig,
                                            const TString& ydbDatabaseRootOverride);

    static TTopicConverterPtr ForFederation(const TString& pqRoot, const TString& ydbTestDatabaseRoot,
                                            const TString& schemeName, const TString& schemeDir,
                                            const TString& database, bool isLocal,
                                            const TString& localDc = TString(),
                                            const TString& federationAccount = TString());

    /** Returns name for interaction client, such as locks and (maybe) sensors.
     * rt3.dc--account@dir--topic for federation
     * topic path relative to database for 1st class */
    const TString& GetClientsideName() const;

    /** Same as clientside name, but without dc for federation ("account@dir--topic"). */
    const TString& GetShortClientsideName() const;

//    /** Legacy name with Dc - 'rt3.dc.account--topic. Undefined for first-class */
//    TString GetFullLegacyName() const;

    /** Legacy name without Dc - 'account--topic. Undefined for first-class */
    TString GetShortLegacyName() const;

    /** Producer in legacy and PQv0. Undefined for first class */
    const TString& GetLegacyProducer() const;

    const TString& GetLegacyLogtype() const;

    /** Account. Only defined for topics where it was specified upon creating (or alter) */
    //ToDo - should verify that we'll actually have it everywhere required prior to use.
    const TString& GetAccount() const;

    /** Get logbroker logical path, 'account/dir/topic'. Generally this is not topic location path.
     * Identical for clientside name for 1st class */
    TString GetFederationPath() const;
    TString GetFederationPathWithDC() const;


    /** Gets DC */
    const TString& GetCluster() const;

    /** Gets clientside name for migrated topic.
     * `dir/topic` OR `dir/.topic/mirrored-from-dc` for mirrored topics.
     * (No federation account).
     * Database + ModernName compose full path for migrated topic.
     * */
    const TString& GetModernName() const;

    /** Unique convertable name for internal purposes. Maybe uses hashing/for mappings.
     * Single topic in any representation is supposed to have same internal name
     * DO NOT use for business logic, such as sensors, SourceIds, etc
     *
     * /user-database/dir/my-topic
     * OR
     * /user-database/dir/.my-topic/mirrored-from-dc
     *
     * */
    TString GetInternalName() const;


    TString GetTopicForSrcId() const;
    TString GetTopicForSrcIdHash() const;

    TString GetSecondaryPath() const;

    bool IsFirstClass() const;

    operator bool() const { return Valid && !ClientsideName; };

private:
    void BuildInternals(const NKikimrPQ::TPQTabletConfig& config);

private:
    TString ClientsideName;
    TString ShortClientsideName;
    TString Account;
    TString InternalName;
};

using TDiscoveryConverterPtr = std::shared_ptr<TDiscoveryConverter>;
using TTopicConverterPtr = std::shared_ptr<TTopicNameConverter>;

class TTopicNamesConverterFactory {
public:
    TTopicNamesConverterFactory(
            bool noDcMode, const TString& pqRootPrefix,
            const TString& localDc, TMaybe<bool> isLocalDc = Nothing()
    )
        : NoDcMode(noDcMode)
        , PQRootPrefix(pqRootPrefix)
    {
        if (isLocalDc.Defined()) {
            IsLocalDc = *isLocalDc;
        } else {
            LocalDc = localDc;
        }
        SetPQNormPrefix();
    }

    TTopicNamesConverterFactory(
            const NKikimrPQ::TPQConfig& pqConfig, const TString& localDc, TMaybe<bool> isLocalDc = Nothing()
    )
    {
        if (isLocalDc.Defined()) {
            IsLocalDc = *isLocalDc;
        } else {
            LocalDc = localDc;
        }
        // The part after ' || ' is actually a hack. FirstClass citizen is expected to be explicitly set,
        // but some tests (such as CDC) run without PQConfig at all
        NoDcMode = pqConfig.GetTopicsAreFirstClassCitizen() || !pqConfig.GetEnabled();
        PQRootPrefix = pqConfig.GetRoot();
        YdbDatabasePathOverride = pqConfig.GetTestDatabaseRoot();
        SetPQNormPrefix();
    }

    TDiscoveryConverterPtr MakeDiscoveryConverter(
            const TString& topic, TMaybe<bool> isInLocalDc, const TString& dc = TString(), const TString& database = TString()
    ) {
        if (NoDcMode) {
            return TDiscoveryConverter::ForFstClass(topic, database);
        } else {
            TString localDc;
            if (!IsLocalDc.Defined()) {
                localDc = LocalDc;
            } else if (IsLocalDc.GetRef()) {
                if (!dc.empty()) {
                    localDc = dc;
                } else {
                    localDc = ".local";
                }
            } else {
                localDc = dc + ".non-local"; // Just always mismatch with any DC;
            }
            if (isInLocalDc.Defined()) {
                if (*isInLocalDc) {
                    return TDiscoveryConverter::ForFederation(
                            topic, localDc, localDc, database, NormalizedPrefix//, RootDatabases
                    );
                } else if (dc.empty()) {
                    TDiscoveryConverterPtr converter{new TDiscoveryConverter()};
                    converter->Valid = false;
                    converter->Reason = TStringBuilder() << "DC should be explicitly specified for topic " << topic << Endl;
                    return converter;
                }
            }
            return TDiscoveryConverter::ForFederation(topic, dc, localDc, database, NormalizedPrefix); //, RootDatabases);
        }
    }

    TTopicConverterPtr MakeTopicConverter(
            const NKikimrPQ::TPQTabletConfig& pqTabletConfig
    ) {
        TTopicConverterPtr converter;
        if (NoDcMode) {
            converter = TTopicNameConverter::ForFirstClass(pqTabletConfig);
        } else {
            converter = TTopicNameConverter::ForFederation(NormalizedPrefix, pqTabletConfig, YdbDatabasePathOverride);
        }
        return converter;
    }

    void SetLocalCluster(const TString& localCluster) { LocalDc = localCluster;}
    TString GetLocalCluster() const { return LocalDc; }
    bool GetNoDCMode() const { return NoDcMode; }

private:
    void SetPQNormPrefix() {
        TStringBuf prefix(PQRootPrefix);
        prefix.SkipPrefix("/");
        prefix.ChopSuffix("/");
        NormalizedPrefix = prefix;
    }

    bool NoDcMode;
    TString PQRootPrefix;
    TString LocalDc;
    TMaybe<bool> IsLocalDc;
    TString NormalizedPrefix;
    TString YdbDatabasePathOverride;
};

using TConverterFactoryPtr = std::shared_ptr<NPersQueue::TTopicNamesConverterFactory>;

struct TTopicsToConverter {
    THashMap<TString, TDiscoveryConverterPtr> Topics;
    THashMap<TString, TVector<TDiscoveryConverterPtr>> ClientTopics;
    bool IsValid = true;
    TString Reason;
};

class TTopicsListController {
public:
    TTopicsListController(const TConverterFactoryPtr& converterFactory,
                          const TVector<TString>& clusters = {});

    void UpdateClusters(const TVector<TString>& clusters);
    TTopicsToConverter GetReadTopicsList(const THashSet<TString>& clientTopics, bool onlyLocal, const TString& database) const;
    TDiscoveryConverterPtr GetWriteTopicConverter(const TString& clientName, const TString& database);
    TConverterFactoryPtr GetConverterFactory() const;

    TString GetLocalCluster() const { return ConverterFactory->GetLocalCluster(); }
private:
    TConverterFactoryPtr ConverterFactory;
    TVector<TString> Clusters;

public:
};

TString NormalizeFullPath(const TString& fullPath);
TString StripLeadSlash(const TString& path);


} // namespace NPersQueue

