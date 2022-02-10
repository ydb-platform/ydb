#pragma once

#include <library/cpp/actors/core/actor.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <ydb/core/base/path.h> 
#include <ydb/library/persqueue/topic_parser_public/topic_parser.h> 

namespace NPersQueue {

TString GetFullTopicPath(const NActors::TActorContext& ctx, TMaybe<TString> database, const TString& topicPath);
TString ConvertNewConsumerName(const TString& consumer, const NActors::TActorContext& ctx);
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
        Y_VERIFY(!result.empty());        \
        return result;                    \
    }

class TTopicNamesConverterFactory;

class TTopicNameConverter {
    friend class TTopicNamesConverterFactory;

public:
    TTopicNameConverter() = default;

protected:
    TTopicNameConverter(bool noDcMode, TStringBuf& pqNormalizedRootPrefix)
        : NoDcMode(noDcMode)
        , PQRootPrefix(pqNormalizedRootPrefix)
    {}
 
    TTopicNameConverter& ApplyToTopic(
            const TString& topic, const TString& dc = TString(), const TString& database = TString()
    ) {
        OriginalTopic = topic;

        TStringBuf topicNormalized = topic;
        topicNormalized.SkipPrefix("/");

        TStringBuf dbNormalized = database;
        dbNormalized.SkipPrefix("/");
        dbNormalized.ChopSuffix("/");

        TStringBuf root;
        if (NoDcMode || !PQRootPrefix.StartsWith(dbNormalized)) {
            Database = database;
            root = dbNormalized;
        } else {
            root = PQRootPrefix;
        }
        topicNormalized.SkipPrefix(root);
        topicNormalized.SkipPrefix("/");
        Dc = dc;
        bool supposeLegacyMode = Database.empty() && !NoDcMode;
        auto hasDcInName = IsLegacyTopicNameWithDc(topicNormalized);

        CHECK_SET_VALID(!hasDcInName || dc.Empty(),  "May not specify DC with legacy-style topic name containing dc", return *this);
        if (NoDcMode) {
            CHECK_SET_VALID(!Database.empty(), "Database is mandatory in FirstClass mode", return *this);
        } else {
            HasDc = hasDcInName || !dc.empty();
        }

        if (hasDcInName) {
            supposeLegacyMode = true;
            SplitFullLegacyName(topicNormalized);
            return *this;
        } else if (IsLegacyTopicName(topicNormalized)) { // Also must get here if 'SplitFullLegacyName' performed
            supposeLegacyMode = true;
            SplitShortLegacyName(topicNormalized);
            return *this;
        } else {
            SplitModernName(topicNormalized);

            BuildModernNames(TString(root));
            if (supposeLegacyMode) {
                ClientsideName = FullLegacyName;
                PrimaryFullPath = FullLegacyPath;
            } else {
                ClientsideName = ModernName;
                PrimaryFullPath = FullModernPath;
            }
        }
        return *this;
    }

public:
    bool IsValid() const {
        return Valid;
    }

    const TString& GetReason() {
        return Reason;
    }

    TString GetClientsideName() {
        CHECK_VALID_AND_RETURN(ClientsideName);
    }

    TString GetPrimaryPath() {
        CHECK_VALID_AND_RETURN(PrimaryFullPath);
    }

    TString GetSecondaryPath() {
        Y_FAIL("UNIMPLEMENTED");
        CHECK_VALID_AND_RETURN(SecondaryFullPath);
    }

    TString GetOriginalPath() const {
        CHECK_VALID_AND_RETURN(OriginalTopic);
    }

    TString GetShortLegacyName() const {
        CHECK_VALID_AND_RETURN(LegacyName);
    }

    TString GetFullLegacyName() const {
        CHECK_VALID_AND_RETURN(FullLegacyName);
    }

    TString GetFullLegacyPath() const {
        CHECK_VALID_AND_RETURN(FullLegacyPath);
    }
    TString GetLegacyProducer() const {
        CHECK_VALID_AND_RETURN(Producer);
    }

    TString GetModernName() const {
        CHECK_VALID_AND_RETURN(ModernName);
    }

    TString GetModernPath() const {
        CHECK_VALID_AND_RETURN(FullModernPath);
    }
    TString GetCluster() const {
        return Dc;
    }

    TString GetAccount() const {
        if (!Valid)
            return {};
        if (!Database.empty()) {
            return Database;
        }
        TStringBuf buf(ModernName);
        TStringBuf fst, snd;
        auto res = buf.TrySplit("/", fst, snd);
        if (!res) {
            fst = buf;
        }
        return TString{fst};
    }

private:
    bool IsLegacyTopicName(const TStringBuf& path) {
        if (path.find("--") != TStringBuf::npos)
            return true;
        if (Database.empty() && !NoDcMode && path.find("/") == TStringBuf::npos)
            return true;
        return false;
    }

    bool IsLegacyTopicNameWithDc(const TStringBuf& path) {
        return path.StartsWith("rt3.");
    }

    void SplitFullLegacyName(TStringBuf& name) {
        // rt3.sas--accountName[@dirName]--topicName
        FullLegacyName = name;
        TStringBuf fst, snd;
        name.SkipPrefix("rt3.");
        auto res = name.TrySplit("--", fst, snd);
        CHECK_SET_VALID(res, Sprintf("topic name %s is corrupted", OriginalTopic.c_str()), return);
        Dc = fst;
        LegacyName = snd;
        FullLegacyPath = NKikimr::JoinPath({TString(PQRootPrefix), FullLegacyName});
        SplitShortLegacyName(snd);
    }

    void SplitShortLegacyName(const TStringBuf& name) {
        if (LegacyName.empty())
            LegacyName = name;
        CHECK_SET_VALID(!NoDcMode, "legacy-style topic name as first-class citizen", return);
        TStringBuf buf, logtype;
        TStringBuilder topicPath;
        auto res = name.TryRSplit("--", buf, logtype);
        if (!res) {
            topicPath << name;
            Producer = name;
        } else {
            Producer = buf;
            while (true) {
                TStringBuf fst, snd;
                if (!buf.TrySplit("@", fst, snd))
                    break;
                topicPath << fst << "/";
                buf = fst;
            }
            topicPath << buf << "/" << logtype;
        }
        ModernName = topicPath;
        BuildFullLegacyName();
        PrimaryFullPath = FullLegacyPath;
        if (ClientsideName.empty() && !NoDcMode && !FullLegacyName.empty()) {
            ClientsideName = FullLegacyName;
        }
    }

    void SplitModernName(TStringBuf& name) {
        if (ModernName.empty()) {
            ModernName = name;
        }
        TStringBuf fst, snd;

        auto res = name.TrySplit("/", fst, snd);
        if (!res) {
            LegacyName = name;
            Producer = name;
        } else {
            TStringBuilder legacyName;
            legacyName << fst;
            while (true) {
                name = snd;
                res = name.TrySplit("/", fst, snd);
                if (!res)
                    break;
                legacyName << "@" << fst;
            }
            Producer = legacyName;
            legacyName << "--" << name;
            LegacyName = legacyName;
        }
        BuildFullLegacyName();
    }

    void BuildModernNames(const TString& prefix) {
        if (!NoDcMode && !Dc.empty()) {
            // ToDo[migration]: build full modern name, full modern path
        } else {
            FullModernName = ModernName;
            FullModernPath = NKikimr::JoinPath({prefix, FullModernName});
        }
    }
    void BuildFullLegacyName() {
        if (FullLegacyName.empty() || !Dc.empty()) {
            TStringBuilder builder;
            builder << "rt3." << Dc << "--" << LegacyName;
            FullLegacyName = builder;
        }
        if (FullLegacyPath.empty() && !FullLegacyName.empty()) {
            FullLegacyPath = NKikimr::JoinPath({PQRootPrefix, FullLegacyName});
        }
    }


    bool NoDcMode;
    TString PQRootPrefix;

    TString OriginalTopic;
    TString Database;
    TString Dc;

    TString LegacyName;
    TString FullLegacyName;
    TString FullLegacyPath;

    TString ModernName;
    TString FullModernName;
    TString FullModernPath;

    TString PrimaryFullPath;
    TString SecondaryFullPath;
    TString ClientsideName;

    TString Producer;
    TString Account;
    bool HasDc = false;

    bool Valid = true;
    TString Reason;
};

using TConverterPtr = std::shared_ptr<TTopicNameConverter>;

class TTopicNamesConverterFactory {
public:
    TTopicNamesConverterFactory(bool noDcMode, const TString& pqRootPrefix)
        : NoDcMode(noDcMode)
        , PQRootPrefix(pqRootPrefix)
        , NormalizedPrefix(PQRootPrefix)
    {
        NormalizedPrefix.SkipPrefix("/");
        NormalizedPrefix.ChopSuffix("/");
    }

    TConverterPtr MakeTopicNameConverter(
            const TString& topic, const TString& dc = TString(), const TString& database = TString()
    ) {
        auto* converter = new TTopicNameConverter(NoDcMode, NormalizedPrefix);
        converter->ApplyToTopic(topic, dc, database);
        return TConverterPtr(converter);
    }

private:
    bool NoDcMode;
    TString PQRootPrefix;
    TStringBuf NormalizedPrefix;
};

using TConverterFactoryPtr = std::shared_ptr<NPersQueue::TTopicNamesConverterFactory>;
using TTopicsToConverter = THashMap<TString, TConverterPtr>;

class TTopicsListController {
public:
    TTopicsListController(const TConverterFactoryPtr& converterFactory,
                          bool haveClusters, const TVector<TString>& clusters = {}, const TString& localCluster = {});

    void UpdateClusters(const TVector<TString>& clusters, const TString& localCluster);
    TTopicsToConverter GetReadTopicsList(const THashSet<TString>& clientTopics, bool onlyLocal, const TString& database) const;
    TConverterPtr GetWriteTopicConverter(const TString& clientName, const TString& database);
    TConverterFactoryPtr GetConverterFactory() const;

    bool GetHaveClusters() const { return HaveClusters;};

private:
    TConverterFactoryPtr ConverterFactory;
    bool HaveClusters;
    TVector<TString> Clusters;
    TString LocalCluster;

};

TString NormalizeFullPath(const TString& fullPath);


} // namespace NPersQueue

