#pragma once

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>

namespace google::protobuf {
    class Arena;
}

namespace NYql {
    class TIssues;
}

namespace NSQLTranslation {
    constexpr const size_t SQL_MAX_PARSER_ERRORS = 100;

    enum class ESqlMode {
        QUERY = 0,
        LIMITED_VIEW = 1,
        LIBRARY = 2,
        SUBQUERY = 3,
        DISCOVERY = 4,
    };

    inline bool IsQueryMode(NSQLTranslation::ESqlMode mode) {
        return mode == NSQLTranslation::ESqlMode::QUERY || mode == NSQLTranslation::ESqlMode::DISCOVERY;
    }

    using TIncrementMonCounterFunction = std::function<void(const TString&, const TString&)>;

    enum class EV0Behavior {
        Silent = 0,
        Report,
        Disable
    };

    class ISqlFeaturePolicy : public TThrRefBase {
    public:
        virtual ~ISqlFeaturePolicy() = default;
        virtual bool Allow() const = 0;

        using TPtr = TIntrusivePtr<ISqlFeaturePolicy>;

        static TPtr MakeAlwaysDisallow();
    };

    struct TTableBindingSettings {
        TString ClusterType;
        THashMap<TString, TString> Settings;
    };

    struct TTranslationSettings
    {
        TTranslationSettings();
        google::protobuf::Arena* Arena = nullptr;

        THashMap<TString, TString> ClusterMapping;
        TString PathPrefix;
        // keys (cluster name) should be normalized
        THashMap<TString, TString> ClusterPathPrefixes;
        THashMap<TString, TString> ModuleMapping;
        THashSet<TString> Libraries;
        THashSet<TString> Flags;

        THashMap<TString, TTableBindingSettings> PrivateBindings;
        THashMap<TString, TTableBindingSettings> ScopedBindings;

        // each (name, type) entry in this map is equivalent to
        // DECLARE $name AS type;
        TMap<TString, TString> DeclaredNamedExprs;

        ESqlMode Mode;
        TString DefaultCluster;
        TIncrementMonCounterFunction IncrementCounter;
        size_t MaxErrors;
        bool EndOfQueryCommit;
        TString File;
        bool EnableGenericUdfs;
        ui16 SyntaxVersion;
        bool AnsiLexer;
        bool PgParser;
        bool InferSyntaxVersion;
        EV0Behavior V0Behavior;
        bool V0ForceDisable;
        bool WarnOnV0;
        ISqlFeaturePolicy::TPtr V0WarnAsError;
        ISqlFeaturePolicy::TPtr DqDefaultAuto;
        bool AssumeYdbOnClusterWithSlash;
    };

    bool ParseTranslationSettings(const TString& query, NSQLTranslation::TTranslationSettings& settings, NYql::TIssues& issues);

}  // namespace NSQLTranslation
