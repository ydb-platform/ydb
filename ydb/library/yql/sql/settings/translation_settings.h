#pragma once

#include <ydb/library/yql/core/pg_settings/guc_settings.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

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

    enum class EBindingsMode {
        // raise error
        DISABLED,
        // classic support for bindings
        ENABLED,
        // bindings.my_binding -> current_cluster.my_binding + raise warning
        DROP_WITH_WARNING,
        // bindings.my_binding -> current_cluster.my_binding
        DROP
    };

    inline bool IsQueryMode(NSQLTranslation::ESqlMode mode) {
        return mode == NSQLTranslation::ESqlMode::QUERY || mode == NSQLTranslation::ESqlMode::DISCOVERY;
    }

    using TIncrementMonCounterFunction = std::function<void(const TString&, const TString&)>;

    // persisted
    enum class EV0Behavior : ui32 {
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

        static TPtr MakeAlwaysAllow();

        static TPtr Make(bool allow);
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

        EBindingsMode BindingsMode;
        THashMap<TString, TTableBindingSettings> Bindings;
        bool SaveWorldDependencies = false;

        // each (name, type) entry in this map is equivalent to
        // DECLARE $name AS type;
        // NOTE: DECLARE statement in SQL text overrides entry in DeclaredNamedExprs
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
        ISqlFeaturePolicy::TPtr BlockDefaultAuto;
        bool AssumeYdbOnClusterWithSlash;
        TString DynamicClusterProvider;
        TString FileAliasPrefix;

        TVector<ui32> PgParameterTypeOids;
        bool AutoParametrizeEnabled = false;
        bool AutoParametrizeValuesStmt = false;

        TGUCSettings::TPtr GUCSettings = std::make_shared<TGUCSettings>();
        bool UnicodeLiterals = false;

        TMaybe<TString> ApplicationName;
        bool PgSortNulls = false;
    };

    bool ParseTranslationSettings(const TString& query, NSQLTranslation::TTranslationSettings& settings, NYql::TIssues& issues);

}  // namespace NSQLTranslation
