#pragma once

#include "node.h"
#include "sql.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/public/issue/yql_warning.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/sql/cluster_mapping.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/stack.h>
#include <util/generic/vector.h>

namespace NSQLTranslationV0 {

    typedef TMap<TString, TNodePtr> TNamedNodesMap;

    class TContext {
    public:
        TContext(const NSQLTranslation::TTranslationSettings& settings,
                 NYql::TIssues& issues);

        virtual ~TContext();

        const NYql::TPosition& Pos() const;

        void ClearBlockScope();
        TString MakeName(const TString& name);

        IOutputStream& Error();
        IOutputStream& Error(NYql::TPosition pos);
        IOutputStream& Warning(NYql::TPosition pos, NYql::TIssueCode code);
        IOutputStream& Info(NYql::TPosition pos);

        template <typename TToken>
        const TString& Token(const TToken& token) {
            Position.Row = token.GetLine();
            Position.Column = token.GetColumn() + 1;
            return token.GetValue();
        }

        template <typename TToken>
        TPosition TokenPosition(const TToken& token) {
            TPosition pos = Position;
            pos.Row = token.GetLine();
            pos.Column = token.GetColumn() + 1;
            return pos;
        }

        inline void IncrementMonCounter(const TString& name, const TString& value) {
            if (IncrementMonCounterFunction) {
                IncrementMonCounterFunction(name, value);
            }
        }

        bool HasCluster(const TString& cluster) const {
            return GetClusterProvider(cluster).Defined();
        }

        TMaybe<TString> GetClusterProvider(const TString& cluster) const {
            TString unusedNormalizedClusterName;
            return GetClusterProvider(cluster, unusedNormalizedClusterName);
        }

        TMaybe<TString> GetClusterProvider(const TString& cluster, TString& normalizedClusterName) const {
            auto provider = ClusterMapping.GetClusterProvider(cluster, normalizedClusterName);
            if (!provider) {
                if (Settings.AssumeYdbOnClusterWithSlash && cluster.StartsWith('/')) {
                    normalizedClusterName = cluster;
                    return TString(NYql::KikimrProviderName);
                }
                return Nothing();
            }

            return provider;
        }

        TString GetServiceName(const ISource& source) const;
        bool UseUnordered(const ISource& source) const;
        bool UseUnordered(const TTableRef& table) const;

        bool SetPathPrefix(const TString& value, TMaybe<TString> arg = TMaybe<TString>());

        TNodePtr GetPrefixedPath(const TString& cluster, const TDeferredAtom& path);

        TNodePtr UniversalAlias(const TString& baseName, TNodePtr&& node);

        TString HasBlockShortcut(const TNodePtr& baseNode);
        TString RegisterBlockShortcut(const TNodePtr& baseNode, const TNodePtr& node, const TString& baseName);
        TNodePtr GetBlockShortcut(const TString& alias) const;
        TNodePtr GroundBlockShortcuts(NYql::TPosition pos, TNodePtr groundList = {});
        TNodePtr GroundBlockShortcutsForExpr(const TNodePtr& expr);

        void PushBlockShortcuts();
        void PopBlockShortcuts();

        void BodyPart() {
            IntoHeading = false;
        }

        bool IsParseHeading() const {
            return IntoHeading;
        }

        bool DeclareVariable(const TString& varName, const TNodePtr& typeNode);

        bool AddExports(const TVector<TString>& symbols);
        TString AddImport(const TVector<TString>& modulePath);
        TString AddSimpleUdf(const TString& udf);
    private:
        IOutputStream& MakeIssue(NYql::ESeverity severity, NYql::TIssueCode code, NYql::TPosition pos);

    private:
        NYql::TPosition Position;
        THolder<TStringOutput> IssueMsgHolder;
        NSQLTranslation::TClusterMapping ClusterMapping;
        TString PathPrefix;
        THashMap<TString, TString> ProviderPathPrefixes;
        THashMap<TString, TString> ClusterPathPrefixes;
        bool IntoHeading = true;

    public:
        THashMap<TString, TNodePtr> Variables;
        NSQLTranslation::TTranslationSettings Settings;
        std::unique_ptr<TMemoryPool> Pool;
        NYql::TIssues& Issues;
        TMap<TString, TStack<TNodePtr>> NamedNodes;
        TMap<TString, TNodePtr> UniversalAliases;
        THashSet<TString> Exports;
        THashMap<TString, TString> ImportModuleAliases;
        TMap<TString, TString> SimpleUdfs;
        NSQLTranslation::TIncrementMonCounterFunction IncrementMonCounterFunction;
        TString CurrCluster;
        bool HasPendingErrors;
        THashMap<TString, ui32> GenIndexes;
        bool PragmaRefSelect = false;
        bool PragmaSampleSelect = false;
        bool PragmaAllowDotInAlias = false;
        bool PragmaInferSchema = false;
        bool PragmaAutoCommit = false;
        bool SimpleColumns = false;
        bool PragmaDirectRead = false;
        bool PragmaYsonAutoConvert = false;
        bool PragmaYsonStrict = false;
        bool PragmaClassicDivision = true;
        bool PragmaPullUpFlatMapOverJoin = true;
        bool EnableSystemColumns = true;
        ui32 ResultRowsLimit = 0;
        ui64 ResultSizeLimit = 0;
        ui32 PragmaGroupByLimit = 1 << 6;
        ui32 PragmaGroupByCubeLimit = 5;
        THashSet<TString> Libraries;
        NYql::TWarningPolicy WarningPolicy;
        TVector<TString> AllResults;
        TSet<TString> UsedClusters;

        struct ShortcutStore {
            THashMap<INode*, TString> BaseMap;
            TVector<std::pair<TString, TNodePtr>> Goal;
        };
        THashMap<ui32, ShortcutStore> Shortcuts;
        ui32 ShortcutCurrentLevel = 0;

    };

    class TTranslation {
    protected:
        typedef TSet<ui32> TSetType;

    protected:
        TTranslation(TContext& ctx);

    public:
        TContext& Context();
        IOutputStream& Error();

        template <typename TToken>
        const TString& Token(const TToken& token) {
            return Ctx.Token(token);
        }

        template <typename TToken>
        TString Identifier(const TToken& token) {
            return IdContent(Ctx, Token(token));
        }

        TString Identifier(const TString& str) const {
            return IdContent(Ctx, str);
        }

        TNodePtr GetNamedNode(const TString& name);
        void PushNamedNode(const TString& name, TNodePtr node);
        void PopNamedNode(const TString& name);

        template <typename TNode>
        void AltNotImplemented(const TString& ruleName, const TNode& node) {
            AltNotImplemented(ruleName, node.Alt_case(), node, TNode::descriptor());
        }

        template <typename TNode>
        TString AltDescription(const TNode& node) const {
            return AltDescription(node, node.Alt_case(), TNode::descriptor());
        }

    protected:
        void AltNotImplemented(const TString& ruleName, ui32 altCase, const google::protobuf::Message& node, const google::protobuf::Descriptor* descr);
        TString AltDescription(const google::protobuf::Message& node, ui32 altCase, const google::protobuf::Descriptor* descr) const;

    protected:
        TContext& Ctx;
    };
}  // namespace NSQLTranslationV0
