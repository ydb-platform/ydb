#pragma once

#include "session.h"

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <yt/yql/providers/yt/provider/yql_yt_table.h>

namespace NYql {

class TYtGatewayConfig;
using TYtGatewayConfigPtr = std::shared_ptr<TYtGatewayConfig>;

struct TYtBaseServices: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtBaseServices>;

    virtual ~TYtBaseServices() = default;

    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    TYtGatewayConfigPtr Config;
};

struct TInputInfo {
    TInputInfo() = default;
    TInputInfo(const TString& name, const NYT::TRichYPath& path, bool temp, bool strict, const TYtTableBaseInfo& info, const NYT::TNode& spec, ui32 group = 0);

    TString Name;
    NYT::TRichYPath Path;
    TString Cluster;
    bool Temp = false;
    bool Dynamic = false;
    bool Strict = true;
    ui64 Records = 0;
    ui64 DataSize = 0;
    NYT::TNode Spec;
    NYT::TNode QB2Premapper;
    ui32 Group = 0;
    bool Lookup = false;
    TString ErasureCodec;
    TString CompressionCode;
    TString PrimaryMedium;
    NYT::TNode Media;
};

struct TOutputInfo {
    TOutputInfo() = default;
    TOutputInfo(const TString& name, const TString& path, const NYT::TNode& codecSpec, const NYT::TNode& attrSpec,
        const NYT::TSortColumns& sortedBy, NYT::TNode columnGroups)
        : Name(name)
        , Path(path)
        , Spec(codecSpec)
        , AttrSpec(attrSpec)
        , SortedBy(sortedBy)
        , ColumnGroups(std::move(columnGroups))
    {
    }
    TString Name;
    TString Path;
    NYT::TNode Spec;
    NYT::TNode AttrSpec;
    NYT::TSortColumns SortedBy;
    NYT::TNode ColumnGroups;
};

struct TExecContextBaseSimple: public TThrRefBase {
protected:
    TExecContextBaseSimple(
        const TYtBaseServices::TPtr& services,
        const TConfigClusters::TPtr& clusters,
        TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler> mkqlCompiler,
        const TString& cluster,
        const TSessionBase::TPtr& session
    );

public:
    TString GetInputSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput) const;
    TString GetOutSpec(bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const;
    TString GetOutSpec(size_t beginIdx, size_t endIdx, NYT::TNode initialOutSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags) const;

    TString GetSessionId() const;

    virtual ~TExecContextBaseSimple() = default;

protected:

    void SetInput(NNodes::TExprBase input, bool forcePathColumns, const THashSet<TString>& extraSysColumns, const TYtSettings::TConstPtr& settings);

    void SetOutput(NNodes::TYtOutSection output, const TYtSettings::TConstPtr& settings, const TString& opHash);

    virtual void SetCache(const TVector<TString>& outTablePaths, const TVector<NYT::TNode>& outTableSpecs,
        const TString& tmpFolder, const TYtSettings::TConstPtr& settings, const TString& opHash);

    void SetSingleOutput(const TYtOutTableInfo& outTable, const TYtSettings::TConstPtr& settings);

    template <class TTableType>
    static TString GetSpecImpl(const TVector<TTableType>& tables, size_t beginIdx, size_t endIdx, NYT::TNode initialOutSpec, bool ensureOldTypesOnly, ui64 nativeTypeCompatibilityFlags, bool intermediateInput);

    virtual void FillRichPathForPullCaseInput(NYT::TRichYPath& path, TYtTableBaseInfo::TPtr tableInfo);
    virtual void FillRichPathForInput(NYT::TRichYPath& path, const TYtPathInfo& pathInfo, const TString& newPath, bool localChainTest);
    virtual bool IsLocalChainTest() const;

public:
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_ = nullptr;
    TYtGatewayConfigPtr Config_;
    TConfigClusters::TPtr Clusters_;
    TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler> MkqlCompiler_;

    TString Cluster_;
    TVector<TInputInfo> InputTables_;
    TVector<TOutputInfo> OutTables_;
    bool YamrInput = false;
    TMaybe<TSampleParams> Sampling;
    const TSessionBase::TPtr BaseSession_;
};


template <class T>
class TExecContextSimple: public TExecContextBaseSimple {
public:
    using TPtr = ::TIntrusivePtr<TExecContextSimple>;
    using TOptions = T;

    TExecContextSimple(
        const TYtBaseServices::TPtr services,
        const TConfigClusters::TPtr& clusters,
        const TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler>& mkqlCompiler,
        TOptions&& options,
        const TString& cluster,
        TSessionBase::TPtr session
    )
        : TExecContextBaseSimple(services, clusters, mkqlCompiler, cluster, session)
        , Options_(options)
    {
    }

    void SetInput(NNodes::TExprBase input, bool forcePathColumns, const THashSet<TString>& extraSysColumns) {
        TExecContextBaseSimple::SetInput(input, forcePathColumns, extraSysColumns, Options_.Config());
    }

    virtual void SetOutput(NNodes::TYtOutSection output) {
        TExecContextBaseSimple::SetOutput(output, Options_.Config(), Options_.OperationHash());
    }

    void SetSingleOutput(const TYtOutTableInfo& outTable) {
        TExecContextBaseSimple::SetSingleOutput(outTable, Options_.Config());
    }

    TOptions Options_;
};

} // namespace NYql
