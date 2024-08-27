#pragma once

#include "yql_yt_table.h"
#include "yql_yt_table_desc.h"

#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <ydb/library/yql/providers/stat/uploader/yql_stat_uploader.h>

#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/file_storage/storage.h>

#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/public.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/typetraits.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <utility>

namespace NYql {

class TYtClusterConfig;

namespace NCommon {
    class TMkqlCallableCompilerBase;
}

class IYtGateway : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IYtGateway>;

#define OPTION_FIELD_METHODS(type, name)                        \
    public:                                                     \
    TSelf& name(TTypeTraits<type>::TFuncParam arg##name)& {     \
        name##_ = arg##name;                                    \
        return *this;                                           \
    }                                                           \
    TSelf&& name(TTypeTraits<type>::TFuncParam arg##name)&& {   \
        name##_ = arg##name;                                    \
        return std::move(*this);                                \
    }                                                           \
    TTypeTraits<type>::TFuncParam name() const {                \
        return name##_;                                         \
    }                                                           \
    type& name() {                                              \
        return name##_;                                         \
    }

#define OPTION_FIELD(type, name)                                \
    private:                                                    \
    type name##_;                                               \
    OPTION_FIELD_METHODS(type, name)

#define OPTION_FIELD_DEFAULT(type, name, def)                   \
    private:                                                    \
    type name##_ = def;                                         \
    OPTION_FIELD_METHODS(type, name)


    //////////////////////////////////////////////////////////////

    using TSecureParams = THashMap<TString, TString>;

    //////////////////////////////////////////////////////////////

    struct TCommonOptions {
        TString SessionId_;

        TCommonOptions(const TString& sessionId)
            : SessionId_(sessionId)
        {
        }

        const TString& SessionId() const {
            return SessionId_;
        }
    };

    //////////////////////////////////////////////////////////////

    struct TOpenSessionOptions : public TCommonOptions {
        using TSelf = TOpenSessionOptions;

        TOpenSessionOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, UserName)
        OPTION_FIELD(TOperationProgressWriter, ProgressWriter)
        OPTION_FIELD(TYqlOperationOptions, OperationOptions)
        OPTION_FIELD(TIntrusivePtr<IRandomProvider>, RandomProvider)
        OPTION_FIELD(TIntrusivePtr<ITimeProvider>, TimeProvider)
        OPTION_FIELD(TStatWriter, StatWriter)
        OPTION_FIELD_DEFAULT(bool, CreateOperationTracker, true)
    };

    //////////////////////////////////////////////////////////////

    struct TCloseSessionOptions : public TCommonOptions {
        using TSelf = TCloseSessionOptions;

        TCloseSessionOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }
    };

    //////////////////////////////////////////////////////////////

    struct TCleanupSessionOptions : public TCommonOptions {
        using TSelf = TCleanupSessionOptions;

        TCleanupSessionOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }
    };

    //////////////////////////////////////////////////////////////

    struct TFinalizeOptions : public TCommonOptions {
        using TSelf = TFinalizeOptions;

        TFinalizeOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD_DEFAULT(bool, Abort, false)
        OPTION_FIELD_DEFAULT(bool, DetachSnapshotTxs, false)
    };

    struct TFinalizeResult : public NCommon::TOperationResult {
    };

    //////////////////////////////////////////////////////////////

    struct TCanonizeReq {
        using TSelf = TCanonizeReq;

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TString, Path)
        OPTION_FIELD(TPosition, Pos)
    };

    struct TCanonizePathsOptions : public TCommonOptions {
        using TSelf = TCanonizePathsOptions;

        TCanonizePathsOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TVector<TCanonizeReq>, Paths)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
    };

    struct TCanonizedPath {
        TString Path;
        TMaybe<TVector<TString>> Columns;
        TMaybe<TVector<NYT::TReadRange>> Ranges;
        TMaybe<TString> AdditionalAttributes;
    };

    struct TCanonizePathsResult: public NCommon::TOperationResult {
        TVector<TCanonizedPath> Data;
    };

    //////////////////////////////////////////////////////////////

    struct TTableReq {
        using TSelf = TTableReq;

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TString, Table)
        OPTION_FIELD_DEFAULT(bool, LockOnly, false)
        OPTION_FIELD(TYtTableIntents, Intents)
        OPTION_FIELD_DEFAULT(ui32, InferSchemaRows, 0)
        OPTION_FIELD_DEFAULT(bool, ForceInferSchema, false)
        OPTION_FIELD_DEFAULT(bool, Anonymous, false)
        OPTION_FIELD_DEFAULT(bool, IgnoreYamrDsv, false)
        OPTION_FIELD_DEFAULT(bool, IgnoreWeakSchema, false)
    };

    struct TGetTableInfoOptions : public TCommonOptions {
        using TSelf = TGetTableInfoOptions;

        TGetTableInfoOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TVector<TTableReq>, Tables)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD_DEFAULT(bool, ReadOnly, false)
        OPTION_FIELD_DEFAULT(ui32, Epoch, 0)
    };

    struct TTableInfoResult: public NCommon::TOperationResult {
        struct TTableData {
            TYtTableMetaInfo::TPtr Meta;
            TYtTableStatInfo::TPtr Stat;
            bool WriteLock = false;
        };
        TVector<TTableData> Data;
    };

    struct TTableStatResult: public NCommon::TOperationResult {
        TYtTableStatInfo::TPtr Data;
    };

    //////////////////////////////////////////////////////////////

    struct TTableRangeOptions : public TCommonOptions {
        using TSelf = TTableRangeOptions;

        TTableRangeOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TString, Prefix)
        OPTION_FIELD_DEFAULT(const TExprNode*, Filter, nullptr)
        OPTION_FIELD_DEFAULT(TExprContext*, ExprCtx, nullptr)
        OPTION_FIELD(TString, Suffix)
        OPTION_FIELD(TUserDataTable, UserDataBlocks)
        OPTION_FIELD(TUdfModulesTable, UdfModules)
        OPTION_FIELD(IUdfResolver::TPtr, UdfResolver)
        OPTION_FIELD_DEFAULT(NKikimr::NUdf::EValidateMode, UdfValidateMode, NKikimr::NUdf::EValidateMode::None)
        OPTION_FIELD(TMaybe<ui32>, PublicId)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TString, OptLLVM)
        OPTION_FIELD(TString, OperationHash)
        OPTION_FIELD(TPosition, Pos)
        OPTION_FIELD(TSecureParams, SecureParams)
    };

    struct TTableRangeResult : public NCommon::TOperationResult {
        TVector<TCanonizedPath> Tables;
    };

    //////////////////////////////////////////////////////////////

    struct TFolderOptions : public TCommonOptions {
        using TSelf = TFolderOptions;

        TFolderOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TString, Prefix)
        OPTION_FIELD(TSet<TString>, Attributes)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TPosition, Pos)
    };

    struct TBatchFolderOptions : public TCommonOptions {
        using TSelf = TBatchFolderOptions;

        TBatchFolderOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        struct TFolderPrefixAttrs {
            TString Prefix;
            TSet<TString> AttrKeys;
        };

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TVector<TFolderPrefixAttrs>, Folders)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TPosition, Pos)
    };

    struct TBatchFolderResult : public NCommon::TOperationResult {
        struct TFolderItem {
            TString Path;
            TString Type;
            NYT::TNode Attributes;

        };
        TVector<TFolderItem> Items;
    };

    struct TSerializedFolderItem {
    };

    struct TFolderResult : public NCommon::TOperationResult {
        struct TFolderItem {
            TString Path;
            TString Type;
            TString Attributes;

            auto operator<=>(const TFolderItem&) const = default;
        };
        std::variant<TVector<TFolderItem>, TFileLinkPtr> ItemsOrFileLink;
    };

    //////////////////////////////////////////////////////////////

    struct TResolveOptions : public TCommonOptions {
        using TSelf = TResolveOptions;

        TResolveOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        struct TItemWithReqAttrs {
            TBatchFolderResult::TFolderItem Item;
            TSet<TString> AttrKeys;
        };

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TVector<TItemWithReqAttrs>, Items)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TPosition, Pos)
    };

    //////////////////////////////////////////////////////////////

    struct TResOrPullOptions : public TCommonOptions {
        using TSelf = TResOrPullOptions;

        TResOrPullOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TUserDataTable, UserDataBlocks)
        OPTION_FIELD(TUdfModulesTable, UdfModules)
        OPTION_FIELD(IUdfResolver::TPtr, UdfResolver)
        OPTION_FIELD_DEFAULT(NKikimr::NUdf::EValidateMode, UdfValidateMode, NKikimr::NUdf::EValidateMode::None)
        OPTION_FIELD(IDataProvider::TFillSettings, FillSettings)
        OPTION_FIELD(TMaybe<ui32>, PublicId)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TString, UsedCluster)
        OPTION_FIELD(TString, OptLLVM)
        OPTION_FIELD(TString, OperationHash)
        OPTION_FIELD(TSecureParams, SecureParams)
    };

    struct TResOrPullResult : public NCommon::TOperationResult {
        TString Data;
    };

    //////////////////////////////////////////////////////////////

    struct TRunOptions : public TCommonOptions {
        using TSelf = TRunOptions;

        TRunOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TUserDataTable, UserDataBlocks)
        OPTION_FIELD(TUdfModulesTable, UdfModules)
        OPTION_FIELD(IUdfResolver::TPtr, UdfResolver)
        OPTION_FIELD_DEFAULT(NKikimr::NUdf::EValidateMode, UdfValidateMode, NKikimr::NUdf::EValidateMode::None)
        OPTION_FIELD(TMaybe<ui32>, PublicId);
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TString, OptLLVM)
        OPTION_FIELD(TString, OperationHash)
        OPTION_FIELD(TSecureParams, SecureParams)
    };

    struct TRunResult : public NCommon::TOperationResult {
        // Return pair of table name, table stat for each output table
        TVector<std::pair<TString, TYtTableStatInfo::TPtr>> OutTableStats;
    };


    //////////////////////////////////////////////////////////////

    struct TPrepareOptions : public TCommonOptions {
        using TSelf = TPrepareOptions;

        TPrepareOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TMaybe<ui32>, PublicId)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TString, OperationHash)
        OPTION_FIELD_DEFAULT(THashSet<TString>, SecurityTags, {})
    };

    //////////////////////////////////////////////////////////////

    struct TCalcOptions : public TCommonOptions {
        using TSelf = TCalcOptions;

        TCalcOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TUserDataTable, UserDataBlocks)
        OPTION_FIELD(TUdfModulesTable, UdfModules)
        OPTION_FIELD(IUdfResolver::TPtr, UdfResolver)
        OPTION_FIELD_DEFAULT(NKikimr::NUdf::EValidateMode, UdfValidateMode, NKikimr::NUdf::EValidateMode::None)
        OPTION_FIELD(TMaybe<ui32>, PublicId);
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TString, OptLLVM)
        OPTION_FIELD(TString, OperationHash)
        OPTION_FIELD(TSecureParams, SecureParams)
    };

    struct TCalcResult : public NCommon::TOperationResult {
        TVector<NYT::TNode> Data;
    };

    //////////////////////////////////////////////////////////////

    struct TPublishOptions : public TCommonOptions {
        using TSelf = TPublishOptions;

        TPublishOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TMaybe<ui32>, PublicId)
        OPTION_FIELD(TYqlRowSpecInfo::TPtr, DestinationRowSpec)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TString, OptLLVM)
        OPTION_FIELD(TString, OperationHash)
    };

    struct TPublishResult : public NCommon::TOperationResult {
    };

    //////////////////////////////////////////////////////////////

    struct TCommitOptions : public TCommonOptions {
        using TSelf = TCommitOptions;

        TCommitOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, Cluster)
    };

    struct TCommitResult : public NCommon::TOperationResult {
    };

    //////////////////////////////////////////////////////////////

    struct TDropTrackablesOptions : public TCommonOptions {
        using TSelf = TDropTrackablesOptions;

        struct TClusterAndPath
        {
            TString Cluster;
            TString Path;
        };

        TDropTrackablesOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TVector<TClusterAndPath>, Pathes)
    };

    struct TDropTrackablesResult : public NCommon::TOperationResult {
    };

    //////////////////////////////////////////////////////////////

    struct TPathStatReq {
        using TSelf = TPathStatReq;

        OPTION_FIELD(NYT::TRichYPath, Path)
        OPTION_FIELD_DEFAULT(bool, IsTemp, false)
        OPTION_FIELD_DEFAULT(bool, IsAnonymous, false)
        OPTION_FIELD_DEFAULT(ui32, Epoch, 0)
    };

    struct TPathStatOptions : public TCommonOptions {
        using TSelf = TPathStatOptions;

        TPathStatOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TVector<TPathStatReq>, Paths)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
    };

    struct TPathStatResult: public NCommon::TOperationResult {
        TVector<ui64> DataSize;
    };

    struct TFullResultTableOptions : public TCommonOptions {
        using TSelf = TFullResultTableOptions;

        TFullResultTableOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
        OPTION_FIELD(TYtOutTableInfo, OutTable)
    };

    struct TFullResultTableResult: public NCommon::TOperationResult {
        TMaybe<TString> RootTransactionId;
        TMaybe<TString> ExternalTransactionId;
        TString Server;
        TString Path;
        TString RefName;
        TString CodecSpec;
        TString TableAttrs;
    };

    struct TGetTablePartitionsOptions : public TCommonOptions {
        using TSelf = TGetTablePartitionsOptions;

        TGetTablePartitionsOptions(const TString& sessionId)
            : TCommonOptions(sessionId)
        {
        }

        OPTION_FIELD(TString, Cluster)
        OPTION_FIELD(TVector<TYtPathInfo::TPtr>, Paths)
        OPTION_FIELD_DEFAULT(size_t, DataSizePerJob, 0)
        OPTION_FIELD_DEFAULT(size_t, MaxPartitions, 0)
        OPTION_FIELD_DEFAULT(bool, AdjustDataWeightPerPartition, true)
        OPTION_FIELD(TYtSettings::TConstPtr, Config)
    };

    struct TGetTablePartitionsResult: public NCommon::TOperationResult {
        NYT::TMultiTablePartitions Partitions;
    };

public:
    virtual ~IYtGateway() = default;

    virtual void OpenSession(TOpenSessionOptions&& options) = 0;

    virtual NThreading::TFuture<void> CloseSession(TCloseSessionOptions&& options) = 0;

    virtual NThreading::TFuture<void> CleanupSession(TCleanupSessionOptions&& options) = 0;

    virtual NThreading::TFuture<TFinalizeResult> Finalize(TFinalizeOptions&& options) = 0;

    virtual NThreading::TFuture<TCanonizePathsResult> CanonizePaths(TCanonizePathsOptions&& options) = 0;

    virtual NThreading::TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) = 0;

    virtual NThreading::TFuture<TTableRangeResult> GetTableRange(TTableRangeOptions&& options) = 0;

    virtual NThreading::TFuture<TFolderResult> GetFolder(TFolderOptions&& options) = 0;

    virtual NThreading::TFuture<TBatchFolderResult> ResolveLinks(TResolveOptions&& options) = 0;

    virtual NThreading::TFuture<TBatchFolderResult> GetFolders(TBatchFolderOptions&& options) = 0;

    virtual NThreading::TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) = 0;

    virtual NThreading::TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) = 0;

    virtual NThreading::TFuture<TRunResult> Prepare(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) const = 0;
    virtual NThreading::TFuture<TRunResult> GetTableStat(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) = 0 ;

    virtual NThreading::TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) = 0;

    virtual NThreading::TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) = 0;

    virtual NThreading::TFuture<TCommitResult> Commit(TCommitOptions&& options) = 0;

    virtual NThreading::TFuture<TDropTrackablesResult> DropTrackables(TDropTrackablesOptions&& options) = 0;

    virtual NThreading::TFuture<TPathStatResult> PathStat(TPathStatOptions&& options) = 0;
    virtual TPathStatResult TryPathStat(TPathStatOptions&& options) = 0;

    virtual bool TryParseYtUrl(const TString& url, TString* cluster, TString* path) const = 0;

    virtual TString GetDefaultClusterName() const = 0;
    virtual TString GetClusterServer(const TString& cluster) const = 0;
    virtual NYT::TRichYPath GetRealTable(const TString& sessionId, const TString& cluster, const TString& table, ui32 epoch, const TString& tmpFolder) const = 0;
    virtual NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const = 0;

    virtual TFullResultTableResult PrepareFullResultTable(TFullResultTableOptions&& options) = 0;

    virtual void SetStatUploader(IStatUploader::TPtr statUploader) = 0;

    virtual void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) = 0;

    virtual TGetTablePartitionsResult GetTablePartitions(TGetTablePartitionsOptions&& options) = 0;

    virtual void AddCluster(const TYtClusterConfig& cluster) = 0;
};

}