#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <library/cpp/yson/writer.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>

#include <utility>

namespace NYson {
    class TYsonWriter;
}

namespace NKikimr {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

namespace NYql {

struct TTypeAnnotationContext;
struct TOperationStatistics;

namespace NCommon {

constexpr TStringBuf PgCatalogFileName = "_yql_pg_catalog";

struct TWriteTableSettings {
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TCoAtom> Temporary;
    NNodes::TMaybeNode<NNodes::TExprList> Columns;
    NNodes::TMaybeNode<NNodes::TExprList> ReturningList;
    NNodes::TMaybeNode<NNodes::TCoAtomList> PrimaryKey;
    NNodes::TMaybeNode<NNodes::TCoAtomList> PartitionBy;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> OrderBy;
    NNodes::TMaybeNode<NNodes::TCoLambda> Filter;
    NNodes::TMaybeNode<NNodes::TCoLambda> Update;
    NNodes::TMaybeNode<NNodes::TCoIndexList> Indexes;
    NNodes::TMaybeNode<NNodes::TCoChangefeedList> Changefeeds;
    NNodes::TCoNameValueTupleList Other;
    NNodes::TMaybeNode<NNodes::TExprList> ColumnFamilies;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> ColumnsDefaultValues;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> TableSettings;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> AlterActions;
    NNodes::TMaybeNode<NNodes::TCoAtom> TableType;
    NNodes::TMaybeNode<NNodes::TCallable> PgFilter;

    TWriteTableSettings(const NNodes::TCoNameValueTupleList& other)
        : Other(other) {}
};

struct TWriteSequenceSettings {
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TCoAtom> ValueType;
    NNodes::TMaybeNode<NNodes::TCoAtom> Temporary;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> SequenceSettings;

    NNodes::TCoNameValueTupleList Other;

    TWriteSequenceSettings(const NNodes::TCoNameValueTupleList& other)
        : Other(other) {}
};

struct TWriteTopicSettings {
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> TopicSettings;
    NNodes::TMaybeNode<NNodes::TCoTopicConsumerList> Consumers;
    NNodes::TMaybeNode<NNodes::TCoTopicConsumerList> AddConsumers;
    NNodes::TMaybeNode<NNodes::TCoTopicConsumerList> AlterConsumers;
    NNodes::TMaybeNode<NNodes::TCoAtomList> DropConsumers;
    NNodes::TCoNameValueTupleList Other;

    TWriteTopicSettings(const NNodes::TCoNameValueTupleList& other)
        : Other(other)
    {}

};

struct TWriteReplicationSettings {
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TCoReplicationTargetList> Targets;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> ReplicationSettings;
    NNodes::TCoNameValueTupleList Other;

    TWriteReplicationSettings(const NNodes::TCoNameValueTupleList& other)
        : Other(other)
    {}
};

struct TWriteRoleSettings {
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TCoAtomList> Roles;
    NNodes::TMaybeNode<NNodes::TCoAtom> NewName;
    NNodes::TCoNameValueTupleList Other;

    TWriteRoleSettings(const NNodes::TCoNameValueTupleList& other)
        : Other(other) {}
};

struct TWritePermissionSettings {
    NNodes::TMaybeNode<NNodes::TCoAtomList> Permissions;
    NNodes::TMaybeNode<NNodes::TCoAtomList> Paths;
    NNodes::TMaybeNode<NNodes::TCoAtomList> RoleNames;

    TWritePermissionSettings(NNodes::TMaybeNode<NNodes::TCoAtomList>&& permissions, NNodes::TMaybeNode<NNodes::TCoAtomList>&& paths, NNodes::TMaybeNode<NNodes::TCoAtomList>&& roleNames)
        : Permissions(std::move(permissions))
        , Paths(std::move(paths))
        , RoleNames(std::move(roleNames)) {}
};

struct TWriteObjectSettings {
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TCoNameValueTupleList Features;
    NNodes::TCoAtomList ResetFeatures;
    TWriteObjectSettings(NNodes::TMaybeNode<NNodes::TCoAtom>&& mode, NNodes::TCoNameValueTupleList&& kvFeatures, NNodes::TCoAtomList&& resetFeatures)
        : Mode(std::move(mode))
        , Features(std::move(kvFeatures))
        , ResetFeatures(std::move(resetFeatures))
    {
    }
};

struct TCommitSettings
{
    TPositionHandle Pos;
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TCoAtom> Epoch;
    NNodes::TCoNameValueTupleList Other;

    TCommitSettings(NNodes::TCoNameValueTupleList other)
        : Other(other) {}

    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx) const;

    bool EnsureModeEmpty(TExprContext& ctx);
    bool EnsureEpochEmpty(TExprContext& ctx);
    bool EnsureOtherEmpty(TExprContext& ctx);
};

struct TPgObjectSettings
{
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TCoAtom> IfExists;

    TPgObjectSettings(NNodes::TMaybeNode<NNodes::TCoAtom>&& mode, NNodes::TMaybeNode<NNodes::TCoAtom>&& ifExists)
        : Mode(std::move(mode))
        , IfExists(std::move(ifExists)) {}
};

const TStructExprType* BuildCommonTableListType(TExprContext& ctx);

TExprNode::TPtr BuildTypeExpr(TPositionHandle pos, const TTypeAnnotationNode& ann, TExprContext& ctx);

bool HasResOrPullOption(const TExprNode& node, const TStringBuf& option);

TVector<TString> GetResOrPullColumnHints(const TExprNode& node);

TWriteTableSettings ParseWriteTableSettings(NNodes::TExprList node, TExprContext& ctx);
TWriteTopicSettings ParseWriteTopicSettings(NNodes::TExprList node, TExprContext& ctx);
TWriteReplicationSettings ParseWriteReplicationSettings(NNodes::TExprList node, TExprContext& ctx);

TWriteRoleSettings ParseWriteRoleSettings(NNodes::TExprList node, TExprContext& ctx);
TWriteObjectSettings ParseWriteObjectSettings(NNodes::TExprList node, TExprContext& ctx);

TWritePermissionSettings ParseWritePermissionsSettings(NNodes::TExprList node, TExprContext& ctx);

TCommitSettings ParseCommitSettings(NNodes::TCoCommit node, TExprContext& ctx);

TPgObjectSettings ParsePgObjectSettings(NNodes::TExprList node, TExprContext& ctx);

TWriteSequenceSettings ParseSequenceSettings(NNodes::TExprList node, TExprContext& ctx);

TString FullTableName(const TStringBuf& cluster, const TStringBuf& table);

IDataProvider::TFillSettings GetFillSettings(const TExprNode& node);
NYson::EYsonFormat GetYsonFormat(const IDataProvider::TFillSettings& fillSettings);

TVector<TString> GetStructFields(const TTypeAnnotationNode* type);

void TransformerStatsToYson(const TString& name, const IGraphTransformer::TStatistics& stats, NYson::TYsonWriter& writer);

TString TransformerStatsToYson(const IGraphTransformer::TStatistics& stats, NYson::EYsonFormat format
    = NYson::EYsonFormat::Pretty);

void FillSecureParams(const TExprNode::TPtr& node, const TTypeAnnotationContext& types, THashMap<TString, TString>& secureParams);

bool FillUsedFiles(const TExprNode& node, TUserDataTable& files, const TTypeAnnotationContext& types, TExprContext& ctx, const TUserDataTable& crutches = {});

std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> FreezeUsedFiles(const TExprNode& node, TUserDataTable& files, const TTypeAnnotationContext& types, TExprContext& ctx, const std::function<bool(const TString&)>& urlDownloadFilter, const TUserDataTable& crutches = {});

bool FreezeUsedFilesSync(const TExprNode& node, TUserDataTable& files, const TTypeAnnotationContext& types, TExprContext& ctx, const std::function<bool(const TString&)>& urlDownloadFilter);

void WriteColumns(NYson::TYsonWriter& writer, const NNodes::TExprBase& columns);

TString SerializeExpr(TExprContext& ctx, const TExprNode& expr, bool withTypes = false);
TString ExprToPrettyString(TExprContext& ctx, const TExprNode& expr);

void WriteStream(NYson::TYsonWriter& writer, const TExprNode* node, const TExprNode* source);
void WriteStreams(NYson::TYsonWriter& writer, TStringBuf name, const NNodes::TCoLambda& lambda);

double GetDataReplicationFactor(const TExprNode& lambda, TExprContext& ctx);

void WriteStatistics(NYson::TYsonWriter& writer, bool totalOnly, const THashMap<ui32, TOperationStatistics>& statistics, bool addTotalKey = true, bool addExternalMap = true);
void WriteStatistics(NYson::TYsonWriter& writer, const TOperationStatistics& statistics);

bool ValidateCompressionForInput(std::string_view format, std::string_view compression, TExprContext& ctx);
bool ValidateCompressionForOutput(std::string_view format, std::string_view compression, TExprContext& ctx);

bool ValidateFormatForInput(std::string_view format, const TStructExprType* schemaStructRowType, const std::function<bool(TStringBuf)>& excludeFields, TExprContext& ctx);
bool ValidateFormatForOutput(std::string_view format, TExprContext& ctx);

bool ValidateIntervalUnit(std::string_view unit, TExprContext& ctx);
bool ValidateDateTimeFormatName(std::string_view formatName, TExprContext& ctx);
bool ValidateTimestampFormatName(std::string_view formatName, TExprContext& ctx);

bool TransformPgSetItemOption(
    const NNodes::TCoPgSelect& pgSelect,
    TStringBuf optionName,
    std::function<void(const NNodes::TExprBase&)> lambda
);

TExprNode::TPtr GetSetItemOption(const NNodes::TCoPgSelect& pgSelect, TStringBuf optionName);

TExprNode::TPtr GetSetItemOptionValue(const NNodes::TExprBase& setItemOption);

bool NeedToRenamePgSelectColumns(const NNodes::TCoPgSelect& pgSelect);

bool RenamePgSelectColumns(
    const NNodes::TCoPgSelect& node,
    TExprNode::TPtr& output,
    const TMaybe<TColumnOrder>& tableColumnOrder,
    TExprContext& ctx,
    TTypeAnnotationContext& types);
} // namespace NCommon
} // namespace NYql
