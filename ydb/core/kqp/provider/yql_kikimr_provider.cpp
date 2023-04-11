#include "yql_kikimr_provider_impl.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/core/base/path.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NYql {

using namespace NNodes;

namespace {

const TStringBuf CommitModeFlush = "flush";
const TStringBuf CommitModeRollback = "rollback";
const TStringBuf CommitModeScheme = "scheme";

struct TKikimrData {
    THashSet<TStringBuf> DataSourceNames;
    THashSet<TStringBuf> DataSinkNames;

    THashSet<TStringBuf> CommitModes;
    THashSet<TStringBuf> SupportedEffects;

    TYdbOperations SchemeOps;
    TYdbOperations DataOps;
    TYdbOperations ModifyOps;
    TYdbOperations ReadOps;

    TMap<TString, NKikimr::NUdf::EDataSlot> SystemColumns;

    TKikimrData() {
        DataSourceNames.insert(TKiReadTable::CallableName());
        DataSourceNames.insert(TKiReadTableScheme::CallableName());
        DataSourceNames.insert(TKiReadTableList::CallableName());

        DataSinkNames.insert(TKiWriteTable::CallableName());
        DataSinkNames.insert(TKiUpdateTable::CallableName());
        DataSinkNames.insert(TKiDeleteTable::CallableName());
        DataSinkNames.insert(TKiCreateTable::CallableName());
        DataSinkNames.insert(TKiAlterTable::CallableName());
        DataSinkNames.insert(TKiDropTable::CallableName());
        DataSinkNames.insert(TKiCreateTopic::CallableName());
        DataSinkNames.insert(TKiAlterTopic::CallableName());
        DataSinkNames.insert(TKiDropTopic::CallableName());
        DataSinkNames.insert(TKiCreateUser::CallableName());
        DataSinkNames.insert(TKiAlterUser::CallableName());
        DataSinkNames.insert(TKiDropUser::CallableName());
        DataSinkNames.insert(TKiCreateObject::CallableName());
        DataSinkNames.insert(TKiAlterObject::CallableName());
        DataSinkNames.insert(TKiDropObject::CallableName());
        DataSinkNames.insert(TKiCreateGroup::CallableName());
        DataSinkNames.insert(TKiAlterGroup::CallableName());
        DataSinkNames.insert(TKiDropGroup::CallableName());
        DataSinkNames.insert(TKiDataQueryBlock::CallableName());
        DataSinkNames.insert(TKiDataQueryBlocks::CallableName());
        DataSinkNames.insert(TKiExecDataQuery::CallableName());
        DataSinkNames.insert(TKiEffects::CallableName());

        CommitModes.insert(CommitModeFlush);
        CommitModes.insert(CommitModeRollback);
        CommitModes.insert(CommitModeScheme);

        SupportedEffects.insert(TKiWriteTable::CallableName());
        SupportedEffects.insert(TKiUpdateTable::CallableName());
        SupportedEffects.insert(TKiDeleteTable::CallableName());

        ModifyOps =
            TYdbOperation::Upsert |
            TYdbOperation::Replace |
            TYdbOperation::Update |
            TYdbOperation::UpdateOn |
            TYdbOperation::Delete |
            TYdbOperation::DeleteOn |
            TYdbOperation::InsertRevert |
            TYdbOperation::InsertAbort;

        ReadOps =
            TYdbOperation::Select |
            TYdbOperation::Update |
            TYdbOperation::Delete |
            TYdbOperation::InsertRevert |
            TYdbOperation::InsertAbort |
            TYdbOperation::UpdateOn; // TODO: KIKIMR-3206

        DataOps = ModifyOps | ReadOps;

        SchemeOps =
            TYdbOperation::CreateTable |
            TYdbOperation::DropTable |
            TYdbOperation::AlterTable |
            TYdbOperation::CreateTopic |
            TYdbOperation::AlterTopic |
            TYdbOperation::DropTopic |
            TYdbOperation::CreateUser |
            TYdbOperation::AlterUser |
            TYdbOperation::DropUser |
            TYdbOperation::CreateGroup |
            TYdbOperation::AlterGroup |
            TYdbOperation::DropGroup;

        SystemColumns = {
            {"_yql_partition_id", NKikimr::NUdf::EDataSlot::Uint64}
        };
    }
};

} // namespace

const TKikimrTableDescription* TKikimrTablesData::EnsureTableExists(const TString& cluster,
    const TString& table, TPositionHandle pos, TExprContext& ctx) const
{
    auto desc = Tables.FindPtr(std::make_pair(cluster, table));
    if (desc && (desc->GetTableType() != ETableType::Table || desc->DoesExist())) {
        return desc;
    }

    ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder()
        << "Cannot find table '" << NCommon::FullTableName(cluster, table)
        << "' because it does not exist or you do not have access permissions."
        << " Please check correctness of table path and user permissions."));
    return nullptr;
}

TKikimrTableDescription& TKikimrTablesData::GetOrAddTable(const TString& cluster, const TString& database, const TString& table, ETableType tableType) {
    if (!Tables.FindPtr(std::make_pair(cluster, table))) {
        auto& desc = Tables[std::make_pair(cluster, table)];

        TString error;
        std::pair<TString, TString> pathPair;
        if (NKikimr::TrySplitPathByDb(table, database, pathPair, error)) {
            desc.RelativePath = pathPair.second;
        }
        desc.SetTableType(tableType);

        return desc;
    }

    return Tables[std::make_pair(cluster, table)];
}

TKikimrTableDescription& TKikimrTablesData::GetTable(const TString& cluster, const TString& table) {
    auto desc = Tables.FindPtr(std::make_pair(cluster, table));
    YQL_ENSURE(desc, "Unexpected empty metadata, cluster '" << cluster << "', table '" << table << "'");

    return *desc;
}

const TKikimrTableDescription& TKikimrTablesData::ExistingTable(const TStringBuf& cluster,
    const TStringBuf& table) const
{
    auto desc = Tables.FindPtr(std::make_pair(TString(cluster), TString(table)));
    YQL_ENSURE(desc);
    YQL_ENSURE(desc->DoesExist());

    return *desc;
}

bool TKikimrTableDescription::Load(TExprContext& ctx, bool withSystemColumns) {
    ColumnTypes.clear();

    TVector<const TItemExprType*> items;
    for (auto pair : Metadata->Columns) {
        auto& column = pair.second;

        // Currently Kikimr doesn't have parametrized types and Decimal type
        // is passed with no params. It's known to always be Decimal(22,9),
        // so we transform Decimal type here.
        const TTypeAnnotationNode *type;
        if (to_lower(column.Type) == "decimal") {
            type = ctx.MakeType<TDataExprParamsType>(
                NKikimr::NUdf::GetDataSlot(column.Type),
                ToString(NKikimr::NScheme::DECIMAL_PRECISION),
                ToString(NKikimr::NScheme::DECIMAL_SCALE));
        } else {
            if (column.TypeInfo.GetTypeId() != NKikimr::NScheme::NTypeIds::Pg) {
                type = ctx.MakeType<TDataExprType>(NKikimr::NUdf::GetDataSlot(column.Type));
            } else {
                type = ctx.MakeType<TPgExprType>(NKikimr::NPg::PgTypeIdFromTypeDesc(column.TypeInfo.GetTypeDesc()));
            }
        }

        if (!column.NotNull && column.TypeInfo.GetTypeId() != NKikimr::NScheme::NTypeIds::Pg) {
            type = ctx.MakeType<TOptionalExprType>(type);
        }

        items.push_back(ctx.MakeType<TItemExprType>(column.Name, type));

        auto insertResult = ColumnTypes.insert(std::make_pair(column.Name, type));
        YQL_ENSURE(insertResult.second);
    }

    if (withSystemColumns) {
        for (const auto& [name, type] : KikimrSystemColumns()) {
            const TOptionalExprType* optType = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(type));
            items.push_back(ctx.MakeType<TItemExprType>(name, optType));

            auto insertResult = ColumnTypes.insert(std::make_pair(name, optType));
            YQL_ENSURE(insertResult.second);
        }
    }

    SchemeNode = ctx.MakeType<TStructExprType>(items);
    return true;
}

TMaybe<ui32> TKikimrTableDescription::GetKeyColumnIndex(const TString& name) const {
    auto it = std::find(Metadata->KeyColumnNames.begin(), Metadata->KeyColumnNames.end(), name);
    return it == Metadata->KeyColumnNames.end()
        ? TMaybe<ui32>()
        : it - Metadata->KeyColumnNames.begin();
}

const TTypeAnnotationNode* TKikimrTableDescription::GetColumnType(const TString& name) const {
    auto* type = ColumnTypes.FindPtr(name);
    if (!type) {
        return nullptr;
    }

    return *type;
}

bool TKikimrTableDescription::DoesExist() const {
    return Metadata->DoesExist;
}

void TKikimrTableDescription::ToYson(NYson::TYsonWriter& writer) const {
    YQL_ENSURE(Metadata);

    auto& meta = *Metadata;

    writer.OnBeginMap();
    writer.OnKeyedItem(TStringBuf("Cluster"));
    writer.OnStringScalar(meta.Cluster);
    writer.OnKeyedItem(TStringBuf("Name"));
    writer.OnStringScalar(meta.Name);
    writer.OnKeyedItem(TStringBuf("Id"));
    writer.OnStringScalar(meta.PathId.ToString());

    writer.OnKeyedItem(TStringBuf("DoesExist"));
    writer.OnBooleanScalar(DoesExist());
    writer.OnKeyedItem(TStringBuf("IsSorted"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("IsDynamic"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("UniqueKeys"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("CanWrite"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("IsRealData"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("YqlCompatibleSchema"));
    writer.OnBooleanScalar(true);

    writer.OnKeyedItem(TStringBuf("RecordsCount"));
    writer.OnInt64Scalar(meta.RecordsCount);
    writer.OnKeyedItem(TStringBuf("DataSize"));
    writer.OnInt64Scalar(meta.DataSize);
    writer.OnKeyedItem(TStringBuf("MemorySize"));
    writer.OnInt64Scalar(meta.MemorySize);
    writer.OnKeyedItem(TStringBuf("ChunkCount"));
    writer.OnInt64Scalar(meta.ShardsCount);

    writer.OnKeyedItem(TStringBuf("AccessTime"));
    writer.OnInt64Scalar(meta.LastAccessTime.Seconds());
    writer.OnKeyedItem(TStringBuf("ModifyTime"));
    writer.OnInt64Scalar(meta.LastUpdateTime.Seconds());

    writer.OnKeyedItem("Fields");
    writer.OnBeginList();
    {
        for (auto& item: SchemeNode->GetItems()) {
            writer.OnListItem();

            auto name = item->GetName();
            writer.OnBeginMap();

            writer.OnKeyedItem("Name");
            writer.OnStringScalar(name);

            writer.OnKeyedItem("Type");
            NCommon::WriteTypeToYson(writer, item->GetItemType());

            TMaybe<ui32> keyIndex = GetKeyColumnIndex(TString(name));

            writer.OnKeyedItem("ClusterSortOrder");
            writer.OnBeginList();
            if (keyIndex.Defined()) {
                writer.OnListItem();
                writer.OnInt64Scalar(*keyIndex);
            }
            writer.OnEndList();

            writer.OnKeyedItem("Ascending");
            writer.OnBeginList();
            if (keyIndex.Defined()) {
                writer.OnListItem();
                writer.OnBooleanScalar(true);
            }
            writer.OnEndList();

            writer.OnEndMap();
        }
    }
    writer.OnEndList();

    writer.OnKeyedItem("RowType");
    NCommon::WriteTypeToYson(writer, SchemeNode);

    writer.OnEndMap();
}

bool TKikimrKey::Extract(const TExprNode& key) {
    if (key.IsCallable("MrTableConcat")) {
        Ctx.AddError(TIssue(Ctx.GetPosition(key.Pos()), "CONCAT is not supported on Kikimr clusters."));
        return false;
    }

    if (!key.IsCallable("Key")) {
        Ctx.AddError(TIssue(Ctx.GetPosition(key.Pos()), "Expected key"));
        return false;
    }

    const auto& tagName = key.Child(0)->Child(0)->Content();
    if (tagName == "table") {
        KeyType = Type::Table;
        const TExprNode* nameNode = key.Child(0)->Child(1);

        if (nameNode->IsCallable("MrTableRange") || nameNode->IsCallable("MrTableRangeStrict")) {
            Ctx.AddError(TIssue(Ctx.GetPosition(key.Pos()), "RANGE is not supported on Kikimr clusters."));
            return false;
        }

        if (!nameNode->IsCallable("String")) {
            Ctx.AddError(TIssue(Ctx.GetPosition(key.Pos()), "Expected String as table key."));
            return false;
        }

        Target = nameNode->Child(0)->Content();
    } else if (tagName == "tablescheme") {
        KeyType = Type::TableScheme;
        Target = key.Child(0)->Child(1)->Child(0)->Content();
    } else if (tagName == "tablelist") {
        KeyType = Type::TableList;
        Target = key.Child(0)->Child(1)->Child(0)->Content();
    } else if (tagName == "role") {
        KeyType = Type::Role;
        Target = key.Child(0)->Child(1)->Child(0)->Content();
    } else if (tagName == "objectId") {
        KeyType = Type::Object;
        Target = key.Child(0)->Child(1)->Child(0)->Content();
        ObjectType = key.Child(1)->Child(1)->Child(0)->Content();
    } else if (tagName == "topic") {
        KeyType = Type::Topic;
        const TExprNode* nameNode = key.Child(0)->Child(1);
        if (!nameNode->IsCallable("String")) {
            Ctx.AddError(TIssue(Ctx.GetPosition(key.Pos()), "Expected String as topic key."));
            return false;
        }
        Target = nameNode->Child(0)->Content();
    } else {
        Ctx.AddError(TIssue(Ctx.GetPosition(key.Child(0)->Pos()), TString("Unexpected tag for kikimr key: ") + tagName));
        return false;
    }

    if (key.ChildrenSize() > 1 && KeyType != Type::Object) {
        for (ui32 i = 1; i < key.ChildrenSize(); ++i) {
            auto tag = key.Child(i)->Child(0);
            if (tag->Content() == TStringBuf("view")) {
                const TExprNode* viewNode = key.Child(i)->Child(1);
                if (!viewNode->IsCallable("String")) {
                    Ctx.AddError(TIssue(Ctx.GetPosition(viewNode->Pos()), "Expected String"));
                    return false;
                }

                if (viewNode->ChildrenSize() != 1 || !EnsureAtom(*viewNode->Child(0), Ctx)) {
                    Ctx.AddError(TIssue(Ctx.GetPosition(viewNode->Child(0)->Pos()), "Dynamic views names are not supported"));
                    return false;
                }
                if (viewNode->Child(0)->Content().empty()) {
                    Ctx.AddError(TIssue(Ctx.GetPosition(viewNode->Child(0)->Pos()), "Secondary index name must not be empty"));
                    return false;
                }
                View = viewNode->Child(0)->Content();

            } else {
                Ctx.AddError(TIssue(Ctx.GetPosition(tag->Pos()), TStringBuilder() << "Unexpected tag for kikimr key child: " << tag->Content()));
                return false;
            }
        }
    }
    return true;
}

TCoAtomList BuildColumnsList(const TKikimrTableDescription& table, TPositionHandle pos,
    TExprContext& ctx, bool withSystemColumns)
{
    TVector<TExprBase> columnsToSelect;
    for (const auto& pair : table.Metadata->Columns) {
        auto atom = Build<TCoAtom>(ctx, pos)
            .Value(pair.second.Name)
            .Done();

        columnsToSelect.emplace_back(std::move(atom));
    }

    if (withSystemColumns) {
        for (const auto& pair : KikimrSystemColumns()) {
            auto atom = Build<TCoAtom>(ctx, pos)
                .Value(pair.first)
                .Done();

            columnsToSelect.emplace_back(std::move(atom));
        }
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(columnsToSelect)
        .Done();
}

TVector<NKqpProto::TKqpTableOp> TableOperationsToProto(const TCoNameValueTupleList& operations, TExprContext& ctx) {
    TVector<NKqpProto::TKqpTableOp> protoOps;
    for (const auto& op : operations) {
        auto table = TString(op.Name());
        auto tableOp = FromString<TYdbOperation>(TString(op.Value().Cast<TCoAtom>()));
        auto pos = ctx.GetPosition(op.Pos());

        NKqpProto::TKqpTableOp protoOp;
        protoOp.MutablePosition()->SetRow(pos.Row);
        protoOp.MutablePosition()->SetColumn(pos.Column);
        protoOp.SetTable(table);
        protoOp.SetOperation((ui32)tableOp);

        protoOps.push_back(protoOp);
    }

    return protoOps;
}

TVector<NKqpProto::TKqpTableOp> TableOperationsToProto(const TKiOperationList& operations, TExprContext& ctx) {
    TVector<NKqpProto::TKqpTableOp> protoOps;
    for (const auto& op : operations) {
        auto table = TString(op.Table());
        auto tableOp = FromString<TYdbOperation>(TString(op.Operation()));
        auto pos = ctx.GetPosition(op.Pos());

        NKqpProto::TKqpTableOp protoOp;
        protoOp.MutablePosition()->SetRow(pos.Row);
        protoOp.MutablePosition()->SetColumn(pos.Column);
        protoOp.SetTable(table);
        protoOp.SetOperation((ui32)tableOp);

        protoOps.push_back(protoOp);
    }

    return protoOps;
}

template<class OutputIterator>
void TableDescriptionToTableInfoImpl(const TKikimrTableDescription& desc, TYdbOperation op, OutputIterator back_inserter)
{
    YQL_ENSURE(desc.Metadata);

    auto info = NKqpProto::TKqpTableInfo();
    info.SetTableName(desc.Metadata->Name);
    info.MutableTableId()->SetOwnerId(desc.Metadata->PathId.OwnerId());
    info.MutableTableId()->SetTableId(desc.Metadata->PathId.TableId());
    info.SetSchemaVersion(desc.Metadata->SchemaVersion);

    if (desc.Metadata->Indexes) {
        info.SetHasIndexTables(true);
    }

    back_inserter = std::move(info);
    ++back_inserter;

    if (KikimrModifyOps() & op) {
        for (ui32 idxNo = 0; idxNo < desc.Metadata->Indexes.size(); ++idxNo) {
            const auto& idxDesc = desc.Metadata->Indexes[idxNo];
            if (!idxDesc.ItUsedForWrite()) {
                continue;
            }

            const auto& idxTableDesc = desc.Metadata->SecondaryGlobalIndexMetadata[idxNo];

            auto info = NKqpProto::TKqpTableInfo();
            info.SetTableName(idxTableDesc->Name);
            info.MutableTableId()->SetOwnerId(idxTableDesc->PathId.OwnerId());
            info.MutableTableId()->SetTableId(idxTableDesc->PathId.TableId());
            info.SetSchemaVersion(idxTableDesc->SchemaVersion);

            back_inserter = std::move(info);
            ++back_inserter;
        }
    }
}

void TableDescriptionToTableInfo(const TKikimrTableDescription& desc, TYdbOperation op, NProtoBuf::RepeatedPtrField<NKqpProto::TKqpTableInfo>& infos) {
    TableDescriptionToTableInfoImpl(desc, op, google::protobuf::internal::RepeatedPtrFieldBackInsertIterator<NKqpProto::TKqpTableInfo>(&infos));
}

void TableDescriptionToTableInfo(const TKikimrTableDescription& desc, TYdbOperation op, TVector<NKqpProto::TKqpTableInfo>& infos) {
    TableDescriptionToTableInfoImpl(desc, op, std::back_inserter(infos));
}

const THashSet<TStringBuf>& KikimrDataSourceFunctions() {
    return Singleton<TKikimrData>()->DataSourceNames;
}

const THashSet<TStringBuf>& KikimrDataSinkFunctions() {
    return Singleton<TKikimrData>()->DataSinkNames;
}

const THashSet<TStringBuf>& KikimrSupportedEffects() {
    return Singleton<TKikimrData>()->SupportedEffects;
}

const THashSet<TStringBuf>& KikimrCommitModes() {
    return Singleton<TKikimrData>()->CommitModes;
}

const TStringBuf& KikimrCommitModeFlush() {
    return CommitModeFlush;
}

const TStringBuf& KikimrCommitModeRollback() {
    return CommitModeRollback;
}

const TStringBuf& KikimrCommitModeScheme() {
    return CommitModeScheme;
}

const TYdbOperations& KikimrSchemeOps() {
    return Singleton<TKikimrData>()->SchemeOps;
}

const TYdbOperations& KikimrDataOps() {
    return Singleton<TKikimrData>()->DataOps;
}

const TYdbOperations& KikimrModifyOps() {
    return Singleton<TKikimrData>()->ModifyOps;
}

const TYdbOperations& KikimrReadOps() {
    return Singleton<TKikimrData>()->ReadOps;
}

const TMap<TString, NKikimr::NUdf::EDataSlot>& KikimrSystemColumns() {
    return Singleton<TKikimrData>()->SystemColumns;
}

bool IsKikimrSystemColumn(const TStringBuf columnName) {
    return KikimrSystemColumns().FindPtr(columnName);
}

bool ValidateTableHasIndex(TKikimrTableMetadataPtr metadata, TExprContext& ctx, const TPositionHandle& pos) {
    if (metadata->Indexes.empty()) {
        ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder()
                                  << "No global indexes for table " << metadata->Name));
        return false;
    }
    return true;
}

bool AddDmlIssue(const TIssue& issue, TExprContext& ctx) {
    auto newIssue = AddDmlIssue(issue);
    ctx.AddError(newIssue);
    return false;
}

TIssue AddDmlIssue(const TIssue& issue) {
    TIssue newIssue;
    newIssue.SetCode(issue.GetCode(), ESeverity::TSeverityIds_ESeverityId_S_ERROR);
    newIssue.SetMessage("Detected violation of logical DML constraints. YDB transactions don't see their own"
        " changes, make sure you perform all table reads before any modifications.");
    newIssue.AddSubIssue(new TIssue(issue));
    return newIssue;
}

TKiDataQueryBlockSettings TKiDataQueryBlockSettings::Parse(const NNodes::TKiDataQueryBlock& node) {
    TKiDataQueryBlockSettings settings;

    for (const auto& tuple : node.Settings()) {
        auto name = tuple.Name().Value();
        if (name == HasUncommittedChangesReadSettingName) {
            settings.HasUncommittedChangesRead = true;
        }
    }

    return settings;
}

NNodes::TCoNameValueTupleList TKiDataQueryBlockSettings::BuildNode(TExprContext& ctx, TPositionHandle pos) const {
    TVector<TCoNameValueTuple> settings;

    if (HasUncommittedChangesRead) {
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(HasUncommittedChangesReadSettingName)
            .Done());
    }

    return Build<TCoNameValueTupleList>(ctx, pos)
        .Add(settings)
        .Done();
}

TKiExecDataQuerySettings TKiExecDataQuerySettings::Parse(TKiExecDataQuery exec) {
    TKiExecDataQuerySettings settings;

    for (const auto& setting : exec.Settings()) {
        if (setting.Name() == "mode") {
            YQL_ENSURE(setting.Value().Maybe<TCoAtom>());
            settings.Mode = setting.Value().Cast<TCoAtom>();
        } else {
            settings.Other.push_back(setting);
        }
    }

    return settings;
}

TCoNameValueTupleList TKiExecDataQuerySettings::BuildNode(TExprContext& ctx, TPositionHandle pos) const {
    TVector<TCoNameValueTuple> settings(Other);

    if (Mode) {
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build("mode")
            .Value<TCoAtom>().Build(*Mode)
            .Done());
    }

    return Build<TCoNameValueTupleList>(ctx, pos)
        .Add(settings)
        .Done();
}

} // namespace NYql
