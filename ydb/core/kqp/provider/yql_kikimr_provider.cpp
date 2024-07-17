#include "yql_kikimr_provider_impl.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/core/protos/pqconfig.pb.h>

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
        DataSinkNames.insert(TKiCreateReplication::CallableName());
        DataSinkNames.insert(TKiAlterReplication::CallableName());
        DataSinkNames.insert(TKiDropReplication::CallableName());
        DataSinkNames.insert(TKiCreateUser::CallableName());
        DataSinkNames.insert(TKiModifyPermissions::CallableName());
        DataSinkNames.insert(TKiAlterUser::CallableName());
        DataSinkNames.insert(TKiDropUser::CallableName());
        DataSinkNames.insert(TKiUpsertObject::CallableName());
        DataSinkNames.insert(TKiCreateObject::CallableName());
        DataSinkNames.insert(TKiAlterObject::CallableName());
        DataSinkNames.insert(TKiDropObject::CallableName());
        DataSinkNames.insert(TKiCreateGroup::CallableName());
        DataSinkNames.insert(TKiAlterGroup::CallableName());
        DataSinkNames.insert(TKiRenameGroup::CallableName());
        DataSinkNames.insert(TKiDropGroup::CallableName());
        DataSinkNames.insert(TKiDataQueryBlock::CallableName());
        DataSinkNames.insert(TKiDataQueryBlocks::CallableName());
        DataSinkNames.insert(TKiExecDataQuery::CallableName());
        DataSinkNames.insert(TKiEffects::CallableName());
        DataSinkNames.insert(TPgDropObject::CallableName());
        DataSinkNames.insert(TKiReturningList::CallableName());
        DataSinkNames.insert(TKiCreateSequence::CallableName());
        DataSinkNames.insert(TKiDropSequence::CallableName());
        DataSinkNames.insert(TKiAlterSequence::CallableName());
        DataSinkNames.insert(TKiAnalyzeTable::CallableName());

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
            TYdbOperation::CreateReplication |
            TYdbOperation::AlterReplication |
            TYdbOperation::DropReplication |
            TYdbOperation::CreateUser |
            TYdbOperation::AlterUser |
            TYdbOperation::DropUser |
            TYdbOperation::CreateGroup |
            TYdbOperation::AlterGroup |
            TYdbOperation::DropGroup |
            TYdbOperation::RenameGroup |
            TYdbOperation::ModifyPermission;

        SystemColumns = {
            {"_yql_partition_id", NKikimr::NUdf::EDataSlot::Uint64}
        };
    }
};

} // namespace

const TKikimrTableDescription* TKikimrTablesData::EnsureTableExists(const TString& cluster,
    const TString& table, TPositionHandle pos, TExprContext& ctx) const
{
    auto tablePath = table;
    if (TempTablesState) {
        auto tempTableInfoIt = TempTablesState->FindInfo(table, true);

        if (tempTableInfoIt != TempTablesState->TempTables.end()) {
            tablePath = tempTableInfoIt->first;
        }
    }

    auto desc = Tables.FindPtr(std::make_pair(cluster, tablePath));
    if (desc && (desc->GetTableType() != ETableType::Table || desc->DoesExist())) {
        return desc;
    }

    ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder()
        << "Cannot find table '" << NCommon::FullTableName(cluster, tablePath)
        << "' because it does not exist or you do not have access permissions."
        << " Please check correctness of table path and user permissions."));
    return nullptr;
}

TKikimrTableDescription& TKikimrTablesData::GetOrAddTable(const TString& cluster, const TString& database, const TString& table, ETableType tableType) {
    auto tablePath = table;
    if (TempTablesState) {
        auto tempTableInfoIt = TempTablesState->FindInfo(table, true);

        if (tempTableInfoIt != TempTablesState->TempTables.end()) {
            tablePath = tempTableInfoIt->first;
        }
    }

    if (!Tables.FindPtr(std::make_pair(cluster, tablePath))) {
        auto& desc = Tables[std::make_pair(cluster, tablePath)];

        TString error;
        std::pair<TString, TString> pathPair;
        if (NKikimr::TrySplitPathByDb(tablePath, database, pathPair, error)) {
            desc.RelativePath = pathPair.second;
        }
        desc.SetTableType(tableType);

        return desc;
    }

    return Tables[std::make_pair(cluster, tablePath)];
}

TKikimrTableDescription& TKikimrTablesData::GetTable(const TString& cluster, const TString& table) {
    auto tablePath = table;
    if (TempTablesState) {
        auto tempTableInfoIt = TempTablesState->FindInfo(table, true);

        if (tempTableInfoIt != TempTablesState->TempTables.end()) {
            tablePath = tempTableInfoIt->first;
        }
    }

    auto desc = Tables.FindPtr(std::make_pair(cluster, tablePath));
    YQL_ENSURE(desc, "Unexpected empty metadata, cluster '" << cluster << "', table '" << table << "'");

    return *desc;
}

const TKikimrTableDescription& TKikimrTablesData::ExistingTable(const TStringBuf& cluster,
    const TStringBuf& table) const
{
    auto tablePath = table;
    if (TempTablesState) {
        auto tempTableInfoIt = TempTablesState->FindInfo(table, true);

        if (tempTableInfoIt != TempTablesState->TempTables.end()) {
            tablePath = tempTableInfoIt->first;
        }
    }

    auto desc = Tables.FindPtr(std::make_pair(TString(cluster), TString(tablePath)));
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
    } else if (tagName == "replication") {
        KeyType = Type::Replication;
        const TExprNode* nameNode = key.Child(0)->Child(1);
        if (!nameNode->IsCallable("String")) {
            Ctx.AddError(TIssue(Ctx.GetPosition(key.Pos()), "Expected String as replication key."));
            return false;
        }
        Target = nameNode->Child(0)->Content();
    } else if(tagName == "permission") {
        KeyType = Type::Permission;
        Target = key.Child(0)->Child(1)->Child(0)->Content();
    } else if (tagName == "pgObject") {
        KeyType = Type::PGObject;
        Target = key.Child(0)->Child(1)->Child(0)->Content();
        ObjectType = key.Child(0)->Child(2)->Child(0)->Content();
    } else {
        Ctx.AddError(TIssue(Ctx.GetPosition(key.Child(0)->Pos()), TString("Unexpected tag for kikimr key: ") + tagName));
        return false;
    }

    if (key.ChildrenSize() > 1 && KeyType != Type::Object) {
        for (ui32 i = 1; i < key.ChildrenSize(); ++i) {
            auto tag = key.Child(i)->Child(0);
            if (tag->Content() == TStringBuf("view")) {
                const TExprNode* viewNode = key.Child(i)->Child(1);
                if (viewNode->ChildrenSize() == 0 && viewNode->IsList()) {
                    View = {"", true};
                    continue;
                }
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

                if (View) {
                    Ctx.AddError(TIssue(Ctx.GetPosition(tag->Pos()), "Incosistent view tags"));
                    return false;
                }

                View = TViewDescription{TString(viewNode->Child(0)->Content())};
            } else if (tag->Content() == TStringBuf("primary_view")) {
                if (View) {
                    Ctx.AddError(TIssue(Ctx.GetPosition(tag->Pos()), "Incosistent view tags"));
                    return false;
                }

                View = TViewDescription{"", true};
            } else {
                Ctx.AddError(TIssue(Ctx.GetPosition(tag->Pos()), TStringBuilder() << "Unexpected tag for kikimr key child: " << tag->Content()));
                return false;
            }
        }
    }
    return true;
}

TCoAtomList BuildColumnsList(const TKikimrTableDescription& table, TPositionHandle pos,
    TExprContext& ctx, bool withSystemColumns, bool ignoreWriteOnlyColumns)
{
    TVector<TExprBase> columnsToSelect;
    for (const auto& pair : table.Metadata->Columns) {
        if (pair.second.IsBuildInProgress && ignoreWriteOnlyColumns) {
            continue;
        }

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

// Is used only for key types now
template<typename TProto>
void FillLiteralProtoImpl(const NNodes::TCoDataCtor& literal, TProto& proto) {
    auto type = literal.Ref().GetTypeAnn();

    // TODO: support pg types
    YQL_ENSURE(type->GetKind() != ETypeAnnotationKind::Pg, "pg types are not supported");

    auto slot = type->Cast<TDataExprType>()->GetSlot();
    auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;

    YQL_ENSURE(NKikimr::NScheme::NTypeIds::IsYqlType(typeId) &&
        NKikimr::NSchemeShard::IsAllowedKeyType(NKikimr::NScheme::TTypeInfo(typeId)));

    auto& protoType = *proto.MutableType();
    auto& protoValue = *proto.MutableValue();

    protoType.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    protoType.MutableData()->SetScheme(typeId);

    auto value = literal.Literal().Value();

    switch (slot) {
        case EDataSlot::Bool:
            protoValue.SetBool(FromString<bool>(value));
            break;
        case EDataSlot::Uint8:
        case EDataSlot::Uint32:
        case EDataSlot::Date:
        case EDataSlot::Datetime:
            protoValue.SetUint32(FromString<ui32>(value));
            break;
        case EDataSlot::Int8:
        case EDataSlot::Int32:
        case EDataSlot::Date32:
            protoValue.SetInt32(FromString<i32>(value));
            break;
        case EDataSlot::Int64:
        case EDataSlot::Interval:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
            protoValue.SetInt64(FromString<i64>(value));
            break;
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            protoValue.SetUint64(FromString<ui64>(value));
            break;
        case EDataSlot::String:
        case EDataSlot::DyNumber:
            protoValue.SetBytes(value.Data(), value.Size());
            break;
        case EDataSlot::Utf8:
        case EDataSlot::Json:
            protoValue.SetText(ToString(value));
            break;
        case EDataSlot::Double:
            protoValue.SetDouble(FromString<double>(value));
            break;
        case EDataSlot::Float:
            protoValue.SetFloat(FromString<float>(value));
            break;
        case EDataSlot::Yson:
            protoValue.SetBytes(ToString(value));
            break;
        case EDataSlot::Decimal: {
            const auto paramsDataType = type->Cast<TDataExprParamsType>();
            auto precision = FromString<ui8>(paramsDataType->GetParamOne());
            auto scale = FromString<ui8>(paramsDataType->GetParamTwo());
            protoType.MutableData()->MutableDecimalParams()->SetPrecision(precision);
            protoType.MutableData()->MutableDecimalParams()->SetScale(scale);

            auto v = NDecimal::FromString(literal.Cast<TCoDecimal>().Literal().Value(), precision, scale);
            const auto p = reinterpret_cast<ui8*>(&v);
            protoValue.SetLow128(*reinterpret_cast<ui64*>(p));
            protoValue.SetHi128(*reinterpret_cast<ui64*>(p + 8));
            break;
        }
        case EDataSlot::Uuid: {
            const ui64* uuidData = reinterpret_cast<const ui64*>(value.Data());
            protoValue.SetLow128(uuidData[0]);
            protoValue.SetHi128(uuidData[1]);
            break;
        }

        default:
            YQL_ENSURE(false, "Unexpected type slot " << slot);
    }
}

void FillLiteralProto(const NNodes::TCoDataCtor& literal, NKqpProto::TKqpPhyLiteralValue& proto) {
    FillLiteralProtoImpl(literal, proto);
}

bool IsPgNullExprNode(const NNodes::TExprBase& maybeLiteral) {
    return maybeLiteral.Ptr()->IsCallable() &&
        maybeLiteral.Ptr()->Content() == "PgCast" && maybeLiteral.Ptr()->ChildrenSize() >= 1 &&
        maybeLiteral.Ptr()->Child(0)->IsCallable() && maybeLiteral.Ptr()->Child(0)->Content() == "Null";
}

std::optional<TString> FillLiteralProto(NNodes::TExprBase maybeLiteral, const TTypeAnnotationNode* valueType, Ydb::TypedValue& proto)
{
    if (auto maybeJust = maybeLiteral.Maybe<TCoJust>()) {
        maybeLiteral = maybeJust.Cast().Input();
    }

    if (auto literal = maybeLiteral.Maybe<TCoDataCtor>()) {
        FillLiteralProto(literal.Cast(), proto);
        return std::nullopt;
    }

    const bool isPgNull = IsPgNullExprNode(maybeLiteral);
    if (maybeLiteral.Maybe<TCoPgConst>() || isPgNull) {
        YQL_ENSURE(valueType);
        auto actualPgType = valueType->Cast<TPgExprType>();
        YQL_ENSURE(actualPgType);

        auto* typeDesc = NKikimr::NPg::TypeDescFromPgTypeId(actualPgType->GetId());
        if (!typeDesc) {
            return TStringBuilder() << "Failed to parse default expr typename " << actualPgType->GetName();
        }

        if (isPgNull) {
            proto.mutable_value()->set_null_flag_value(NProtoBuf::NULL_VALUE);
        } else {
            YQL_ENSURE(maybeLiteral.Maybe<TCoPgConst>());
            auto pgConst = maybeLiteral.Cast<TCoPgConst>();
            TString content = TString(pgConst.Value().Value());
            auto parseResult = NKikimr::NPg::PgNativeBinaryFromNativeText(content, typeDesc);
            if (parseResult.Error) {
                return TStringBuilder() << "Failed to parse default expr for typename " << actualPgType->GetName()
                    << ", error reason: " << *parseResult.Error;
            }

            proto.mutable_value()->set_bytes_value(parseResult.Str);
        }

        auto* pg = proto.mutable_type()->mutable_pg_type();
        pg->set_type_name(NKikimr::NPg::PgTypeNameFromTypeDesc(typeDesc));
        pg->set_oid(NKikimr::NPg::PgTypeIdFromTypeDesc(typeDesc));
        return std::nullopt;
    }

    return TStringBuilder() << "Unsupported type of literal: " << maybeLiteral.Ptr()->Content();
}

void FillLiteralProto(const NNodes::TCoDataCtor& literal, Ydb::TypedValue& proto)
{
    auto type = literal.Ref().GetTypeAnn();

    // TODO: support pg types
    YQL_ENSURE(type->GetKind() != ETypeAnnotationKind::Pg, "pg types are not supported");

    auto slot = type->Cast<TDataExprType>()->GetSlot();
    auto typeId = NKikimr::NUdf::GetDataTypeInfo(slot).TypeId;

    YQL_ENSURE(NKikimr::NScheme::NTypeIds::IsYqlType(typeId));

    auto& protoType = *proto.mutable_type();
    auto& protoValue = *proto.mutable_value();

    protoType.set_type_id((Ydb::Type::PrimitiveTypeId)typeId);

    auto value = literal.Literal().Value();

    switch (slot) {
        case EDataSlot::Bool:
            protoValue.set_bool_value(FromString<bool>(value));
            break;
        case EDataSlot::Uint8:
        case EDataSlot::Uint32:
        case EDataSlot::Date:
        case EDataSlot::Datetime:
            protoValue.set_uint32_value(FromString<ui32>(value));
            break;
        case EDataSlot::Int8:
        case EDataSlot::Int32:
        case EDataSlot::Date32:
            protoValue.set_int32_value(FromString<i32>(value));
            break;
        case EDataSlot::Int64:
        case EDataSlot::Interval:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
            protoValue.set_int64_value(FromString<i64>(value));
            break;
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            protoValue.set_uint64_value(FromString<ui64>(value));
            break;
        case EDataSlot::String:
        case EDataSlot::DyNumber:
            protoValue.set_bytes_value(value.Data(), value.Size());
            break;
        case EDataSlot::Utf8:
        case EDataSlot::Json:
            protoValue.set_text_value(ToString(value));
            break;
        case EDataSlot::Double:
            protoValue.set_double_value(FromString<double>(value));
            break;
        case EDataSlot::Float:
            protoValue.set_float_value(FromString<float>(value));
            break;
        case EDataSlot::Yson:
            protoValue.set_bytes_value(ToString(value));
            break;
        case EDataSlot::Decimal: {
            const auto paramsDataType = type->Cast<TDataExprParamsType>();
            auto precision = FromString<ui8>(paramsDataType->GetParamOne());
            auto scale = FromString<ui8>(paramsDataType->GetParamTwo());
            protoType.mutable_decimal_type()->set_precision(precision);
            protoType.mutable_decimal_type()->set_scale(scale);

            auto v = NDecimal::FromString(literal.Cast<TCoDecimal>().Literal().Value(), precision, scale);
            const auto p = reinterpret_cast<ui8*>(&v);
            protoValue.set_low_128(*reinterpret_cast<ui64*>(p));
            protoValue.set_high_128(*reinterpret_cast<ui64*>(p + 8));
            break;
        }
        case EDataSlot::Uuid: {
            const ui64* uuidData = reinterpret_cast<const ui64*>(value.Data());
            protoValue.set_low_128(uuidData[0]);
            protoValue.set_high_128(uuidData[1]);
            break;
        }

        default:
            YQL_ENSURE(false, "Unexpected type slot " << slot);
    }
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

Ydb::Table::VectorIndexSettings_Distance VectorIndexSettingsParseDistance(std::string_view distance) {
    if (distance == "cosine")
        return Ydb::Table::VectorIndexSettings::DISTANCE_COSINE;
    else if (distance == "manhattan")
        return Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN;
    else if (distance == "euclidean")
        return Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN;
    else 
        YQL_ENSURE(false, "Wrong index setting distance: " << distance);
};

Ydb::Table::VectorIndexSettings_Similarity VectorIndexSettingsParseSimilarity(std::string_view similarity) {
    if (similarity == "cosine")
        return Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE;
    else if (similarity == "inner_product")
        return Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT;
    else
        YQL_ENSURE(false, "Wrong index setting similarity: " << similarity);
};

Ydb::Table::VectorIndexSettings_VectorType VectorIndexSettingsParseVectorType(std::string_view vectorType) {
    if (vectorType == "float")
        return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT;
    else if (vectorType == "uint8")
        return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8;
    else if (vectorType == "int8")
        return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8;
    else if (vectorType == "bit")
        return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT;
    else
        YQL_ENSURE(false, "Wrong index setting vector_type: " << vectorType);
};

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
