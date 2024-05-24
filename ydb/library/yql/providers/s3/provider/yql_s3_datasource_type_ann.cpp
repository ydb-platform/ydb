#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

bool ValidateS3PackedPaths(TPositionHandle pos, TStringBuf blob, bool isTextEncoded, TExprContext& ctx) {
    using namespace NYql::NS3Details;
    try {
        TPathList paths;
        UnpackPathsList(blob, isTextEncoded, paths);
        for (size_t i = 0; i < paths.size(); ++i) {
            if (paths[i].Path.empty()) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected non-empty path (index " << i << ")"));
                return false;
            }
        }
    } catch (const std::exception& ex) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Failed to parse packed paths: " << ex.what()));
        return false;
    }
    return true;
}

bool ValidateS3Paths(TExprNode& node, const TStructExprType*& extraColumnsType, TExprContext& ctx) {
    if (!EnsureTupleMinSize(node, 1, ctx)) {
        return false;
    }

    extraColumnsType = nullptr;
    for (auto& path : node.ChildrenList()) {
        if (!EnsureTupleSize(*path, 3, ctx)) {
            return false;
        }

        auto pathAndSizeList = path->Child(TS3Path::idx_Data);
        if (!TCoString::Match(pathAndSizeList)) {
            ctx.AddError(TIssue(ctx.GetPosition(pathAndSizeList->Pos()), "Expected String literal for Data"));
            return false;
        }

        const TExprNode* isTextEncoded = path->Child(TS3Path::idx_IsText);
        if (!TCoBool::Match(isTextEncoded)) {
            ctx.AddError(TIssue(ctx.GetPosition(isTextEncoded->Pos()), "Expected Bool literal for IsText"));
            return false;
        }

        if (!ValidateS3PackedPaths(pathAndSizeList->Pos(), pathAndSizeList->Head().Content(), FromString<bool>(isTextEncoded->Head().Content()), ctx)) {
            return false;
        }

        auto extraColumns = path->Child(TS3Path::idx_ExtraColumns);
        if (!EnsureStructType(*extraColumns, ctx) || !EnsurePersistable(*extraColumns, ctx)) {
            return false;
        }

        const TStructExprType* current = extraColumns->GetTypeAnn()->Cast<TStructExprType>();
        if (!extraColumnsType) {
            extraColumnsType = current;
        } else if (!IsSameAnnotation(*current, *extraColumnsType)) {
            ctx.AddError(TIssue(ctx.GetPosition(path->Pos()), TStringBuilder() << "Extra columns type mismatch: got "
                << *(const TTypeAnnotationNode*)current << ", expecting "
                << *(const TTypeAnnotationNode*)extraColumnsType));
            return false;
        }
    }
    return true;
}

class TTypeValidator {
    using TTypesContainer = std::unordered_set<const TTypeAnnotationNode*, TTypeAnnotationNode::THash, TTypeAnnotationNode::TEqual>;

public:
    TTypeValidator(TExprContext& ctx, const TExprNode::TPtr& input, const TStructExprType* columnsType)
        : Ctx(ctx)
        , Input(input)
        , ColumnsType(columnsType)
        , IntegerTypes(CreateIntegerAvailableTypes())
        , CommonTypes(CreateCommonAvailableTypes())
        , EnumTypes(CreateEnumAvailableTypes())
        , DateTypes(CreateDateAvailableTypes())
    {
        for (auto item: ColumnsType->GetItems()) {
            auto type = item->GetItemType();
            if (type->GetKind() == ETypeAnnotationKind::Data) {
                DataSlotColumns[TString{item->GetName()}] = type->Cast<TDataExprType>()->GetSlot();
            }
        }
    }

    bool ValidatePartitonBy(const std::vector<TString>& partitionedBy) {
        TSet<TString> partitionedByColumns{partitionedBy.begin(), partitionedBy.end()};
        for (auto item: ColumnsType->GetItems()) {
            if (!partitionedByColumns.contains(item->GetName())) {
                continue;
            }
            if (!ValidateCommonType(item)) {
                return false;
            }
        }
        return true;
    }

    bool ValidateProjection(
        const TString& projection,
        const std::vector<TString>& partitionedBy,
        size_t pathsLimit) {
        auto generator = NPathGenerator::CreatePathGenerator(
            projection, partitionedBy, DataSlotColumns, pathsLimit);
        TMap<TString, NPathGenerator::IPathGenerator::EType> projectionColumns;
        for (const auto& column: generator->GetConfig().Rules) {
            projectionColumns[column.Name] = column.Type;
        }
        for (auto item: ColumnsType->GetItems()) {
            auto it = projectionColumns.find(item->GetName());
            if (it == projectionColumns.end()) {
                continue;
            }
            if (!ValidateType(item, it->second)) {
                return false;
            }
        }
        return true;
    }

private:
    bool ValidateCommonType(const TItemExprType* item) {
        return ValidateType(item, CommonTypes);
    }

    bool ValidateType(const TItemExprType* item, NYql::NPathGenerator::IPathGenerator::EType type) {
        switch (type) {
            case NYql::NPathGenerator::IPathGenerator::EType::INTEGER:
                return ValidateIntegerType(item);
            case NYql::NPathGenerator::IPathGenerator::EType::ENUM:
                return ValidateEnumType(item);
            case NYql::NPathGenerator::IPathGenerator::EType::DATE:
                return ValidateDateType(item);
            case NYql::NPathGenerator::IPathGenerator::EType::UNDEFINED:
                Ctx.AddError(TIssue(Ctx.GetPosition(Input->Child(TS3ReadObject::idx_RowType)->Pos()), TStringBuilder{} << "Projection column \"" << item->GetName() << "\" has undefined projection type"));
                return false;
        }
    }

    bool ValidateDateType(const TItemExprType* item) {
        return ValidateType(item, DateTypes);
    }

    bool ValidateIntegerType(const TItemExprType* item) {
        return ValidateType(item, IntegerTypes);
    }

    bool ValidateEnumType(const TItemExprType* item) {
        return ValidateType(item, EnumTypes);
    }

    bool ValidateType(const TItemExprType* item, const TTypesContainer& availableTypes) {
        auto it = availableTypes.find(item->GetItemType());
        if (it != availableTypes.end()) {
            return true;

        }
        Ctx.AddError(TIssue(Ctx.GetPosition(Input->Child(TS3ReadObject::idx_RowType)->Pos()), TStringBuilder{} << "Projection column \"" << item->GetName() << "\" has invalid type " << *item->GetItemType()));
        return false;
    }

    TTypesContainer CreateIntegerAvailableTypes() const {
        return {
            Ctx.MakeType<TDataExprType>(EDataSlot::String),
            Ctx.MakeType<TDataExprType>(EDataSlot::Utf8),
            Ctx.MakeType<TDataExprType>(EDataSlot::Int64),
            Ctx.MakeType<TDataExprType>(EDataSlot::Int32),
            Ctx.MakeType<TDataExprType>(EDataSlot::Uint32),
            Ctx.MakeType<TDataExprType>(EDataSlot::Uint64)
        };
    }

    TTypesContainer CreateEnumAvailableTypes() const {
        return {
            Ctx.MakeType<TDataExprType>(EDataSlot::String)
        };
    }

    TTypesContainer CreateCommonAvailableTypes() const {
        return {
            Ctx.MakeType<TDataExprType>(EDataSlot::String),
            Ctx.MakeType<TDataExprType>(EDataSlot::Utf8),
            Ctx.MakeType<TDataExprType>(EDataSlot::Int64),
            Ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
            Ctx.MakeType<TDataExprType>(EDataSlot::Int32),
            Ctx.MakeType<TDataExprType>(EDataSlot::Uint32),
            Ctx.MakeType<TDataExprType>(EDataSlot::Date),
            Ctx.MakeType<TDataExprType>(EDataSlot::Datetime)
        };
    }

    TTypesContainer CreateDateAvailableTypes() const {
        return {
            Ctx.MakeType<TDataExprType>(EDataSlot::String),
            Ctx.MakeType<TDataExprType>(EDataSlot::Utf8),
            Ctx.MakeType<TDataExprType>(EDataSlot::Uint32),
            Ctx.MakeType<TDataExprType>(EDataSlot::Date),
            Ctx.MakeType<TDataExprType>(EDataSlot::Datetime)
        };
    }

private:
    TExprContext& Ctx;
    const TExprNode::TPtr& Input;
    const TStructExprType* ColumnsType;
    const TTypesContainer IntegerTypes;
    const TTypesContainer CommonTypes;
    const TTypesContainer EnumTypes;
    const TTypesContainer DateTypes;
    TMap<TString, NUdf::EDataSlot> DataSlotColumns;
};

bool ValidateProjectionTypes(
    const TStructExprType* columnsType,
    const TString& projection,
    const std::vector<TString>& partitionedBy,
    const TExprNode::TPtr& input,
    TExprContext& ctx,
    size_t pathsLimit) {
    if (!columnsType) {
        return true;
    }

    TTypeValidator typeValidator(ctx, input, columnsType);
    if (!projection && !partitionedBy.empty()) {
        if (!typeValidator.ValidatePartitonBy(partitionedBy)) {
            return false;
        }
    }

    if (!projection || partitionedBy.empty()) {
        return true;
    }

    try {
        if (!typeValidator.ValidateProjection(projection, partitionedBy, pathsLimit)) {
            return false;
        }
    } catch (...) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3ReadObject::idx_RowType)->Pos()), CurrentExceptionMessage()));
        return false;
    }

    return true;
}

bool ExtractSettingValue(const TExprNode& value, TStringBuf settingName, TStringBuf format, TStringBuf expectedFormat, TExprContext& ctx, TStringBuf& settingValue) {
    if (expectedFormat && format != expectedFormat) {
        ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), TStringBuilder() << settingName << " can only be used with " << expectedFormat << " format"));
        return false;
    }

    if (value.IsAtom()) {
        settingValue = value.Content();
        return true;
    }

    if (!value.IsCallable({ "String", "Utf8" })) {
        ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), TStringBuilder() << settingName << " must be literal value"));
        return false;
    }
    settingValue = value.Head().Content();
    return true;

}

bool EnsureParquetTypeSupported(TPositionHandle position, const TTypeAnnotationNode* type, TExprContext& ctx, const IArrowResolver::TPtr& arrowResolver) {
    auto resolveStatus = arrowResolver->AreTypesSupported(ctx.GetPosition(position), { type }, ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);

    if (resolveStatus != IArrowResolver::OK) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder() << "Type " << *type << " is not supported for parquet"));
        return false;
    }

    return true;
}

class TS3DataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TS3DataSourceTypeAnnotationTransformer(TS3State::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TS3DataSourceTypeAnnotationTransformer;
        AddHandler({TS3ReadObject::CallableName()}, Hndl(&TSelf::HandleRead));
        AddHandler({TS3Object::CallableName()}, Hndl(&TSelf::HandleObject));
        AddHandler({TS3SourceSettings::CallableName()}, Hndl(&TSelf::HandleS3SourceSettings));
        AddHandler({TS3ParseSettings::CallableName()}, Hndl(&TSelf::HandleS3ParseSettings));
        AddHandler({TCoConfigure::CallableName()}, Hndl(&TSelf::HandleConfig));
    }

    TStatus HandleS3SourceSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 3U, ctx)) {
            return TStatus::Error;
        }

        const TStructExprType* extraColumnsType = nullptr;
        if (!ValidateS3Paths(*input->Child(TS3SourceSettings::idx_Paths), extraColumnsType, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TS3SourceSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3SourceSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TS3SourceSettings::idx_RowsLimitHint), ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = ctx.MakeType<TDataExprType>(EDataSlot::String);
        if (extraColumnsType->GetSize()) {
            itemType = ctx.MakeType<TTupleExprType>(
                TTypeAnnotationNode::TListType{ itemType, ctx.MakeType<TDataExprType>(EDataSlot::Uint64) });
        }
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(itemType));
        return TStatus::Ok;
    }

    TStatus HandleS3ParseSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 5U, 6U, ctx)) {
            return TStatus::Error;
        }

        const TStructExprType* extraColumnsType = nullptr;
        if (!ValidateS3Paths(*input->Child(TS3ParseSettings::idx_Paths), extraColumnsType, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TS3ParseSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3ParseSettings::idx_Token)->Pos()),
                                TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TS3ParseSettings::idx_RowsLimitHint), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TS3ParseSettings::idx_Format), ctx) ||
            !NCommon::ValidateFormatForInput(input->Child(TS3ParseSettings::idx_Format)->Content(), nullptr, nullptr, ctx))
        {
            return TStatus::Error;
        }

        const auto& rowTypeNode = *input->Child(TS3ParseSettings::idx_RowType);
        if (!EnsureType(rowTypeNode, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* rowType = rowTypeNode.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureStructType(rowTypeNode.Pos(), *rowType, ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TS3ParseSettings::idx_Settings &&
            !EnsureTuple(*input->Child(TS3ParseSettings::idx_Settings), ctx))
        {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        if (input->Child(TS3ParseSettings::idx_Format)->Content() == "parquet") {
            std::unordered_set<TString> extraColumnNames(extraColumnsType->GetSize());
            for (const auto& extraColumn : extraColumnsType->GetItems()) {
                extraColumnNames.insert(TString{extraColumn->GetName()});
            }

            TVector<const TItemExprType*> blockRowTypeItems;
            for (const auto& x : rowType->Cast<TStructExprType>()->GetItems()) {
                if (!extraColumnNames.contains(TString{x->GetName()})) {
                    blockRowTypeItems.push_back(ctx.MakeType<TItemExprType>(x->GetName(), ctx.MakeType<TBlockExprType>(x->GetItemType())));
                }
            }

            blockRowTypeItems.push_back(ctx.MakeType<TItemExprType>(BlockLengthColumnName, ctx.MakeType<TScalarExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64))));
            itemType = ctx.MakeType<TStructExprType>(blockRowTypeItems);
        } else {
            itemType = ctx.MakeType<TResourceExprType>("ClickHouseClient.Block");
        }

        if (extraColumnsType->GetSize()) {
            itemType = ctx.MakeType<TTupleExprType>(
                TTypeAnnotationNode::TListType{ itemType, ctx.MakeType<TDataExprType>(EDataSlot::Uint64) });
        }
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(itemType));
        return TStatus::Ok;
    }

    TStatus HandleRead(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 4U, 5U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TS3ReadObject::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TS3ReadObject::idx_DataSource), S3ProviderName, ctx)) {
            return TStatus::Error;
        }

        const auto& objectNode = input->Child(TS3ReadObject::idx_Object);
        if (!TS3Object::Match(objectNode)) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3ReadObject::idx_Object)->Pos()), "Expected S3 object."));
            return TStatus::Error;
        }

        const auto& rowTypeNode = *input->Child(TS3ReadObject::idx_RowType);
        if (!EnsureType(rowTypeNode, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* rowType = rowTypeNode.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureStructType(rowTypeNode.Pos(), *rowType, ctx)) {
            return TStatus::Error;
        }

        std::vector<TString> partitionedBy;
        TString projection;
        {
            TS3Object s3Object(input->Child(TS3ReadObject::idx_Object));
            auto format = s3Object.Format().Ref().Content();
            const TStructExprType* structRowType = rowType->Cast<TStructExprType>();

            THashSet<TStringBuf> columns;
            for (const TItemExprType* item : structRowType->GetItems()) {
                columns.emplace(item->GetName());
            }
            
            if (TMaybeNode<TExprBase> settings = s3Object.Settings()) {
                for (auto& settingNode : settings.Raw()->ChildrenList()) {
                    const TStringBuf name = settingNode->Head().Content();
                    if (name == "partitionedby"sv) {
                        for (size_t i = 1; i < settingNode->ChildrenSize(); ++i) {
                            const auto& column = settingNode->Child(i);
                            if (!EnsureAtom(*column, ctx)) {
                                return TStatus::Error;
                            }
                            columns.erase(column->Content());
                            partitionedBy.push_back(TString{column->Content()});
                        }
                        if (columns.empty()) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Table contains no columns except partitioning columns"));
                            return TStatus::Error;
                        }
                    }
                    if (name == "projection"sv) {
                        projection = settingNode->Tail().Content();
                    }
                }
            }

            TSet<TString> partitionedBySet{partitionedBy.begin(), partitionedBy.end()};
            if (!NCommon::ValidateFormatForInput(
                format,
                structRowType,
                [partitionedBySet](TStringBuf fieldName) {return partitionedBySet.contains(fieldName); },
                ctx)) {
                return TStatus::Error;
            }
        }

        if (!ValidateProjectionTypes(
                rowType->Cast<TStructExprType>(),
                projection,
                partitionedBy,
                input,
                ctx,
                State_->Configuration->GeneratorPathsLimit)) {
            return TStatus::Error;
        }

        if (objectNode->Child(TS3Object::idx_Format)->Content() == "parquet") {
            YQL_ENSURE(State_->Types->ArrowResolver);
            bool allTypesSupported = true;
            for (const auto& item : rowType->Cast<TStructExprType>()->GetItems()) {
                if (!EnsureParquetTypeSupported(input->Pos(), item->GetItemType(), ctx, State_->Types->ArrowResolver)) {
                    allTypesSupported = false;
                }
            }
            if (!allTypesSupported) {
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TS3ReadObject::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(rowType)
        }));

        if (input->ChildrenSize() > TS3ReadObject::idx_ColumnOrder) {
            auto& order = *input->Child(TS3ReadObject::idx_ColumnOrder);
            if (!EnsureTupleOfAtoms(order, ctx)) {
                return TStatus::Error;
            }
            TVector<TString> columnOrder;
            THashSet<TStringBuf> uniqs;
            columnOrder.reserve(order.ChildrenSize());
            uniqs.reserve(order.ChildrenSize());

            for (auto& child : order.ChildrenList()) {
                TStringBuf col = child->Content();
                if (!uniqs.emplace(col).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate column '" << col << "' in column order list"));
                    return TStatus::Error;
                }
                columnOrder.push_back(ToString(col));
            }
            return State_->Types->SetColumnOrder(*input, columnOrder, ctx);
        }

        return TStatus::Ok;
    }

    TStatus HandleConfig(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TCoConfigure::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TCoConfigure::idx_DataSource), S3ProviderName, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(input->Child(TCoConfigure::idx_World)->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleObject(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 2U, 3U, ctx)) {
            return TStatus::Error;
        }

        const TStructExprType* extraColumnsType = nullptr;
        if (!ValidateS3Paths(*input->Child(TS3Object::idx_Paths), extraColumnsType, ctx)) {
            return TStatus::Error;
        }

        const auto format = input->Child(TS3Object::idx_Format)->Content();
        if (!EnsureAtom(*input->Child(TS3Object::idx_Format), ctx) || !NCommon::ValidateFormatForInput(format, nullptr, nullptr, ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TS3Object::idx_Settings) {
            bool haveProjection = false;
            bool havePartitionedBy = false;
            bool hasDateTimeFormat = false;
            bool hasDateTimeFormatName = false;
            bool hasTimestampFormat = false;
            bool hasTimestampFormatName = false;
            auto validator = [&](TStringBuf name, TExprNode& setting, TExprContext& ctx) {
                if (name != "partitionedby"sv && name != "directories"sv && setting.ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()),
                        TStringBuilder() << "Expected single value setting for " << name << ", but got " << setting.ChildrenSize() - 1));
                    return false;
                }

                if (name == "compression"sv) {
                    TStringBuf compression;
                    if (!ExtractSettingValue(setting.Tail(), "compression"sv, format, {}, ctx, compression)) {
                        return false;
                    }
                    return NCommon::ValidateCompressionForInput(format, compression, ctx);
                }

                if (name == "partitionedby"sv) {
                    havePartitionedBy = true;
                    if (setting.ChildrenSize() < 2) {
                        ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Expected at least one column in partitioned_by setting"));
                        return false;
                    }

                    THashSet<TStringBuf> uniqs;
                    for (size_t i = 1; i < setting.ChildrenSize(); ++i) {
                        const auto& column = setting.Child(i);
                        if (!EnsureAtom(*column, ctx)) {
                            return false;
                        }
                        if (!uniqs.emplace(column->Content()).second) {
                            ctx.AddError(TIssue(ctx.GetPosition(column->Pos()),
                                TStringBuilder() << "Duplicate partitioned_by column '" << column->Content() << "'"));
                            return false;
                        }
                    }

                    return true;
                }

                if (name == "data.interval.unit"sv) {
                    TStringBuf unit;
                    if (!ExtractSettingValue(setting.Tail(), "data.interval.unit"sv, format, {}, ctx, unit)) {
                        return false;
                    }
                    return NCommon::ValidateIntervalUnit(unit, ctx);
                }

                if (name == "data.datetime.formatname"sv) {
                    hasDateTimeFormatName = true;
                    TStringBuf formatName;
                    if (!ExtractSettingValue(setting.Tail(), "data.datetime.format_name"sv, format, {}, ctx, formatName)) {
                        return false;
                    }
                    return NCommon::ValidateDateTimeFormatName(formatName, ctx);
                }

                if (name == "data.datetime.format"sv) {
                    hasDateTimeFormat = true;
                    TStringBuf unused;
                    if (!ExtractSettingValue(setting.Tail(), "data.datetime.format"sv, format, {}, ctx, unused)) {
                        return false;
                    }
                    return true;
                }

                if (name == "data.timestamp.formatname"sv) {
                    hasTimestampFormatName = true;
                    TStringBuf formatName;
                    if (!ExtractSettingValue(setting.Tail(), "data.timestamp.format_name"sv, format, {}, ctx, formatName)) {
                        return false;
                    }
                    return NCommon::ValidateTimestampFormatName(formatName, ctx);
                }

                if (name == "data.timestamp.format"sv) {
                    hasTimestampFormat = true;
                    TStringBuf unused;
                    if (!ExtractSettingValue(setting.Tail(), "data.timestamp.format"sv, format, {}, ctx, unused)) {
                        return false;
                    }
                    return true;
                }

                if (name == "readmaxbytes"sv) {
                    TStringBuf unused;
                    if (!ExtractSettingValue(setting.Tail(), "read_max_bytes"sv, format, "raw"sv, ctx, unused)) {
                        return false;
                    }
                    return true;
                }

                if (name == "csvdelimiter"sv) {
                    auto& value = setting.Tail();
                    TStringBuf delimiter;
                    if (!ExtractSettingValue(value, "csv_delimiter"sv, format, "csv_with_names"sv, ctx, delimiter)) {
                        return false;
                    }

                    if (delimiter.Size() != 1) {
                        ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), "csv_delimiter must be single character"));
                        return false;
                    }
                    return true;
                }

                if (name == "directories"sv) {
                    if (setting.ChildrenSize() != 1) {
                        ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Expected no parameters for 'directories' setting"));
                        return false;
                    }
                    return true;
                }

                if (name == "filepattern"sv) {
                    TStringBuf unused;
                    if (!ExtractSettingValue(setting.Tail(), "file_pattern"sv, format, {}, ctx, unused)) {
                        return false;
                    }
                    return true;
                }

                if (name == "pathpattern"sv) {
                    TStringBuf unused;
                    if (!ExtractSettingValue(setting.Tail(), "path_pattern"sv, format, {}, ctx, unused)) {
                        return false;
                    }
                    return true;
                }

                if (name == "pathpatternvariant"sv) {
                    TStringBuf unused;
                    if (!ExtractSettingValue(setting.Tail(), "path_pattern_variant"sv, format, {}, ctx, unused)) {
                        return false;
                    }
                    return true;
                }

                if (name == "constraints"sv) {
                    TStringBuf unused;
                    if (!ExtractSettingValue(setting.Tail(), "constraints"sv, format, {}, ctx, unused)) {
                        return false;
                    }
                    return true;
                }

                YQL_ENSURE(name == "projection"sv);
                haveProjection = true;
                if (!EnsureAtom(setting.Tail(), ctx)) {
                    return false;
                }

                if (setting.Tail().Content().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Expecting non-empty projection setting"));
                    return false;
                }

                return true;
            };
            if (!EnsureValidSettings(*input->Child(TS3Object::idx_Settings),
                                     { "compression"sv, "partitionedby"sv, "projection"sv, "data.interval.unit"sv, "constraints"sv,
                                        "data.datetime.formatname"sv, "data.datetime.format"sv, "data.timestamp.formatname"sv, "data.timestamp.format"sv,
                                        "readmaxbytes"sv, "csvdelimiter"sv, "directories"sv, "filepattern"sv, "pathpattern"sv, "pathpatternvariant"sv }, validator, ctx))
            {
                return TStatus::Error;
            }
            if (haveProjection && !havePartitionedBy) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Object::idx_Settings)->Pos()), "Missing partitioned_by setting for projection"));
                return TStatus::Error;
            }

            if (hasDateTimeFormat && hasDateTimeFormatName) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Object::idx_Settings)->Pos()), "Don't use data.datetime.format_name and data.datetime.format together"));
                return TStatus::Error;
            }

            if (hasTimestampFormat && hasTimestampFormatName) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Object::idx_Settings)->Pos()), "Don't use data.timestamp.format_name and data.timestamp.format together"));
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }
private:
    const TS3State::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateS3DataSourceTypeAnnotationTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3DataSourceTypeAnnotationTransformer>(state);
}

} // namespace NYql
