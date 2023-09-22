#include "yt_codec.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/xrange.h>
#include <util/generic/ylimits.h>
#include <util/generic/ptr.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/digest/numeric.h>
#include <util/str_stl.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

//////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

NYT::TNode ParseYsonSpec(bool inputSpec, const TString& spec) {
    YQL_ENSURE(!spec.empty());

    NYT::TNode attrs;
    TStringStream err;
    if (!NCommon::ParseYson(attrs, spec, err)) {
        ythrow yexception() << "Invalid "
            << (inputSpec ? "input" : "output")
            << " attrs: " << err.Str();
    }

    return attrs;
}

}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
void TMkqlIOSpecs::Init(NCommon::TCodecContext& codecCtx,
    const TString& inputSpecs,
    const TVector<ui32>& inputGroups,
    const TVector<TString>& tableNames,
    TType* itemType,
    const THashSet<TString>& auxColumns,
    const TString& outSpecs,
    NKikimr::NMiniKQL::IStatsRegistry* jobStats
) {
    NYT::TNode inAttrs, outAttrs;

    if (inputSpecs) {
        inAttrs = ParseYsonSpec(true, inputSpecs);
    }

    if (outSpecs) {
        outAttrs = ParseYsonSpec(false, outSpecs);
    }

    Init(
        codecCtx, inAttrs, inputGroups, tableNames,
        itemType, auxColumns, outAttrs, jobStats
    );
}

void TMkqlIOSpecs::Init(NCommon::TCodecContext& codecCtx,
    const NYT::TNode& inAttrs,
    const TVector<ui32>& inputGroups,
    const TVector<TString>& tableNames,
    TType* itemType,
    const THashSet<TString>& auxColumns,
    const NYT::TNode& outAttrs,
    NKikimr::NMiniKQL::IStatsRegistry* jobStats
) {
    if (!inAttrs.IsUndefined()) {
        InitInput(codecCtx, inAttrs, inputGroups, tableNames, itemType, {}, auxColumns);
    }
    if (!outAttrs.IsUndefined()) {
        InitOutput(codecCtx, outAttrs);
    }
    JobStats_ = jobStats;
}

void TMkqlIOSpecs::Init(NCommon::TCodecContext& codecCtx,
    const TString& inputSpecs,
    const TVector<TString>& tableNames,
    const TMaybe<TVector<TString>>& columns
) {
    Init(codecCtx, ParseYsonSpec(true, inputSpecs), tableNames, columns);
}

void TMkqlIOSpecs::Init(NCommon::TCodecContext& codecCtx,
    const NYT::TNode& inAttrs,
    const TVector<TString>& tableNames,
    const TMaybe<TVector<TString>>& columns
) {
    InitInput(codecCtx, inAttrs, {}, tableNames, nullptr, columns, {});
}

void TMkqlIOSpecs::Init(NCommon::TCodecContext& codecCtx, const TString& outSpecs) {
    Init(codecCtx, ParseYsonSpec(false, outSpecs));
}

void TMkqlIOSpecs::Init(NCommon::TCodecContext& codecCtx, const NYT::TNode& outAttrs) {
    InitOutput(codecCtx, outAttrs);
}

void TMkqlIOSpecs::SetTableOffsets(const TVector<ui64>& offsets) {
    YQL_ENSURE(Inputs.size() == offsets.size());
    TableOffsets = offsets;
}

void TMkqlIOSpecs::Clear() {
    JobStats_ = nullptr;
    Inputs.clear();
    InputGroups.clear();
    Outputs.clear();
    SystemFields_ = TSystemFields();
}

void TMkqlIOSpecs::LoadSpecInfo(bool inputSpec, const NYT::TNode& attrs, NCommon::TCodecContext& codecCtx, TSpecInfo& info) {
    YQL_ENSURE(inputSpec || attrs.HasKey(YqlRowSpecAttribute), "Missing mandatory "
        << TString{YqlRowSpecAttribute}.Quote() << " attribute");

    if (attrs.HasKey(YqlRowSpecAttribute)) {
        auto& rowSpec = attrs[YqlRowSpecAttribute];
        TStringStream err;
        info.Type = NCommon::ParseOrderAwareTypeFromYson(rowSpec[RowSpecAttrType], codecCtx, err);
        YQL_ENSURE(info.Type, "Invalid row spec type: " << err.Str());
        if (inputSpec && rowSpec.HasKey(RowSpecAttrStrictSchema)) {
            info.StrictSchema = rowSpec[RowSpecAttrStrictSchema].IsInt64()
                ? rowSpec[RowSpecAttrStrictSchema].AsInt64() != 0
                : NYT::GetBool(rowSpec[RowSpecAttrStrictSchema]);

            if (!info.StrictSchema) {
                auto stringType = TDataType::Create(NUdf::TDataType<char*>::Id, codecCtx.Env);
                auto othersDictType = TDictType::Create(stringType, stringType, codecCtx.Env);
                info.Type = codecCtx.Builder.NewStructType(info.Type, YqlOthersColumnName, othersDictType);

                // Extend input record type by weak fields with 'Yson' type
                if (rowSpec.HasKey(RowSpecAttrWeakFields)) {
                    auto& weakFields = rowSpec[RowSpecAttrWeakFields].AsList();
                    auto weakType = codecCtx.Builder.NewOptionalType(codecCtx.Builder.NewDataType(NUdf::EDataSlot::Yson));
                    auto structType = AS_TYPE(TStructType, info.Type);
                    for (auto& field: weakFields) {
                        if (!structType->FindMemberIndex(field.AsString())) {
                            info.Type = codecCtx.Builder.NewStructType(info.Type, field.AsString(), weakType);
                        }
                    }
                }
            }
        }
        if (rowSpec.HasKey(RowSpecAttrDefaultValues)) {
            YQL_ENSURE(inputSpec, "Unexpected DefaultValues attribute in output spec");
            for (auto& value : rowSpec[RowSpecAttrDefaultValues].AsMap()) {
                auto val = NYT::NodeFromYsonString(value.second.AsString());
                YQL_ENSURE(val.IsString(), "DefaultValues contains non-string value: " << value.second.AsString());
                info.DefaultValues[value.first] = val.AsString();
            }
        }

        if (rowSpec.HasKey(RowSpecAttrNativeYtTypeFlags)) {
            info.NativeYtTypeFlags = rowSpec[RowSpecAttrNativeYtTypeFlags].AsUint64();
        } else {
            if (rowSpec.HasKey(RowSpecAttrUseNativeYtTypes)) {
                info.NativeYtTypeFlags = rowSpec[RowSpecAttrUseNativeYtTypes].AsBool() ? NTCF_LEGACY : NTCF_NONE;
            } else if (rowSpec.HasKey(RowSpecAttrUseTypeV2)) {
                info.NativeYtTypeFlags = rowSpec[RowSpecAttrUseTypeV2].AsBool() ? NTCF_LEGACY : NTCF_NONE;
            }
        }

        if (rowSpec.HasKey(RowSpecAttrSortedBy)) {
            auto structType = AS_TYPE(TStructType, info.Type);
            auto& sortedBy = rowSpec[RowSpecAttrSortedBy].AsList();
            auto& sortedByType = rowSpec[RowSpecAttrSortedByTypes].AsList();
            auto& sortedDirections = rowSpec[RowSpecAttrSortDirections].AsList();
            for (size_t i: xrange(sortedBy.size())) {
                if (!structType->FindMemberIndex(sortedBy[i].AsString())) {
                    auto fieldType = NCommon::ParseTypeFromYson(sortedByType[i], codecCtx.Builder, err);
                    YQL_ENSURE(fieldType, "Invalid SortedByTypes type: " << err.Str());
                    if (!sortedDirections.empty() && !sortedDirections[i].AsInt64()) {
                        fieldType = codecCtx.Builder.NewDataType(NUdf::EDataSlot::String);
                    }
                    info.AuxColumns.emplace(sortedBy[i].AsString(), fieldType);
                }
            }
        }

        if (rowSpec.HasKey(RowSpecAttrExplicitYson)) {
            YQL_ENSURE(inputSpec, "Unexpected ExplicitYson attribute in output spec");
            for (auto& value : rowSpec[RowSpecAttrExplicitYson].AsList()) {
                YQL_ENSURE(value.IsString(), "ExplicitYson contains non-string value: " << NYT::NodeToYsonString(value));
                info.ExplicitYson.emplace(value.AsString());
            }
        }

        if (inputSpec && AS_TYPE(TStructType, info.Type)->GetMembersCount() == 0) {
            auto fieldType = codecCtx.Builder.NewDataType(NUdf::EDataSlot::Bool);
            fieldType = codecCtx.Builder.NewOptionalType(fieldType);
            info.AuxColumns.emplace(YqlFakeColumnName, fieldType);
        }
    }
    else {
        info.Type = codecCtx.Builder.NewEmptyStructType();
        auto stringType = TDataType::Create(NUdf::TDataType<char*>::Id, codecCtx.Env);
        for (auto field: YAMR_FIELDS) {
            info.Type = codecCtx.Builder.NewStructType(info.Type, field, stringType);
        }
    }
    if (attrs.HasKey(YqlDynamicAttribute)) {
        info.Dynamic = attrs[YqlDynamicAttribute].AsBool();
    }
    if (attrs.HasKey(YqlSysColumnPrefix)) {
        for (auto& n: attrs[YqlSysColumnPrefix].AsList()) {
            const TString sys = n.AsString();
            const auto uniq = info.SysColumns.insert(sys).second;
            YQL_ENSURE(uniq, "Duplicate system column: " << sys);
            info.Type = codecCtx.Builder.NewStructType(info.Type, TString(YqlSysColumnPrefix).append(sys), TDataType::Create(GetSysColumnTypeId(sys), codecCtx.Env));
        }
    }
}

void TMkqlIOSpecs::InitDecoder(NCommon::TCodecContext& codecCtx,
    const TSpecInfo& specInfo,
    const THashMap<TString, ui32>& structColumns,
    const THashSet<TString>& auxColumns,
    TDecoderSpec& decoder
) {
    TStructType* inStruct = AS_TYPE(TStructType, specInfo.Type);
    for (auto& col: structColumns) {
        YQL_ENSURE(inStruct->FindMemberIndex(col.first) || specInfo.AuxColumns.contains(col.first), "Bad column " << col.first);
    }
    TStructTypeBuilder extendedStruct(codecCtx.Env);

    for (ui32 index = 0; index < inStruct->GetMembersCount(); ++index) {
        auto name = inStruct->GetMemberNameStr(index);
        if (structColumns.contains(name.Str())) {
            auto type = inStruct->GetMemberType(index);
            extendedStruct.Add(name.Str(), type);
        }
    }
    for (auto& aux: specInfo.AuxColumns) {
        if (structColumns.contains(aux.first)) {
            extendedStruct.Add(aux.first, aux.second);
        }
    }

    auto rowType = extendedStruct.Build();
    decoder.FieldsVec.resize(rowType->GetMembersCount());
    decoder.DefaultValues.resize(rowType->GetMembersCount());
    decoder.StructSize = structColumns.size();
    decoder.NativeYtTypeFlags = specInfo.NativeYtTypeFlags;
    decoder.Dynamic = specInfo.Dynamic;
    THashSet<ui32> virtualColumns;

    if (specInfo.SysColumns.contains("path")) {
        if (auto pos = rowType->FindMemberIndex(YqlSysColumnPath)) {
            virtualColumns.insert(*pos);
            decoder.FillSysColumnPath = pos;
        }
    }
    if (specInfo.SysColumns.contains("record")) {
        if (auto pos = rowType->FindMemberIndex(YqlSysColumnRecord)) {
            virtualColumns.insert(*pos);
            decoder.FillSysColumnRecord = pos;
        }
    }
    if (specInfo.SysColumns.contains("index")) {
        if (auto pos = rowType->FindMemberIndex(YqlSysColumnIndex)) {
            virtualColumns.insert(*pos);
            decoder.FillSysColumnIndex = pos;
        }
    }
    if (specInfo.SysColumns.contains("num")) {
        if (auto pos = rowType->FindMemberIndex(YqlSysColumnNum)) {
            virtualColumns.insert(*pos);
            decoder.FillSysColumnNum = pos;
        }
    }
    if (specInfo.SysColumns.contains("keyswitch")) {
        if (auto pos = rowType->FindMemberIndex(YqlSysColumnKeySwitch)) {
            virtualColumns.insert(*pos);
            decoder.FillSysColumnKeySwitch = pos;
        }
    }

    THashSet<ui32> usedPos;
    for (ui32 index = 0; index < rowType->GetMembersCount(); ++index) {
        auto name = rowType->GetMemberNameStr(index);
        auto ndx = structColumns.FindPtr(name.Str());
        YQL_ENSURE(ndx);
        ui32 pos = *ndx;

        YQL_ENSURE(usedPos.insert(pos).second, "Reused column position");

        auto fieldType = rowType->GetMemberType(index);
        TMkqlIOSpecs::TDecoderSpec::TDecodeField field;
        field.StructIndex = pos;
        field.Type = fieldType;
        field.Name = name.Str();
        field.Virtual = virtualColumns.contains(index);
        field.ExplicitYson = specInfo.ExplicitYson.contains(name.Str());
        decoder.FieldsVec[pos] = field;
        decoder.Fields.emplace(field.Name, field);
        auto it = specInfo.DefaultValues.find(name.Str());
        if (it != specInfo.DefaultValues.end()) {
            YQL_ENSURE(fieldType->GetKind() == TType::EKind::Data &&
                AS_TYPE(TDataType, fieldType)->GetSchemeType() == NUdf::TDataType<char*>::Id,
                "Default values are supported only for string fields");
            decoder.DefaultValues[pos] = MakeString(it->second);
        }

        if (!specInfo.StrictSchema && name.Str() == YqlOthersColumnName) {
            decoder.OthersStructIndex = pos;
            bool isTuple;
            bool encoded;
            bool useIHash;
            GetDictionaryKeyTypes(TDataType::Create(NUdf::TDataType<char*>::Id, codecCtx.Env),
                decoder.OthersKeyTypes, isTuple, encoded, useIHash);
        }
    }

    // Store all unused fields with Max<ui32>() position to correctly fill _other or skip aux
    if (decoder.OthersStructIndex || !auxColumns.empty()) {
        for (ui32 index = 0; index < inStruct->GetMembersCount(); ++index) {
            auto name = inStruct->GetMemberNameStr(index);
            if (!structColumns.contains(name.Str())) {
                if (decoder.OthersStructIndex || auxColumns.contains(name.Str())) {
                    TMkqlIOSpecs::TDecoderSpec::TDecodeField field;
                    field.StructIndex = Max<ui32>();
                    field.Name = name.Str();
                    field.Type = inStruct->GetMemberType(index);
                    decoder.FieldsVec.push_back(field);
                    decoder.Fields.emplace(name.Str(), field);
                    if (!specInfo.StrictSchema && name.Str() == YqlOthersColumnName && !decoder.OthersStructIndex) {
                        decoder.OthersStructIndex = Max<ui32>();
                        bool isTuple;
                        bool encoded;
                        bool useIHash;
                        GetDictionaryKeyTypes(TDataType::Create(NUdf::TDataType<char*>::Id, codecCtx.Env),
                            decoder.OthersKeyTypes, isTuple, encoded, useIHash);
                    }
                }
            }
        }
    }
    decoder.SkiffSize = decoder.FieldsVec.size();

    for (auto& col: specInfo.AuxColumns) {
        if (!structColumns.contains(col.first)) {
            TMkqlIOSpecs::TDecoderSpec::TDecodeField field;
            field.StructIndex = Max<ui32>();
            field.Name = col.first;
            field.Type = col.second;
            decoder.FieldsVec.push_back(field);
            auto res = decoder.Fields.emplace(col.first, field);
            YQL_ENSURE(res.second, "Aux column " << col.first << " already added");
        }
    }
}

void TMkqlIOSpecs::PrepareInput(const TVector<ui32>& inputGroups) {
    InputGroups = inputGroups;
    THashSet<ui32> groups;
    for (auto& x : InputGroups) {
        groups.emplace(x);
    }

    for (ui32 group = 0; group < groups.size(); ++group) {
        YQL_ENSURE(groups.contains(group), "Missing group: " << group);
    }
}

void TMkqlIOSpecs::InitInput(NCommon::TCodecContext& codecCtx,
    const NYT::TNode& inAttrs,
    const TVector<ui32>& inputGroups,
    const TVector<TString>& tableNames,
    TType* itemType,
    const TMaybe<TVector<TString>>& columns,
    const THashSet<TString>& auxColumns
) {
    PrepareInput(inputGroups);

    Y_ENSURE(inAttrs.IsMap(), "Expect Map type of input meta attrs, but got type " << inAttrs.GetType());
    Y_ENSURE(inAttrs.HasKey(YqlIOSpecTables), "Expect " << TString{YqlIOSpecTables}.Quote() << " key");

    InputSpec = inAttrs;

    TVariantType* itemVarType = nullptr;
    if (!InputGroups.empty()) {
        YQL_ENSURE(itemType, "Expect non-null item type");
        YQL_ENSURE(TType::EKind::Variant == itemType->GetKind(), "Expect Variant item type, but got " << itemType->GetKindAsStr());
        itemVarType = static_cast<TVariantType*>(itemType);
    }

    auto& inputSpecs = inAttrs[YqlIOSpecTables].AsList();
    Inputs.resize(inputSpecs.size());
    YQL_ENSURE(InputGroups.empty() || InputGroups.size() == Inputs.size());

    bool useCommonColumns = true;
    THashMap<TString, ui32> structColumns;
    if (columns.Defined()) {
        for (size_t i = 0; i < columns->size(); ++i) {
            structColumns.insert({columns->at(i), (ui32)i});
        }
    }
    else if (itemType && InputGroups.empty()) {
        TStructType* itemTypeStruct = AS_TYPE(TStructType, itemType);
        for (ui32 index = 0; index < itemTypeStruct->GetMembersCount(); ++index) {
            structColumns.emplace(itemTypeStruct->GetMemberName(index), index);
        }
    }
    else {
        useCommonColumns = false;
    }

    THashMap<TString, TSpecInfo> specInfoRegistry;

    for (size_t inputIndex = 0; inputIndex < inputSpecs.size(); ++inputIndex) {
        try {
            auto group = InputGroups.empty() ? 0 : inputGroups.at(inputIndex);
            TSpecInfo localSpecInfo;
            TSpecInfo* specInfo = &localSpecInfo;
            TString decoderRefName = TStringBuilder() << "_internal" << inputIndex;
            if (inputSpecs[inputIndex].IsString()) {
                auto refName = inputSpecs[inputIndex].AsString();
                decoderRefName = refName;
                if (auto p = specInfoRegistry.FindPtr(refName)) {
                    specInfo = p;
                } else {
                    Y_ENSURE(inAttrs.HasKey(YqlIOSpecRegistry) && inAttrs[YqlIOSpecRegistry].HasKey(refName), "Bad input registry reference: " << refName);
                    specInfo = &specInfoRegistry[refName];
                    LoadSpecInfo(true, inAttrs[YqlIOSpecRegistry][refName], codecCtx, *specInfo);
                }
            } else {
                LoadSpecInfo(true, inputSpecs[inputIndex], codecCtx, localSpecInfo);
            }

            TStructType* inStruct = AS_TYPE(TStructType, specInfo->Type);
            if (itemType) { // itemType may be null for operations without graph (TopSort f.e.)
                inStruct = itemVarType
                    ? AS_TYPE(TStructType, itemVarType->GetAlternativeType(group))
                    : AS_TYPE(TStructType, itemType);
            }

            if (!useCommonColumns) {
                structColumns.clear();
                for (ui32 index = 0; index < inStruct->GetMembersCount(); ++index) {
                    auto col = inStruct->GetMemberName(index);
                    structColumns.emplace(col, index);
                    decoderRefName.append(';').append(ToString(col.length())).append(':').append(col); // Make decoder unique by input type and set of used columns
                }
            }
            if (specInfo->Dynamic) {
                decoderRefName.append(";dynamic");
            }

            if (auto p = Decoders.FindPtr(decoderRefName)) {
                // Reuse already initialized decoder for the same schema and column set
                Inputs[inputIndex] = p;
            }
            else {
                TDecoderSpec* decoder = &Decoders[decoderRefName];
                InitDecoder(codecCtx, *specInfo, structColumns, auxColumns, *decoder);
                Inputs[inputIndex] = decoder;
            }
        }
        catch (const yexception& e) {
            ythrow yexception() << "Invalid decoder spec for " << inputIndex << " input: " << e;
        }
    }

    if (!tableNames.empty()) {
        YQL_ENSURE(tableNames.size() == Inputs.size());
        for (auto& name: tableNames) {
            TableNames.push_back(MakeString(name));
        }
    } else {
        TableNames.resize(Inputs.size(), NUdf::TUnboxedValuePod::Zero());
    }
    TableOffsets.resize(Inputs.size(), 0ull);
}

void TMkqlIOSpecs::InitOutput(NCommon::TCodecContext& codecCtx, const NYT::TNode& outAttrs) {
    Y_ENSURE(outAttrs.IsMap(), "Expect Map type of output meta attrs, but got type " << outAttrs.GetType());
    Y_ENSURE(outAttrs.HasKey(YqlIOSpecTables), "Expect " << TString{YqlIOSpecTables}.Quote() << " key");

    OutputSpec = outAttrs;

    auto& outputSpecs = outAttrs[YqlIOSpecTables].AsList();
    Outputs.resize(outputSpecs.size());

    THashMap<TString, TSpecInfo> specInfoRegistry;
    THashMap<TString, TStructType*> outTypeRegistry;

    for (size_t i = 0; i < outputSpecs.size(); ++i) {
        try {
            TSpecInfo localSpecInfo;
            TSpecInfo* specInfo = &localSpecInfo;
            TString refName;
            if (outputSpecs[i].IsString()) {
                refName = outputSpecs[i].AsString();

                if (auto p = specInfoRegistry.FindPtr(refName)) {
                    specInfo = p;
                }
                else {
                    Y_ENSURE(outAttrs.HasKey(YqlIOSpecRegistry) && outAttrs[YqlIOSpecRegistry].HasKey(refName), "Bad output registry reference: " << refName);
                    specInfo = &specInfoRegistry[refName];
                    LoadSpecInfo(false, outAttrs[YqlIOSpecRegistry][refName], codecCtx, *specInfo);
                }

                if (auto p = outTypeRegistry.FindPtr(refName)) {
                    Outputs[i].RowType = *p;
                    Outputs[i].NativeYtTypeFlags = specInfo->NativeYtTypeFlags;
                    continue;
                }
            } else {
                LoadSpecInfo(false, outputSpecs[i], codecCtx, localSpecInfo);
            }

            auto structType = AS_TYPE(TStructType, specInfo->Type);
            // Extend struct by aux columns
            for (auto& col: specInfo->AuxColumns) {
                structType = AS_TYPE(TStructType, codecCtx.Builder.NewStructType(structType, col.first, col.second));
            }
            if (refName) {
                outTypeRegistry[refName] = structType;
            }

            Outputs[i].RowType = structType;
            Outputs[i].NativeYtTypeFlags = specInfo->NativeYtTypeFlags;
        } catch (const yexception& e) {
            ythrow yexception() << "Invalid encoder spec for " << i << " output: " << e;
        }
    }
}

NYT::TFormat TMkqlIOSpecs::MakeOutputFormat() const {
    if (!UseSkiff_ || Outputs.empty()) {
        return NYT::TFormat::YsonBinary();
    }

    YQL_ENSURE(!OutputSpec.IsUndefined());
    NYT::TNode formatConfig = TablesSpecToOutputSkiff(OutputSpec);
    return NYT::TFormat(formatConfig);
}

NYT::TFormat TMkqlIOSpecs::MakeOutputFormat(size_t tableIndex) const {
    Y_ENSURE(tableIndex < Outputs.size(), "Invalid output table index: " << tableIndex);

    if (!UseSkiff_) {
        return NYT::TFormat::YsonBinary();
    }

    YQL_ENSURE(!OutputSpec.IsUndefined());
    NYT::TNode formatConfig = SingleTableSpecToOutputSkiff(OutputSpec, tableIndex);
    return NYT::TFormat(formatConfig);
}

NYT::TFormat TMkqlIOSpecs::MakeInputFormat(const THashSet<TString>& auxColumns) const {
    if (!UseSkiff_ || Inputs.empty()) {
        return NYT::TFormat::YsonBinary();
    }

    NYT::TNode formatConfig = NYT::TNode("skiff");
    auto& skiffConfig = formatConfig.Attributes();
    auto schemas = NYT::TNode::CreateList();
    THashMap<const TDecoderSpec*, size_t> uniqDecoders;
    for (size_t i = 0; i < Inputs.size(); ++i) {
        auto input = Inputs[i];
        size_t schemaId = uniqDecoders.size();
        auto p = uniqDecoders.emplace(input, schemaId);
        if (p.second) {
            THashMap<TString, ui32> structColumns;
            for (size_t f = 0; f < input->StructSize; ++f) {
                structColumns[input->FieldsVec[f].Name] = f;
            }

            NYT::TNode tableSchema = SingleTableSpecToInputSkiffSchema(InputSpec, i, structColumns, auxColumns,
                SystemFields_.HasFlags(ESystemField::RowIndex),
                SystemFields_.HasFlags(ESystemField::RangeIndex),
                SystemFields_.HasFlags(ESystemField::KeySwitch));
            skiffConfig["skiff_schema_registry"][TStringBuilder() << "table" << schemaId] = tableSchema;
        }
        else {
            schemaId = p.first->second;
        }

        schemas.Add(NYT::TNode(TStringBuilder() << "$table" << schemaId));
    }

    skiffConfig["table_skiff_schemas"] = schemas;
    //Cerr << NYT::NodeToYsonString(skiffConfig) << Endl;
    return NYT::TFormat(formatConfig);
}

NYT::TFormat TMkqlIOSpecs::MakeInputFormat(size_t tableIndex) const {
    Y_ENSURE(tableIndex < Inputs.size(), "Invalid output table index: " << tableIndex);

    if (!UseSkiff_) {
        return NYT::TFormat::YsonBinary();
    }

    NYT::TNode formatConfig = NYT::TNode("skiff");

    auto input = Inputs[tableIndex];
    THashMap<TString, ui32> structColumns;
    for (size_t f = 0; f < input->StructSize; ++f) {
        structColumns[input->FieldsVec[f].Name] = f;
    }

    NYT::TNode tableSchema = SingleTableSpecToInputSkiffSchema(InputSpec, tableIndex, structColumns, {},
        SystemFields_.HasFlags(ESystemField::RowIndex),
        SystemFields_.HasFlags(ESystemField::RangeIndex),
        SystemFields_.HasFlags(ESystemField::KeySwitch));

    auto& skiffConfig = formatConfig.Attributes();
    skiffConfig["table_skiff_schemas"] = NYT::TNode::CreateList().Add(std::move(tableSchema));

    return NYT::TFormat(formatConfig);
}

TMkqlIOCache::TMkqlIOCache(const TMkqlIOSpecs& specs, const THolderFactory& holderFactory)
    : Specs_(specs)
    , HolderFactory(holderFactory)
{
    THashSet<ui32> groups;
    for (auto& x : Specs_.InputGroups) {
        groups.emplace(x);
    }

    for (ui32 i = 0; i < Max<ui32>(1, groups.size()); ++i) {
        RowCache_.emplace_back(MakeHolder<TPlainContainerCache>());
    }

    DecoderCache_.resize(Specs_.Inputs.size());
    for (size_t i: xrange(Specs_.Inputs.size())) {
        DecoderCache_[i].LastFields_.reserve(Specs_.Inputs[i]->FieldsVec.size());
        for (auto& field: Specs_.Inputs[i]->FieldsVec) {
            DecoderCache_[i].LastFields_.push_back(&field);
        }
    }
}

} // NYql
