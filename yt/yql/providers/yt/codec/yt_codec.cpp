#include "yt_codec.h"

#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <yt/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>
#include <yql/essentials/providers/common/schema/parser/yql_type_parser.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/swap_bytes.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/public/result_format/yql_codec_results.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/decimal/yql_decimal_serialize.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

#include <yt/yt/library/decimal/decimal.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/detail.h>

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
using namespace NYson::NDetail;

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

    if (InputBlockRepresentation_ == EBlockRepresentation::BlockStruct) {
        if (auto pos = rowType->FindMemberIndex(BlockLengthColumnName)) {
            virtualColumns.insert(*pos);
            decoder.FillBlockStructSize = pos;
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
        TColumnOrder order(*columns);
        for (size_t i = 0; i < columns->size(); ++i) {
            structColumns.insert({order.at(i).PhysicalName, (ui32)i});
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
            bool newSpec = false;
            if (inputSpecs[inputIndex].IsString()) {
                auto refName = inputSpecs[inputIndex].AsString();
                decoderRefName = refName;
                if (auto p = specInfoRegistry.FindPtr(refName)) {
                    specInfo = p;
                } else {
                    Y_ENSURE(inAttrs.HasKey(YqlIOSpecRegistry) && inAttrs[YqlIOSpecRegistry].HasKey(refName), "Bad input registry reference: " << refName);
                    specInfo = &specInfoRegistry[refName];
                    LoadSpecInfo(true, inAttrs[YqlIOSpecRegistry][refName], codecCtx, *specInfo);
                    newSpec = true;
                }
            } else {
                LoadSpecInfo(true, inputSpecs[inputIndex], codecCtx, localSpecInfo);
                newSpec = true;
            }
            if (InputBlockRepresentation_ == EBlockRepresentation::BlockStruct && newSpec) {
                specInfo->Type = codecCtx.Builder.NewStructType(specInfo->Type, BlockLengthColumnName, TDataType::Create(NUdf::TDataType<ui64>::Id, codecCtx.Env));
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
    if (UseBlockOutput_) {
        return NYT::TFormat(NYT::TNode("arrow"));
    }

    if (!UseSkiff_ || Outputs.empty()) {
        return NYT::TFormat::YsonBinary();
    }

    YQL_ENSURE(!OutputSpec.IsUndefined());
    NYT::TNode formatConfig = TablesSpecToOutputSkiff(OutputSpec);
    return NYT::TFormat(formatConfig);
}

NYT::TFormat TMkqlIOSpecs::MakeOutputFormat(size_t tableIndex) const {
    Y_ENSURE(tableIndex < Outputs.size(), "Invalid output table index: " << tableIndex);

    if (UseBlockOutput_) {
        YQL_ENSURE(tableIndex == 0);
        return NYT::TFormat(NYT::TNode("arrow"));
    }

    if (!UseSkiff_) {
        return NYT::TFormat::YsonBinary();
    }

    YQL_ENSURE(!OutputSpec.IsUndefined());
    NYT::TNode formatConfig = SingleTableSpecToOutputSkiff(OutputSpec, tableIndex);
    return NYT::TFormat(formatConfig);
}

NYT::TFormat TMkqlIOSpecs::MakeInputFormat(const THashSet<TString>& auxColumns) const {
    if (UseBlockInput_) {
        YQL_ENSURE(auxColumns.empty());
        return NYT::TFormat(NYT::TNode("arrow"));
    }

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

    if (UseBlockInput_) {
        YQL_ENSURE(tableIndex == 0);
        return NYT::TFormat(NYT::TNode("arrow"));
    }

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

template <typename T>
T ReadYsonFloatNumberInTableFormat(char cmd, NCommon::TInputBuf& buf) {
    CHECK_EXPECTED(cmd, DoubleMarker);
    double dbl;
    buf.ReadMany((char*)&dbl, sizeof(dbl));
    return dbl;
}

NUdf::TUnboxedValue ReadYsonValueInTableFormat(TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, char cmd, NCommon::TInputBuf& buf) {
    switch (type->GetKind()) {
    case TType::EKind::Variant: {
        auto varType = static_cast<TVariantType*>(type);
        auto underlyingType = varType->GetUnderlyingType();
        if (nativeYtTypeFlags & NTCF_COMPLEX) {
            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = buf.Read();
            TType* type = nullptr;
            i64 index = 0;
            if (cmd == StringMarker) {
                YQL_ENSURE(underlyingType->IsStruct(), "Expected struct as underlying type");
                auto structType = static_cast<TStructType*>(underlyingType);
                auto nameBuffer = ReadNextString(cmd, buf);
                auto foundIndex = structType->FindMemberIndex(nameBuffer);
                YQL_ENSURE(foundIndex.Defined(), "Unexpected member: " << nameBuffer);
                index = *foundIndex;
                type = varType->GetAlternativeType(index);
            } else {
                YQL_ENSURE(cmd == Int64Marker || cmd == Uint64Marker);
                YQL_ENSURE(underlyingType->IsTuple(), "Expected tuple as underlying type");
                if (cmd == Uint64Marker) {
                    index = buf.ReadVarUI64();
                } else {
                    index = buf.ReadVarI64();
                }
                YQL_ENSURE(0 <= index && index < varType->GetAlternativesCount(), "Unexpected member index: " << index);
                type = varType->GetAlternativeType(index);
            }
            cmd = buf.Read();
            CHECK_EXPECTED(cmd, ListItemSeparatorSymbol);
            cmd = buf.Read();
            auto value = ReadYsonValueInTableFormat(type, nativeYtTypeFlags, holderFactory, cmd, buf);
            cmd = buf.Read();
            if (cmd != EndListSymbol) {
                CHECK_EXPECTED(cmd, ListItemSeparatorSymbol);
                cmd = buf.Read();
                CHECK_EXPECTED(cmd, EndListSymbol);
            }
            return holderFactory.CreateVariantHolder(value.Release(), index);
        } else {
            if (cmd == StringMarker) {
                YQL_ENSURE(underlyingType->IsStruct(), "Expected struct as underlying type");
                auto name = ReadNextString(cmd, buf);
                auto index = static_cast<TStructType*>(underlyingType)->FindMemberIndex(name);
                YQL_ENSURE(index, "Unexpected member: " << name);
                YQL_ENSURE(static_cast<TStructType*>(underlyingType)->GetMemberType(*index)->IsVoid(), "Expected Void as underlying type");
                return holderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod::Zero(), *index);
            }

            CHECK_EXPECTED(cmd, BeginListSymbol);
            cmd = buf.Read();
            i64 index = 0;
            YQL_ENSURE(cmd == Int64Marker || cmd == Uint64Marker);
            if (cmd == Uint64Marker) {
                index = buf.ReadVarUI64();
            } else {
                index = buf.ReadVarI64();
            }

            YQL_ENSURE(index < varType->GetAlternativesCount(), "Bad variant alternative: " << index << ", only " <<
                varType->GetAlternativesCount() << " are available");
            YQL_ENSURE(underlyingType->IsTuple() || underlyingType->IsStruct(), "Wrong underlying type");
            TType* itemType;
            if (underlyingType->IsTuple()) {
                itemType = static_cast<TTupleType*>(underlyingType)->GetElementType(index);
            }
            else {
                itemType = static_cast<TStructType*>(underlyingType)->GetMemberType(index);
            }

            EXPECTED(buf, ListItemSeparatorSymbol);
            cmd = buf.Read();
            auto value = ReadYsonValueInTableFormat(itemType, nativeYtTypeFlags, holderFactory, cmd, buf);
            cmd = buf.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = buf.Read();
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            return holderFactory.CreateVariantHolder(value.Release(), index);
        }
    }

    case TType::EKind::Data: {
        auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
        switch (schemeType) {
        case NUdf::TDataType<bool>::Id:
            YQL_ENSURE(cmd == FalseMarker || cmd == TrueMarker, "Expected either true or false, but got: " << TString(cmd).Quote());
            return NUdf::TUnboxedValuePod(cmd == TrueMarker);

        case NUdf::TDataType<ui8>::Id:
            CHECK_EXPECTED(cmd, Uint64Marker);
            return NUdf::TUnboxedValuePod(ui8(buf.ReadVarUI64()));

        case NUdf::TDataType<i8>::Id:
            CHECK_EXPECTED(cmd, Int64Marker);
            return NUdf::TUnboxedValuePod(i8(buf.ReadVarI64()));

        case NUdf::TDataType<ui16>::Id:
            CHECK_EXPECTED(cmd, Uint64Marker);
            return NUdf::TUnboxedValuePod(ui16(buf.ReadVarUI64()));

        case NUdf::TDataType<i16>::Id:
            CHECK_EXPECTED(cmd, Int64Marker);
            return NUdf::TUnboxedValuePod(i16(buf.ReadVarI64()));

        case NUdf::TDataType<i32>::Id:
            CHECK_EXPECTED(cmd, Int64Marker);
            return NUdf::TUnboxedValuePod(i32(buf.ReadVarI64()));

        case NUdf::TDataType<ui32>::Id:
            CHECK_EXPECTED(cmd, Uint64Marker);
            return NUdf::TUnboxedValuePod(ui32(buf.ReadVarUI64()));

        case NUdf::TDataType<i64>::Id:
            CHECK_EXPECTED(cmd, Int64Marker);
            return NUdf::TUnboxedValuePod(buf.ReadVarI64());

        case NUdf::TDataType<ui64>::Id:
            CHECK_EXPECTED(cmd, Uint64Marker);
            return NUdf::TUnboxedValuePod(buf.ReadVarUI64());

        case NUdf::TDataType<float>::Id:
            return NUdf::TUnboxedValuePod(ReadYsonFloatNumberInTableFormat<float>(cmd, buf));

        case NUdf::TDataType<double>::Id:
            return NUdf::TUnboxedValuePod(ReadYsonFloatNumberInTableFormat<double>(cmd, buf));

        case NUdf::TDataType<NUdf::TUtf8>::Id:
        case NUdf::TDataType<char*>::Id:
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TDyNumber>::Id:
        case NUdf::TDataType<NUdf::TUuid>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(nextString)));
        }

        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            if (nativeYtTypeFlags & NTCF_DECIMAL) {
                auto const params = static_cast<TDataDecimalType*>(type)->GetParams();
                if (params.first < 10) {
                    // The YQL format differs from the YT format in the inf/nan values. NDecimal::FromYtDecimal converts nan/inf
                    NDecimal::TInt128 res = NDecimal::FromYtDecimal(NYT::NDecimal::TDecimal::ParseBinary32(params.first, nextString));
                    YQL_ENSURE(!NDecimal::IsError(res));
                    return NUdf::TUnboxedValuePod(res);
                } else if (params.first < 19) {
                    NDecimal::TInt128 res = NDecimal::FromYtDecimal(NYT::NDecimal::TDecimal::ParseBinary64(params.first, nextString));
                    YQL_ENSURE(!NDecimal::IsError(res));
                    return NUdf::TUnboxedValuePod(res);
                } else {
                    YQL_ENSURE(params.first < 36);
                    NYT::NDecimal::TDecimal::TValue128 tmpRes = NYT::NDecimal::TDecimal::ParseBinary128(params.first, nextString);
                    NDecimal::TInt128 res;
                    static_assert(sizeof(NDecimal::TInt128) == sizeof(NYT::NDecimal::TDecimal::TValue128));
                    memcpy(&res, &tmpRes, sizeof(NDecimal::TInt128));
                    res = NDecimal::FromYtDecimal(res);
                    YQL_ENSURE(!NDecimal::IsError(res));
                    return NUdf::TUnboxedValuePod(res);
                }
            }
            else {
                const auto& des = NDecimal::Deserialize(nextString.data(), nextString.size());
                YQL_ENSURE(!NDecimal::IsError(des.first));
                YQL_ENSURE(nextString.size() == des.second);
                return NUdf::TUnboxedValuePod(des.first);
            }
        }

        case NUdf::TDataType<NUdf::TYson>::Id: {
            auto& yson = buf.YsonBuffer();
            yson.clear();
            CopyYsonWithAttrs(cmd, buf, yson);

            return NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(yson)));
        }

        case NUdf::TDataType<NUdf::TDate>::Id:
            CHECK_EXPECTED(cmd, Uint64Marker);
            return NUdf::TUnboxedValuePod((ui16)buf.ReadVarUI64());

        case NUdf::TDataType<NUdf::TDatetime>::Id:
            CHECK_EXPECTED(cmd, Uint64Marker);
            return NUdf::TUnboxedValuePod((ui32)buf.ReadVarUI64());

        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            CHECK_EXPECTED(cmd, Uint64Marker);
            return NUdf::TUnboxedValuePod(buf.ReadVarUI64());

        case NUdf::TDataType<NUdf::TInterval>::Id:
            CHECK_EXPECTED(cmd, Int64Marker);
            return NUdf::TUnboxedValuePod(buf.ReadVarI64());

        case NUdf::TDataType<NUdf::TTzDate>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            NUdf::TUnboxedValuePod data;
            ui16 value;
            ui16 tzId = 0;
            YQL_ENSURE(DeserializeTzDate(nextString, value, tzId));
            data = NUdf::TUnboxedValuePod(value);
            data.SetTimezoneId(tzId);
            return data;
        }

        case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            NUdf::TUnboxedValuePod data;
            ui32 value;
            ui16 tzId = 0;
            YQL_ENSURE(DeserializeTzDatetime(nextString, value, tzId));
            data = NUdf::TUnboxedValuePod(value);
            data.SetTimezoneId(tzId);
            return data;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            NUdf::TUnboxedValuePod data;
            ui64 value;
            ui16 tzId = 0;
            YQL_ENSURE(DeserializeTzTimestamp(nextString, value, tzId));
            data = NUdf::TUnboxedValuePod(value);
            data.SetTimezoneId(tzId);
            return data;
        }

        case NUdf::TDataType<NUdf::TDate32>::Id:
            CHECK_EXPECTED(cmd, Int64Marker);
            return NUdf::TUnboxedValuePod((i32)buf.ReadVarI64());

        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
            CHECK_EXPECTED(cmd, Int64Marker);
            return NUdf::TUnboxedValuePod(buf.ReadVarI64());

        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            return ValueFromString(EDataSlot::JsonDocument, ReadNextString(cmd, buf));
        }

        case NUdf::TDataType<NUdf::TTzDate32>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            NUdf::TUnboxedValuePod data;
            i32 value;
            ui16 tzId = 0;
            YQL_ENSURE(DeserializeTzDate32(nextString, value, tzId));
            data = NUdf::TUnboxedValuePod(value);
            data.SetTimezoneId(tzId);
            return data;
        }

        case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            NUdf::TUnboxedValuePod data;
            i64 value;
            ui16 tzId = 0;
            YQL_ENSURE(DeserializeTzDatetime64(nextString, value, tzId));
            data = NUdf::TUnboxedValuePod(value);
            data.SetTimezoneId(tzId);
            return data;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            auto nextString = ReadNextString(cmd, buf);
            NUdf::TUnboxedValuePod data;
            i64 value;
            ui16 tzId = 0;
            YQL_ENSURE(DeserializeTzTimestamp64(nextString, value, tzId));
            data = NUdf::TUnboxedValuePod(value);
            data.SetTimezoneId(tzId);
            return data;
        }

        default:
            YQL_ENSURE(false, "Unsupported data type: " << schemeType);
        }
    }

    case TType::EKind::Struct: {
        YQL_ENSURE(cmd == BeginListSymbol || cmd == BeginMapSymbol);
        auto structType = static_cast<TStructType*>(type);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue ret = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), items);
        if (cmd == BeginListSymbol) {
            cmd = buf.Read();

            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                items[i] = ReadYsonValueInTableFormat(structType->GetMemberType(i), nativeYtTypeFlags, holderFactory, cmd, buf);

                cmd = buf.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = buf.Read();
                }
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            return ret;
        } else {
            cmd = buf.Read();

            for (;;) {
                if (cmd == EndMapSymbol) {
                    break;
                }

                auto keyBuffer = ReadNextString(cmd, buf);
                auto pos = structType->FindMemberIndex(keyBuffer);
                EXPECTED(buf, KeyValueSeparatorSymbol);
                cmd = buf.Read();
                if (pos && cmd != '#') {
                    auto memberType = structType->GetMemberType(*pos);
                    auto unwrappedType = memberType;
                    if (!(nativeYtTypeFlags & ENativeTypeCompatFlags::NTCF_COMPLEX) && unwrappedType->IsOptional()) {
                        unwrappedType = static_cast<TOptionalType*>(unwrappedType)->GetItemType();
                    }

                    items[*pos] = ReadYsonValueInTableFormat(unwrappedType, nativeYtTypeFlags, holderFactory, cmd, buf);
                } else {
                    SkipYson(cmd, buf);
                }

                cmd = buf.Read();
                if (cmd == KeyedItemSeparatorSymbol) {
                    cmd = buf.Read();
                }
            }

            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                if (items[i]) {
                    continue;
                }

                YQL_ENSURE(structType->GetMemberType(i)->IsOptional(), "Missing required field: " << structType->GetMemberName(i));
            }

            return ret;
        }
    }

    case TType::EKind::List: {
        auto itemType = static_cast<TListType*>(type)->GetItemType();
        TDefaultListRepresentation items;
        CHECK_EXPECTED(cmd, BeginListSymbol);
        cmd = buf.Read();

        for (;;) {
            if (cmd == EndListSymbol) {
                break;
            }

            items = items.Append(ReadYsonValueInTableFormat(itemType, nativeYtTypeFlags, holderFactory, cmd, buf));

            cmd = buf.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = buf.Read();
            }
        }

        return holderFactory.CreateDirectListHolder(std::move(items));
    }

    case TType::EKind::Optional: {
        if (cmd == EntitySymbol) {
            return NUdf::TUnboxedValuePod();
        }
        auto itemType = static_cast<TOptionalType*>(type)->GetItemType();
        if (nativeYtTypeFlags & ENativeTypeCompatFlags::NTCF_COMPLEX) {
            if (itemType->GetKind() == TType::EKind::Optional || itemType->GetKind() == TType::EKind::Pg) {
                CHECK_EXPECTED(cmd, BeginListSymbol);
                cmd = buf.Read();
                auto value = ReadYsonValueInTableFormat(itemType, nativeYtTypeFlags, holderFactory, cmd, buf);
                cmd = buf.Read();
                if (cmd == ListItemSeparatorSymbol) {
                    cmd = buf.Read();
                }
                CHECK_EXPECTED(cmd, EndListSymbol);
                return value.Release().MakeOptional();
            } else {
                return ReadYsonValueInTableFormat(itemType, nativeYtTypeFlags, holderFactory, cmd, buf).Release().MakeOptional();
            }
        } else {
            if (cmd != BeginListSymbol) {
                auto value = ReadYsonValueInTableFormat(itemType, nativeYtTypeFlags, holderFactory, cmd, buf);
                return value.Release().MakeOptional();
            }

            cmd = buf.Read();
            if (cmd == EndListSymbol) {
                return NUdf::TUnboxedValuePod();
            }

            auto value = ReadYsonValueInTableFormat(itemType, nativeYtTypeFlags, holderFactory, cmd, buf);
            cmd = buf.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = buf.Read();
            }

            CHECK_EXPECTED(cmd, EndListSymbol);
            return value.Release().MakeOptional();
        }
    }

    case TType::EKind::Dict: {
        auto dictType = static_cast<TDictType*>(type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        TKeyTypes types;
        bool isTuple;
        bool encoded;
        bool useIHash;
        GetDictionaryKeyTypes(keyType, types, isTuple, encoded, useIHash);

        TMaybe<TValuePacker> packer;
        if (encoded) {
            packer.ConstructInPlace(true, keyType);
        }

        YQL_ENSURE(cmd == BeginListSymbol || cmd == BeginMapSymbol, "Expected '{' or '[', but read: " << TString(cmd).Quote());
        if (cmd == BeginMapSymbol) {
            bool unusedIsOptional;
            auto unpackedType = UnpackOptional(keyType, unusedIsOptional);
            YQL_ENSURE(unpackedType->IsData() &&
                (static_cast<TDataType*>(unpackedType)->GetSchemeType() == NUdf::TDataType<char*>::Id ||
                static_cast<TDataType*>(unpackedType)->GetSchemeType() == NUdf::TDataType<NUdf::TUtf8>::Id),
                "Expected String or Utf8 type as dictionary key type");

            auto filler = [&](TValuesDictHashMap& map) {
                cmd = buf.Read();

                for (;;) {
                    if (cmd == EndMapSymbol) {
                        break;
                    }

                    auto keyBuffer = ReadNextString(cmd, buf);
                    auto keyStr = NUdf::TUnboxedValue(MakeString(keyBuffer));
                    EXPECTED(buf, KeyValueSeparatorSymbol);
                    cmd = buf.Read();
                    auto payload = ReadYsonValueInTableFormat(payloadType, nativeYtTypeFlags, holderFactory, cmd, buf);
                    map.emplace(std::move(keyStr), std::move(payload));

                    cmd = buf.Read();
                    if (cmd == KeyedItemSeparatorSymbol) {
                        cmd = buf.Read();
                    }
                }
            };

            const NUdf::IHash* hash = holderFactory.GetHash(*keyType, useIHash);
            const NUdf::IEquate* equate = holderFactory.GetEquate(*keyType, useIHash);
            return holderFactory.CreateDirectHashedDictHolder(filler, types, isTuple, true, nullptr, hash, equate);
        }
        else {
            auto filler = [&](TValuesDictHashMap& map) {
                cmd = buf.Read();

                for (;;) {
                    if (cmd == EndListSymbol) {
                        break;
                    }

                    CHECK_EXPECTED(cmd, BeginListSymbol);
                    cmd = buf.Read();
                    auto key = ReadYsonValueInTableFormat(keyType, nativeYtTypeFlags, holderFactory, cmd, buf);
                    EXPECTED(buf, ListItemSeparatorSymbol);
                    cmd = buf.Read();
                    auto payload = ReadYsonValueInTableFormat(payloadType, nativeYtTypeFlags, holderFactory, cmd, buf);
                    cmd = buf.Read();
                    if (cmd == ListItemSeparatorSymbol) {
                        cmd = buf.Read();
                    }

                    CHECK_EXPECTED(cmd, EndListSymbol);
                    if (packer) {
                        key = MakeString(packer->Pack(key));
                    }

                    map.emplace(std::move(key), std::move(payload));

                    cmd = buf.Read();
                    if (cmd == ListItemSeparatorSymbol) {
                        cmd = buf.Read();
                    }
                }
            };

            const NUdf::IHash* hash = holderFactory.GetHash(*keyType, useIHash);
            const NUdf::IEquate* equate = holderFactory.GetEquate(*keyType, useIHash);
            return holderFactory.CreateDirectHashedDictHolder(filler, types, isTuple, true, encoded ? keyType : nullptr,
                hash, equate);
        }
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<TTupleType*>(type);
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue ret = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), items);
        CHECK_EXPECTED(cmd, BeginListSymbol);
        cmd = buf.Read();

        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            items[i] = ReadYsonValueInTableFormat(tupleType->GetElementType(i), nativeYtTypeFlags, holderFactory, cmd, buf);

            cmd = buf.Read();
            if (cmd == ListItemSeparatorSymbol) {
                cmd = buf.Read();
            }
        }


        CHECK_EXPECTED(cmd, EndListSymbol);
        return ret;
    }

    case TType::EKind::Void: {
        if (cmd == EntitySymbol) {
            return NUdf::TUnboxedValuePod::Void();
        }

        auto nextString = ReadNextString(cmd, buf);
        YQL_ENSURE(nextString == NResult::TYsonResultWriter::VoidString, "Expected Void");
        return NUdf::TUnboxedValuePod::Void();
    }

    case TType::EKind::Null: {
        CHECK_EXPECTED(cmd, EntitySymbol);
        return NUdf::TUnboxedValuePod();
    }

    case TType::EKind::EmptyList: {
        CHECK_EXPECTED(cmd, BeginListSymbol);
        cmd = buf.Read();
        CHECK_EXPECTED(cmd, EndListSymbol);
        return holderFactory.GetEmptyContainerLazy();
    }

    case TType::EKind::EmptyDict: {
        YQL_ENSURE(cmd == BeginListSymbol || cmd == BeginMapSymbol, "Expected '{' or '[', but read: " << TString(cmd).Quote());
        if (cmd == BeginListSymbol) {
            cmd = buf.Read();
            CHECK_EXPECTED(cmd, EndListSymbol);
        } else {
            cmd = buf.Read();
            CHECK_EXPECTED(cmd, EndMapSymbol);
        }

        return holderFactory.GetEmptyContainerLazy();
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<TPgType*>(type);
        return ReadYsonValueInTableFormatPg(pgType, cmd, buf);
    }

    case TType::EKind::Tagged: {
        auto taggedType = static_cast<TTaggedType*>(type);
        return ReadYsonValueInTableFormat(taggedType->GetBaseType(), nativeYtTypeFlags, holderFactory, cmd, buf);
    }

    default:
        YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
}

TMaybe<NUdf::TUnboxedValue> ParseYsonValueInTableFormat(const THolderFactory& holderFactory,
    const TStringBuf& yson, TType* type, ui64 nativeYtTypeFlags, IOutputStream* err) {
    try {
        class TReader : public NCommon::IBlockReader {
        public:
            TReader(const TStringBuf& yson)
                : Yson_(yson)
            {}

            void SetDeadline(TInstant deadline) override {
                Y_UNUSED(deadline);
            }

            std::pair<const char*, const char*> NextFilledBlock() override {
                if (FirstBuffer_) {
                    FirstBuffer_ = false;
                    return{ Yson_.begin(), Yson_.end() };
                }
                else {
                    return{ nullptr, nullptr };
                }
            }

            void ReturnBlock() override {
            }

            bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) override {
                Y_UNUSED(rangeIndex);
                Y_UNUSED(rowIndex);
                Y_UNUSED(error);
                return false;
            }

        private:
            TStringBuf Yson_;
            bool FirstBuffer_ = true;
        };

        TReader reader(yson);
        NCommon::TInputBuf buf(reader, nullptr);
        char cmd = buf.Read();
        return ReadYsonValueInTableFormat(type, nativeYtTypeFlags, holderFactory, cmd, buf);
    }
    catch (const yexception& e) {
        if (err) {
            *err << "YSON parsing failed: " << e.what();
        }
        return Nothing();
    }
}

TMaybe<NUdf::TUnboxedValue> ParseYsonNode(const THolderFactory& holderFactory,
    const NYT::TNode& node, TType* type, ui64 nativeYtTypeFlags, IOutputStream* err) {
    return ParseYsonValueInTableFormat(holderFactory, NYT::NodeToYsonString(node, NYson::EYsonFormat::Binary), type, nativeYtTypeFlags, err);
}

extern "C" void ReadYsonContainerValue(TType* type, ui64 nativeYtTypeFlags, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf, bool wrapOptional) {
    // yson content
    ui32 size;
    buf.ReadMany((char*)&size, sizeof(size));
    CHECK_STRING_LENGTH_UNSIGNED(size);
    // parse binary yson...
    YQL_ENSURE(size > 0);
    char cmd = buf.Read();
    auto tmp = ReadYsonValueInTableFormat(type, nativeYtTypeFlags, holderFactory, cmd, buf);
    if (!wrapOptional) {
        value = std::move(tmp);
    }
    else {
        value = tmp.Release().MakeOptional();
    }
}

extern "C" void ReadContainerNativeYtValue(TType* type, ui64 nativeYtTypeFlags, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf, bool wrapOptional) {
    auto tmp = ReadSkiffNativeYtValue(type, nativeYtTypeFlags, holderFactory, buf);
    if (!wrapOptional) {
        value = std::move(tmp);
    } else {
        value = tmp.Release().MakeOptional();
    }
}

void WriteYsonValueInTableFormat(NCommon::TOutputBuf& buf, TType* type, ui64 nativeYtTypeFlags, const NUdf::TUnboxedValuePod& value, bool topLevel) {
    // Table format, very compact
    switch (type->GetKind()) {
    case TType::EKind::Variant: {
        buf.Write(BeginListSymbol);
        auto varType = static_cast<TVariantType*>(type);
        auto underlyingType = varType->GetUnderlyingType();
        auto index = value.GetVariantIndex();
        YQL_ENSURE(index < varType->GetAlternativesCount(), "Bad variant alternative: " << index << ", only " << varType->GetAlternativesCount() << " are available");
        YQL_ENSURE(underlyingType->IsTuple() || underlyingType->IsStruct(), "Wrong underlying type");
        TType* itemType;
        if (underlyingType->IsTuple()) {
            itemType = static_cast<TTupleType*>(underlyingType)->GetElementType(index);
        }
        else {
            itemType = static_cast<TStructType*>(underlyingType)->GetMemberType(index);
        }
        if (!(nativeYtTypeFlags & NTCF_COMPLEX) || underlyingType->IsTuple()) {
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(index);
        } else {
            auto structType = static_cast<TStructType*>(underlyingType);
            auto varName = structType->GetMemberName(index);
            buf.Write(StringMarker);
            buf.WriteVarI32(varName.size());
            buf.WriteMany(varName);
        }
        buf.Write(ListItemSeparatorSymbol);
        WriteYsonValueInTableFormat(buf, itemType, nativeYtTypeFlags, value.GetVariantItem(), false);
        buf.Write(ListItemSeparatorSymbol);
        buf.Write(EndListSymbol);
        break;
    }

    case TType::EKind::Data: {
        auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
        switch (schemeType) {
        case NUdf::TDataType<bool>::Id: {
            buf.Write(value.Get<bool>() ? TrueMarker : FalseMarker);
            break;
        }

        case NUdf::TDataType<ui8>::Id:
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(value.Get<ui8>());
            break;

        case NUdf::TDataType<i8>::Id:
            buf.Write(Int64Marker);
            buf.WriteVarI64(value.Get<i8>());
            break;

        case NUdf::TDataType<ui16>::Id:
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(value.Get<ui16>());
            break;

        case NUdf::TDataType<i16>::Id:
            buf.Write(Int64Marker);
            buf.WriteVarI64(value.Get<i16>());
            break;

        case NUdf::TDataType<i32>::Id:
            buf.Write(Int64Marker);
            buf.WriteVarI64(value.Get<i32>());
            break;

        case NUdf::TDataType<ui32>::Id:
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(value.Get<ui32>());
            break;

        case NUdf::TDataType<i64>::Id:
            buf.Write(Int64Marker);
            buf.WriteVarI64(value.Get<i64>());
            break;

        case NUdf::TDataType<ui64>::Id:
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(value.Get<ui64>());
            break;

        case NUdf::TDataType<float>::Id: {
            buf.Write(DoubleMarker);
            double val = value.Get<float>();
            buf.WriteMany((const char*)&val, sizeof(val));
            break;
        }

        case NUdf::TDataType<double>::Id: {
            buf.Write(DoubleMarker);
            double val = value.Get<double>();
            buf.WriteMany((const char*)&val, sizeof(val));
            break;
        }

        case NUdf::TDataType<NUdf::TUtf8>::Id:
        case NUdf::TDataType<char*>::Id:
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TDyNumber>::Id:
        case NUdf::TDataType<NUdf::TUuid>::Id: {
            buf.Write(StringMarker);
            auto str = value.AsStringRef();
            buf.WriteVarI32(str.Size());
            buf.WriteMany(str);
            break;
        }

        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            buf.Write(StringMarker);
            if (nativeYtTypeFlags & NTCF_DECIMAL){
                auto const params = static_cast<TDataDecimalType*>(type)->GetParams();
                const NDecimal::TInt128 data128 = value.GetInt128();
                char tmpBuf[NYT::NDecimal::TDecimal::MaxBinarySize];
                if (params.first < 10) {
                    // The YQL format differs from the YT format in the inf/nan values. NDecimal::FromYtDecimal converts nan/inf
                    TStringBuf resBuf = NYT::NDecimal::TDecimal::WriteBinary32(params.first, NDecimal::ToYtDecimal<i32>(data128), tmpBuf, NYT::NDecimal::TDecimal::MaxBinarySize);
                    buf.WriteVarI32(resBuf.size());
                    buf.WriteMany(resBuf.data(), resBuf.size());
                } else if (params.first < 19) {
                    TStringBuf resBuf = NYT::NDecimal::TDecimal::WriteBinary64(params.first, NDecimal::ToYtDecimal<i64>(data128), tmpBuf, NYT::NDecimal::TDecimal::MaxBinarySize);
                    buf.WriteVarI32(resBuf.size());
                    buf.WriteMany(resBuf.data(), resBuf.size());
                } else {
                    YQL_ENSURE(params.first < 36);
                    NYT::NDecimal::TDecimal::TValue128 val;
                    auto data128Converted = NDecimal::ToYtDecimal<NDecimal::TInt128>(data128);
                    memcpy(&val, &data128Converted, sizeof(val));
                    auto resBuf = NYT::NDecimal::TDecimal::WriteBinary128(params.first, val, tmpBuf, NYT::NDecimal::TDecimal::MaxBinarySize);
                    buf.WriteVarI32(resBuf.size());
                    buf.WriteMany(resBuf.data(), resBuf.size());
                }
            } else {
                char data[sizeof(NDecimal::TInt128)];
                const ui32 size = NDecimal::Serialize(value.GetInt128(), data);
                buf.WriteVarI32(size);
                buf.WriteMany(data, size);
            }
            break;
        }

        case NUdf::TDataType<NUdf::TYson>::Id: {
            // embed content
            buf.WriteMany(value.AsStringRef());
            break;
        }

        case NUdf::TDataType<NUdf::TDate>::Id:
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(value.Get<ui16>());
            break;

        case NUdf::TDataType<NUdf::TDatetime>::Id:
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(value.Get<ui32>());
            break;

        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            buf.Write(Uint64Marker);
            buf.WriteVarUI64(value.Get<ui64>());
            break;

        case NUdf::TDataType<NUdf::TInterval>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
            buf.Write(Int64Marker);
            buf.WriteVarI64(value.Get<i64>());
            break;

        case NUdf::TDataType<NUdf::TDate32>::Id:
            buf.Write(Int64Marker);
            buf.WriteVarI64(value.Get<i32>());
            break;

        case NUdf::TDataType<NUdf::TTzDate>::Id: {
            ui16 tzId = SwapBytes(value.GetTimezoneId());
            ui16 data = SwapBytes(value.Get<ui16>());
            ui32 size = sizeof(data) + sizeof(tzId);
            buf.Write(StringMarker);
            buf.WriteVarI32(size);
            buf.WriteMany((const char*)&data, sizeof(data));
            buf.WriteMany((const char*)&tzId, sizeof(tzId));
            break;
        }

        case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
            ui16 tzId = SwapBytes(value.GetTimezoneId());
            ui32 data = SwapBytes(value.Get<ui32>());
            ui32 size = sizeof(data) + sizeof(tzId);
            buf.Write(StringMarker);
            buf.WriteVarI32(size);
            buf.WriteMany((const char*)&data, sizeof(data));
            buf.WriteMany((const char*)&tzId, sizeof(tzId));
            break;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
            ui16 tzId = SwapBytes(value.GetTimezoneId());
            ui64 data = SwapBytes(value.Get<ui64>());
            ui32 size = sizeof(data) + sizeof(tzId);
            buf.Write(StringMarker);
            buf.WriteVarI32(size);
            buf.WriteMany((const char*)&data, sizeof(data));
            buf.WriteMany((const char*)&tzId, sizeof(tzId));
            break;
        }

        case NUdf::TDataType<NUdf::TTzDate32>::Id: {
            ui16 tzId = SwapBytes(value.GetTimezoneId());
            ui32 data = 0x80 ^ SwapBytes((ui32)value.Get<i32>());
            ui32 size = sizeof(data) + sizeof(tzId);
            buf.Write(StringMarker);
            buf.WriteVarI32(size);
            buf.WriteMany((const char*)&data, sizeof(data));
            buf.WriteMany((const char*)&tzId, sizeof(tzId));
            break;
        }

        case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
            ui16 tzId = SwapBytes(value.GetTimezoneId());
            ui64 data = 0x80 ^ SwapBytes((ui64)value.Get<i64>());
            ui32 size = sizeof(data) + sizeof(tzId);
            buf.Write(StringMarker);
            buf.WriteVarI32(size);
            buf.WriteMany((const char*)&data, sizeof(data));
            buf.WriteMany((const char*)&tzId, sizeof(tzId));
            break;
        }

        case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
            ui16 tzId = SwapBytes(value.GetTimezoneId());
            ui64 data = 0x80 ^ SwapBytes((ui64)value.Get<i64>());
            ui32 size = sizeof(data) + sizeof(tzId);
            buf.Write(StringMarker);
            buf.WriteVarI32(size);
            buf.WriteMany((const char*)&data, sizeof(data));
            buf.WriteMany((const char*)&tzId, sizeof(tzId));
            break;
        }

        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            buf.Write(StringMarker);
            NUdf::TUnboxedValue json = ValueToString(EDataSlot::JsonDocument, value);
            auto str = json.AsStringRef();
            buf.WriteVarI32(str.Size());
            buf.WriteMany(str);
            break;
        }

        default:
            YQL_ENSURE(false, "Unsupported data type: " << schemeType);
        }

        break;
    }

    case TType::EKind::Struct: {
        auto structType = static_cast<TStructType*>(type);
        if (nativeYtTypeFlags & ENativeTypeCompatFlags::NTCF_COMPLEX) {
            buf.Write(BeginMapSymbol);
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                buf.Write(StringMarker);
                auto key = structType->GetMemberName(i);
                buf.WriteVarI32(key.size());
                buf.WriteMany(key);
                buf.Write(KeyValueSeparatorSymbol);
                WriteYsonValueInTableFormat(buf, structType->GetMemberType(i), nativeYtTypeFlags, value.GetElement(i), false);
                buf.Write(KeyedItemSeparatorSymbol);
            }
            buf.Write(EndMapSymbol);
        } else {
            buf.Write(BeginListSymbol);
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                WriteYsonValueInTableFormat(buf, structType->GetMemberType(i), nativeYtTypeFlags, value.GetElement(i), false);
                buf.Write(ListItemSeparatorSymbol);
            }
            buf.Write(EndListSymbol);
        }
        break;
    }

    case TType::EKind::List: {
        auto itemType = static_cast<TListType*>(type)->GetItemType();
        const auto iter = value.GetListIterator();
        buf.Write(BeginListSymbol);
        for (NUdf::TUnboxedValue item; iter.Next(item); buf.Write(ListItemSeparatorSymbol)) {
            WriteYsonValueInTableFormat(buf, itemType, nativeYtTypeFlags, item, false);
        }

        buf.Write(EndListSymbol);
        break;
    }

    case TType::EKind::Optional: {
        auto itemType = static_cast<TOptionalType*>(type)->GetItemType();
        if (nativeYtTypeFlags & ENativeTypeCompatFlags::NTCF_COMPLEX) {
            if (value) {
                if (itemType->GetKind() == TType::EKind::Optional || itemType->GetKind() == TType::EKind::Pg) {
                    buf.Write(BeginListSymbol);
                }
                WriteYsonValueInTableFormat(buf, itemType, nativeYtTypeFlags, value.GetOptionalValue(), false);
                if (itemType->GetKind() == TType::EKind::Optional || itemType->GetKind() == TType::EKind::Pg) {
                    buf.Write(ListItemSeparatorSymbol);
                    buf.Write(EndListSymbol);
                }
            } else {
                buf.Write(EntitySymbol);
            }
        } else {
            if (!value) {
                if (topLevel) {
                    buf.Write(BeginListSymbol);
                    buf.Write(EndListSymbol);
                }
                else {
                    buf.Write(EntitySymbol);
                }
            }
            else {
                buf.Write(BeginListSymbol);
                WriteYsonValueInTableFormat(buf, itemType, nativeYtTypeFlags, value.GetOptionalValue(), false);
                buf.Write(ListItemSeparatorSymbol);
                buf.Write(EndListSymbol);
            }
        }
        break;
    }

    case TType::EKind::Dict: {
        auto dictType = static_cast<TDictType*>(type);
        const auto iter = value.GetDictIterator();
        buf.Write(BeginListSymbol);
        for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
            buf.Write(BeginListSymbol);
            WriteYsonValueInTableFormat(buf, dictType->GetKeyType(), nativeYtTypeFlags, key, false);
            buf.Write(ListItemSeparatorSymbol);
            WriteYsonValueInTableFormat(buf, dictType->GetPayloadType(), nativeYtTypeFlags, payload, false);
            buf.Write(ListItemSeparatorSymbol);
            buf.Write(EndListSymbol);
            buf.Write(ListItemSeparatorSymbol);
        }

        buf.Write(EndListSymbol);
        break;
    }

    case TType::EKind::Tuple: {
        auto tupleType = static_cast<TTupleType*>(type);
        buf.Write(BeginListSymbol);
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            WriteYsonValueInTableFormat(buf, tupleType->GetElementType(i), nativeYtTypeFlags, value.GetElement(i), false);
            buf.Write(ListItemSeparatorSymbol);
        }

        buf.Write(EndListSymbol);
        break;
    }

    case TType::EKind::Void: {
        buf.Write(EntitySymbol);
        break;
    }

    case TType::EKind::Null: {
        buf.Write(EntitySymbol);
        break;
    }

    case TType::EKind::EmptyList: {
        buf.Write(BeginListSymbol);
        buf.Write(EndListSymbol);
        break;
    }

    case TType::EKind::EmptyDict: {
        buf.Write(BeginListSymbol);
        buf.Write(EndListSymbol);
        break;
    }

    case TType::EKind::Pg: {
        auto pgType = static_cast<TPgType*>(type);
        WriteYsonValueInTableFormatPg(buf, pgType, value, topLevel);
        break;
    }

    default:
        YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
}

///////////////////////////////////////////
//
// Initial state first = last = &dummy
//
// +1 block first = &dummy, last = newPage, first.next = newPage, newPage.next= &dummy
// +1 block first = &dummy, last = newPage2, first.next = newPage, newPage.next = newPage2, newPage2.next = &dummy
//
///////////////////////////////////////////
class TTempBlockWriter : public NCommon::IBlockWriter {
public:
    TTempBlockWriter()
        : Pool_(*TlsAllocState)
        , Last_(&Dummy_)
    {
        Dummy_.Avail_ = 0;
        Dummy_.Next_ = &Dummy_;
    }

    ~TTempBlockWriter() {
        auto current = Dummy_.Next_; // skip dummy node
        while (current != &Dummy_) {
            auto next = current->Next_;
            Pool_.ReturnPage(current);
            current = next;
        }
    }

    void SetRecordBoundaryCallback(std::function<void()> callback) override {
        Y_UNUSED(callback);
    }

    void WriteBlocks(NCommon::TOutputBuf& buf) const {
        auto current = Dummy_.Next_; // skip dummy node
        while (current != &Dummy_) {
            auto next = current->Next_;
            buf.WriteMany((const char*)(current + 1), current->Avail_);
            current = next;
        }
    }

    TTempBlockWriter(const TTempBlockWriter&) = delete;
    void operator=(const TTempBlockWriter&) = delete;

    std::pair<char*, char*> NextEmptyBlock() override {
        auto newPage = Pool_.GetPage();
        auto header = (TPageHeader*)newPage;
        header->Avail_ = 0;
        header->Next_ = &Dummy_;
        Last_->Next_ = header;
        Last_ = header;
        return std::make_pair((char*)(header + 1), (char*)newPage + TAlignedPagePool::POOL_PAGE_SIZE);
    }

    void ReturnBlock(size_t avail, std::optional<size_t> lastRecordBoundary) override {
        Y_UNUSED(lastRecordBoundary);
        YQL_ENSURE(avail <= TAlignedPagePool::POOL_PAGE_SIZE - sizeof(TPageHeader));
        Last_->Avail_ = avail;
    }

    void Finish() override {
    }

private:
    struct TPageHeader {
        TPageHeader* Next_ = nullptr;
        ui32 Avail_ = 0;
    };

    NKikimr::TAlignedPagePool& Pool_;
    TPageHeader* Last_;
    TPageHeader Dummy_;
};

extern "C" void WriteYsonContainerValue(TType* type, ui64 nativeYtTypeFlags, const NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    TTempBlockWriter blockWriter;
    NCommon::TOutputBuf ysonBuf(blockWriter, nullptr);
    WriteYsonValueInTableFormat(ysonBuf, type, nativeYtTypeFlags, value, true);
    ysonBuf.Flush();
    ui32 size = ysonBuf.GetWrittenBytes();
    buf.WriteMany((const char*)&size, sizeof(size));
    blockWriter.WriteBlocks(buf);
}

void SkipSkiffField(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, NCommon::TInputBuf& buf) {
    const bool isOptional = type->IsOptional();
    if (isOptional) {
        // Unwrap optional
        type = static_cast<TOptionalType*>(type)->GetItemType();
    }

    if (isOptional) {
        auto marker = buf.Read();
        if (!marker) {
            return;
        }
    }

    if (type->IsData()) {
        auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
        switch (schemeType) {
        case NUdf::TDataType<bool>::Id:
            buf.SkipMany(sizeof(ui8));
            break;

        case NUdf::TDataType<ui8>::Id:
        case NUdf::TDataType<ui16>::Id:
        case NUdf::TDataType<ui32>::Id:
        case NUdf::TDataType<ui64>::Id:
        case NUdf::TDataType<NUdf::TDate>::Id:
        case NUdf::TDataType<NUdf::TDatetime>::Id:
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
            buf.SkipMany(sizeof(ui64));
            break;

        case NUdf::TDataType<i8>::Id:
        case NUdf::TDataType<i16>::Id:
        case NUdf::TDataType<i32>::Id:
        case NUdf::TDataType<i64>::Id:
        case NUdf::TDataType<NUdf::TInterval>::Id:
        case NUdf::TDataType<NUdf::TDate32>::Id:
        case NUdf::TDataType<NUdf::TDatetime64>::Id:
        case NUdf::TDataType<NUdf::TTimestamp64>::Id:
        case NUdf::TDataType<NUdf::TInterval64>::Id:
            buf.SkipMany(sizeof(i64));
            break;

        case NUdf::TDataType<float>::Id:
        case NUdf::TDataType<double>::Id:
            buf.SkipMany(sizeof(double));
            break;

        case NUdf::TDataType<NUdf::TUtf8>::Id:
        case NUdf::TDataType<char*>::Id:
        case NUdf::TDataType<NUdf::TJson>::Id:
        case NUdf::TDataType<NUdf::TYson>::Id:
        case NUdf::TDataType<NUdf::TUuid>::Id:
        case NUdf::TDataType<NUdf::TDyNumber>::Id:
        case NUdf::TDataType<NUdf::TTzDate>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
        case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
            ui32 size;
            buf.ReadMany((char*)&size, sizeof(size));
            CHECK_STRING_LENGTH_UNSIGNED(size);
            buf.SkipMany(size);
            break;
        }
        case NUdf::TDataType<NUdf::TDecimal>::Id: {
            if (nativeYtTypeFlags & NTCF_DECIMAL) {
                auto const params = static_cast<TDataDecimalType*>(type)->GetParams();
                if (params.first < 10) {
                    buf.SkipMany(sizeof(i32));
                } else if (params.first < 19) {
                    buf.SkipMany(sizeof(i64));
                } else {
                    buf.SkipMany(sizeof(NDecimal::TInt128));
                }
            } else {
                ui32 size;
                buf.ReadMany((char*)&size, sizeof(size));
                CHECK_STRING_LENGTH_UNSIGNED(size);
                buf.SkipMany(size);
            }
            break;
        }
        default:
            YQL_ENSURE(false, "Unsupported data type: " << schemeType);
        }
        return;
    }

    if (type->IsPg()) {
        SkipSkiffPg(static_cast<TPgType*>(type), buf);
        return;
    }

    if (type->IsStruct()) {
        auto structType = static_cast<TStructType*>(type);
        const std::vector<size_t>* reorder = nullptr;
        if (auto cookie = structType->GetCookie()) {
            reorder = ((const std::vector<size_t>*)cookie);
        }
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            SkipSkiffField(structType->GetMemberType(reorder ? reorder->at(i) : i), nativeYtTypeFlags, buf);
        }
        return;
    }

    if (type->IsList()) {
        auto itemType = static_cast<TListType*>(type)->GetItemType();
        while (buf.Read() == '\0') {
            SkipSkiffField(itemType, nativeYtTypeFlags, buf);
        }
        return;
    }

    if (type->IsTuple()) {
        auto tupleType = static_cast<TTupleType*>(type);

        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            SkipSkiffField(tupleType->GetElementType(i), nativeYtTypeFlags, buf);
        }
        return;
    }

    if (type->IsVariant()) {
        auto varType = AS_TYPE(TVariantType, type);
        ui16 data = 0;
        if (varType->GetAlternativesCount() < 256) {
            buf.ReadMany((char*)&data, 1);
        } else {
            buf.ReadMany((char*)&data, sizeof(data));
        }

        if (varType->GetUnderlyingType()->IsTuple()) {
            auto tupleType = AS_TYPE(TTupleType, varType->GetUnderlyingType());
            YQL_ENSURE(data < tupleType->GetElementsCount());
            SkipSkiffField(tupleType->GetElementType(data), nativeYtTypeFlags, buf);
        } else {
            auto structType = AS_TYPE(TStructType, varType->GetUnderlyingType());
            if (auto cookie = structType->GetCookie()) {
                const std::vector<size_t>& reorder = *((const std::vector<size_t>*)cookie);
                data = reorder[data];
            }
            YQL_ENSURE(data < structType->GetMembersCount());

            SkipSkiffField(structType->GetMemberType(data), nativeYtTypeFlags, buf);
        }
        return;
    }

    if (type->IsVoid()) {
        return;
    }

    if (type->IsNull()) {
        return;
    }

    if (type->IsEmptyList() || type->IsEmptyDict()) {
        return;
    }

    if (type->IsDict()) {
        auto dictType = AS_TYPE(TDictType, type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        while (buf.Read() == '\0') {
            SkipSkiffField(keyType, nativeYtTypeFlags, buf);
            SkipSkiffField(payloadType, nativeYtTypeFlags, buf);
        }
        return;
    }

    YQL_ENSURE(false, "Unsupported type for skip: " << type->GetKindAsStr());
}

NKikimr::NUdf::TUnboxedValue ReadSkiffNativeYtValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory, NCommon::TInputBuf& buf)
{
    if (type->IsData()) {
        return ReadSkiffData(type, nativeYtTypeFlags, buf);
    }

    if (type->IsPg()) {
        return ReadSkiffPg(static_cast<TPgType*>(type), buf);
    }

    if (type->IsOptional()) {
        auto marker = buf.Read();
        if (!marker) {
            return NUdf::TUnboxedValue();
        }

        auto value = ReadSkiffNativeYtValue(AS_TYPE(TOptionalType, type)->GetItemType(), nativeYtTypeFlags, holderFactory, buf);
        return value.Release().MakeOptional();
    }

    if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        NUdf::TUnboxedValue* items;
        auto value = holderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), items);
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            items[i] = ReadSkiffNativeYtValue(tupleType->GetElementType(i), nativeYtTypeFlags, holderFactory, buf);
        }

        return value;
    }

    if (type->IsStruct()) {
        auto structType = AS_TYPE(TStructType, type);
        NUdf::TUnboxedValue* items;
        auto value = holderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), items);

        if (auto cookie = type->GetCookie()) {
            const std::vector<size_t>& reorder = *((const std::vector<size_t>*)cookie);
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                const auto ndx = reorder[i];
                items[ndx] = ReadSkiffNativeYtValue(structType->GetMemberType(ndx), nativeYtTypeFlags, holderFactory, buf);
            }
        } else {
            for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                items[i] = ReadSkiffNativeYtValue(structType->GetMemberType(i), nativeYtTypeFlags, holderFactory, buf);
            }
        }

        return value;
    }

    if (type->IsList()) {
        auto itemType = AS_TYPE(TListType, type)->GetItemType();
        TDefaultListRepresentation items;
        while (buf.Read() == '\0') {
            items = items.Append(ReadSkiffNativeYtValue(itemType, nativeYtTypeFlags, holderFactory, buf));
        }

        return holderFactory.CreateDirectListHolder(std::move(items));
    }

    if (type->IsVariant()) {
        auto varType = AS_TYPE(TVariantType, type);
        ui16 data = 0;
        if (varType->GetAlternativesCount() < 256) {
            buf.ReadMany((char*)&data, 1);
        } else {
            buf.ReadMany((char*)&data, sizeof(data));
        }
        if (varType->GetUnderlyingType()->IsTuple()) {
            auto tupleType = AS_TYPE(TTupleType, varType->GetUnderlyingType());
            YQL_ENSURE(data < tupleType->GetElementsCount());
            auto item = ReadSkiffNativeYtValue(tupleType->GetElementType(data), nativeYtTypeFlags, holderFactory, buf);
            return holderFactory.CreateVariantHolder(item.Release(), data);
        }
        else {
            auto structType = AS_TYPE(TStructType, varType->GetUnderlyingType());
            if (auto cookie = structType->GetCookie()) {
                const std::vector<size_t>& reorder = *((const std::vector<size_t>*)cookie);
                data = reorder[data];
            }
            YQL_ENSURE(data < structType->GetMembersCount());

            auto item = ReadSkiffNativeYtValue(structType->GetMemberType(data), nativeYtTypeFlags, holderFactory, buf);
            return holderFactory.CreateVariantHolder(item.Release(), data);
        }
    }

    if (type->IsVoid()) {
        return NUdf::TUnboxedValue::Zero();
    }

    if (type->IsNull()) {
        return NUdf::TUnboxedValue();
    }

    if (type->IsEmptyList() || type->IsEmptyDict()) {
        return holderFactory.GetEmptyContainerLazy();
    }

    if (type->IsDict()) {
        auto dictType = AS_TYPE(TDictType, type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();

        auto builder = holderFactory.NewDict(dictType, NUdf::TDictFlags::EDictKind::Hashed);
        while (buf.Read() == '\0') {
            auto key = ReadSkiffNativeYtValue(keyType, nativeYtTypeFlags, holderFactory, buf);
            auto payload = ReadSkiffNativeYtValue(payloadType, nativeYtTypeFlags, holderFactory, buf);
            builder->Add(std::move(key), std::move(payload));
        }

        return builder->Build();
    }

    YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
}

NUdf::TUnboxedValue ReadSkiffData(TType* type, ui64 nativeYtTypeFlags, NCommon::TInputBuf& buf) {
    auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
    switch (schemeType) {
    case NUdf::TDataType<bool>::Id: {
        ui8 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(data != 0);
    }

    case NUdf::TDataType<ui8>::Id: {
        ui64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(ui8(data));
    }

    case NUdf::TDataType<i8>::Id: {
        i64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(i8(data));
    }

    case NUdf::TDataType<NUdf::TDate>::Id:
    case NUdf::TDataType<ui16>::Id: {
        ui64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(ui16(data));
    }

    case NUdf::TDataType<i16>::Id: {
        i64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(i16(data));
    }

    case NUdf::TDataType<NUdf::TDate32>::Id:
    case NUdf::TDataType<i32>::Id: {
        i64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(i32(data));
    }

    case NUdf::TDataType<NUdf::TDatetime>::Id:
    case NUdf::TDataType<ui32>::Id: {
        ui64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(ui32(data));
    }

    case NUdf::TDataType<NUdf::TInterval>::Id:
    case NUdf::TDataType<NUdf::TInterval64>::Id:
    case NUdf::TDataType<NUdf::TDatetime64>::Id:
    case NUdf::TDataType<NUdf::TTimestamp64>::Id:
    case NUdf::TDataType<i64>::Id: {
        i64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(data);
    }

    case NUdf::TDataType<NUdf::TTimestamp>::Id:
    case NUdf::TDataType<ui64>::Id: {
        ui64 data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(data);
    }

    case NUdf::TDataType<float>::Id: {
        double data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(float(data));
    }

    case NUdf::TDataType<double>::Id: {
        double data;
        buf.ReadMany((char*)&data, sizeof(data));
        return NUdf::TUnboxedValuePod(data);
    }

    case NUdf::TDataType<NUdf::TUtf8>::Id:
    case NUdf::TDataType<char*>::Id:
    case NUdf::TDataType<NUdf::TJson>::Id:
    case NUdf::TDataType<NUdf::TYson>::Id:
    case NUdf::TDataType<NUdf::TDyNumber>::Id:
    case NUdf::TDataType<NUdf::TUuid>::Id: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        auto str = NUdf::TUnboxedValue(MakeStringNotFilled(size));
        buf.ReadMany(str.AsStringRef().Data(), size);
        return str;
    }

    case NUdf::TDataType<NUdf::TDecimal>::Id: {
        if (nativeYtTypeFlags & NTCF_DECIMAL) {
            auto const params = static_cast<TDataDecimalType*>(type)->GetParams();
            if (params.first < 10) {
                i32 data;
                buf.ReadMany((char*)&data, sizeof(data));
                return NUdf::TUnboxedValuePod(NDecimal::FromYtDecimal(data));
            } else if (params.first < 19) {
                i64 data;
                buf.ReadMany((char*)&data, sizeof(data));
                return NUdf::TUnboxedValuePod(NDecimal::FromYtDecimal(data));
            } else {
                YQL_ENSURE(params.first < 36);
                NDecimal::TInt128 data;
                buf.ReadMany((char*)&data, sizeof(data));
                return NUdf::TUnboxedValuePod(NDecimal::FromYtDecimal(data));
            }
        } else {
            ui32 size;
            buf.ReadMany(reinterpret_cast<char*>(&size), sizeof(size));
            const auto maxSize = sizeof(NDecimal::TInt128);
            YQL_ENSURE(size > 0U && size <= maxSize, "Bad decimal field size: " << size);
            char data[maxSize];
            buf.ReadMany(data, size);
            const auto& v = NDecimal::Deserialize(data, size);
            YQL_ENSURE(!NDecimal::IsError(v.first), "Bad decimal field data: " << data);
            YQL_ENSURE(size == v.second, "Bad decimal field size: " << size);
            return NUdf::TUnboxedValuePod(v.first);
        }
    }

    case NUdf::TDataType<NUdf::TTzDate>::Id: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        auto& vec = buf.YsonBuffer();
        vec.resize(size);
        buf.ReadMany(vec.data(), size);
        ui16 value;
        ui16 tzId;
        YQL_ENSURE(DeserializeTzDate(TStringBuf(vec.begin(), vec.end()), value, tzId));
        auto data = NUdf::TUnboxedValuePod(value);
        data.SetTimezoneId(tzId);
        return data;
    }

    case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        auto& vec = buf.YsonBuffer();
        vec.resize(size);
        buf.ReadMany(vec.data(), size);
        ui32 value;
        ui16 tzId;
        YQL_ENSURE(DeserializeTzDatetime(TStringBuf(vec.begin(), vec.end()), value, tzId));
        auto data = NUdf::TUnboxedValuePod(value);
        data.SetTimezoneId(tzId);
        return data;
    }

    case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        auto& vec = buf.YsonBuffer();
        vec.resize(size);
        buf.ReadMany(vec.data(), size);
        ui64 value;
        ui16 tzId;
        YQL_ENSURE(DeserializeTzTimestamp(TStringBuf(vec.begin(), vec.end()), value, tzId));
        auto data = NUdf::TUnboxedValuePod(value);
        data.SetTimezoneId(tzId);
        return data;
    }

    case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        auto json = NUdf::TUnboxedValue(MakeStringNotFilled(size));
        buf.ReadMany(json.AsStringRef().Data(), size);
        return ValueFromString(EDataSlot::JsonDocument, json.AsStringRef());
    }

    default:
        YQL_ENSURE(false, "Unsupported data type: " << schemeType);
    }
}

void WriteSkiffData(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    auto schemeType = static_cast<TDataType*>(type)->GetSchemeType();
    switch (schemeType) {
    case NUdf::TDataType<bool>::Id: {
        ui8 data = value.Get<ui8>();
        buf.Write(data);
        break;
    }

    case NUdf::TDataType<ui8>::Id: {
        ui64 data = value.Get<ui8>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<i8>::Id: {
        i64 data = value.Get<i8>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<NUdf::TDate>::Id:
    case NUdf::TDataType<ui16>::Id: {
        ui64 data = value.Get<ui16>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<i16>::Id: {
        i64 data = value.Get<i16>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<NUdf::TDate32>::Id:
    case NUdf::TDataType<i32>::Id: {
        i64 data = value.Get<i32>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<NUdf::TDatetime>::Id:
    case NUdf::TDataType<ui32>::Id: {
        ui64 data = value.Get<ui32>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<NUdf::TInterval>::Id:
    case NUdf::TDataType<NUdf::TInterval64>::Id:
    case NUdf::TDataType<NUdf::TDatetime64>::Id:
    case NUdf::TDataType<NUdf::TTimestamp64>::Id:
    case NUdf::TDataType<i64>::Id: {
        i64 data = value.Get<i64>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<NUdf::TTimestamp>::Id:
    case NUdf::TDataType<ui64>::Id: {
        ui64 data = value.Get<ui64>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<float>::Id: {
        double data = value.Get<float>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<double>::Id: {
        double data = value.Get<double>();
        buf.WriteMany((const char*)&data, sizeof(data));
        break;
    }

    case NUdf::TDataType<NUdf::TUtf8>::Id:
    case NUdf::TDataType<char*>::Id:
    case NUdf::TDataType<NUdf::TJson>::Id:
    case NUdf::TDataType<NUdf::TYson>::Id:
    case NUdf::TDataType<NUdf::TDyNumber>::Id:
    case NUdf::TDataType<NUdf::TUuid>::Id: {
        auto str = value.AsStringRef();
        ui32 size = str.Size();
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany(str);
        break;
    }

    case NUdf::TDataType<NUdf::TDecimal>::Id: {
        if (nativeYtTypeFlags & NTCF_DECIMAL) {
            auto const params = static_cast<TDataDecimalType*>(type)->GetParams();
            const NDecimal::TInt128 data128 = value.GetInt128();
            if (params.first < 10) {
                auto data = NDecimal::ToYtDecimal<i32>(data128);
                buf.WriteMany((const char*)&data, sizeof(data));
            } else if (params.first < 19) {
                auto data = NDecimal::ToYtDecimal<i64>(data128);
                buf.WriteMany((const char*)&data, sizeof(data));
            } else {
                YQL_ENSURE(params.first < 36);
                auto data = NDecimal::ToYtDecimal<NDecimal::TInt128>(data128);
                buf.WriteMany((const char*)&data, sizeof(data));
            }
        } else {
            char data[sizeof(NDecimal::TInt128)];
            const ui32 size = NDecimal::Serialize(value.GetInt128(), data);
            buf.WriteMany(reinterpret_cast<const char*>(&size), sizeof(size));
            buf.WriteMany(data, size);
        }
        break;
    }

    case NUdf::TDataType<NUdf::TTzDate>::Id: {
        ui16 tzId = SwapBytes(value.GetTimezoneId());
        ui16 data = SwapBytes(value.Get<ui16>());
        ui32 size = sizeof(data) + sizeof(tzId);
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany((const char*)&data, sizeof(data));
        buf.WriteMany((const char*)&tzId, sizeof(tzId));
        break;
    }

    case NUdf::TDataType<NUdf::TTzDatetime>::Id: {
        ui16 tzId = SwapBytes(value.GetTimezoneId());
        ui32 data = SwapBytes(value.Get<ui32>());
        ui32 size = sizeof(data) + sizeof(tzId);
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany((const char*)&data, sizeof(data));
        buf.WriteMany((const char*)&tzId, sizeof(tzId));
        break;
    }

    case NUdf::TDataType<NUdf::TTzTimestamp>::Id: {
        ui16 tzId = SwapBytes(value.GetTimezoneId());
        ui64 data = SwapBytes(value.Get<ui64>());
        ui32 size = sizeof(data) + sizeof(tzId);
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany((const char*)&data, sizeof(data));
        buf.WriteMany((const char*)&tzId, sizeof(tzId));
        break;
    }

    case NUdf::TDataType<NUdf::TTzDate32>::Id: {
        ui16 tzId = SwapBytes(value.GetTimezoneId());
        ui32 data = 0x80 ^ SwapBytes((ui32)value.Get<i32>());
        ui32 size = sizeof(data) + sizeof(tzId);
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany((const char*)&data, sizeof(data));
        buf.WriteMany((const char*)&tzId, sizeof(tzId));
        break;
    }

    case NUdf::TDataType<NUdf::TTzDatetime64>::Id: {
        ui16 tzId = SwapBytes(value.GetTimezoneId());
        ui64 data = 0x80 ^ SwapBytes((ui64)value.Get<i64>());
        ui32 size = sizeof(data) + sizeof(tzId);
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany((const char*)&data, sizeof(data));
        buf.WriteMany((const char*)&tzId, sizeof(tzId));
        break;
    }

    case NUdf::TDataType<NUdf::TTzTimestamp64>::Id: {
        ui16 tzId = SwapBytes(value.GetTimezoneId());
        ui64 data = 0x80 ^ SwapBytes((ui64)value.Get<i64>());
        ui32 size = sizeof(data) + sizeof(tzId);
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany((const char*)&data, sizeof(data));
        buf.WriteMany((const char*)&tzId, sizeof(tzId));
        break;
    }

    case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
        NUdf::TUnboxedValue json = ValueToString(EDataSlot::JsonDocument, value);
        auto str = json.AsStringRef();
        ui32 size = str.Size();
        buf.WriteMany((const char*)&size, sizeof(size));
        buf.WriteMany(str);
        break;
    }

    default:
        YQL_ENSURE(false, "Unsupported data type: " << schemeType);
    }
}

void WriteSkiffNativeYtValue(NKikimr::NMiniKQL::TType* type, ui64 nativeYtTypeFlags, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    if (type->IsData()) {
        WriteSkiffData(type, nativeYtTypeFlags, value, buf);
    } else if (type->IsPg()) {
        WriteSkiffPgValue(static_cast<TPgType*>(type), value, buf);
    } else if (type->IsOptional()) {
        if (!value) {
            buf.Write('\0');
            return;
        }

        buf.Write('\1');
        WriteSkiffNativeYtValue(AS_TYPE(TOptionalType, type)->GetItemType(), nativeYtTypeFlags, value.GetOptionalValue(), buf);
    } else if (type->IsList()) {
        auto itemType = AS_TYPE(TListType, type)->GetItemType();
        auto elements = value.GetElements();
        if (elements) {
            ui32 size = value.GetListLength();
            for (ui32 i = 0; i < size; ++i) {
                buf.Write('\0');
                WriteSkiffNativeYtValue(itemType, nativeYtTypeFlags, elements[i], buf);
            }
        } else {
            NUdf::TUnboxedValue item;
            for (auto iter = value.GetListIterator(); iter.Next(item); ) {
                buf.Write('\0');
                WriteSkiffNativeYtValue(itemType, nativeYtTypeFlags, item, buf);
            }
        }

        buf.Write('\xff');
    } else if (type->IsTuple()) {
        auto tupleType = AS_TYPE(TTupleType, type);
        auto elements = value.GetElements();
        if (elements) {
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                WriteSkiffNativeYtValue(tupleType->GetElementType(i), nativeYtTypeFlags, elements[i], buf);
            }
        } else {
            for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
                WriteSkiffNativeYtValue(tupleType->GetElementType(i), nativeYtTypeFlags, value.GetElement(i), buf);
            }
        }
    } else if (type->IsStruct()) {
        auto structType = AS_TYPE(TStructType, type);
        auto elements = value.GetElements();
        if (auto cookie = type->GetCookie()) {
            const std::vector<size_t>& reorder = *((const std::vector<size_t>*)cookie);
            if (elements) {
                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    const auto ndx = reorder[i];
                    WriteSkiffNativeYtValue(structType->GetMemberType(ndx), nativeYtTypeFlags, elements[ndx], buf);
                }
            } else {
                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    const auto ndx = reorder[i];
                    WriteSkiffNativeYtValue(structType->GetMemberType(ndx), nativeYtTypeFlags, value.GetElement(ndx), buf);
                }
            }
        } else {
            if (elements) {
                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    WriteSkiffNativeYtValue(structType->GetMemberType(i), nativeYtTypeFlags, elements[i], buf);
                }
            } else {
                for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
                    WriteSkiffNativeYtValue(structType->GetMemberType(i), nativeYtTypeFlags, value.GetElement(i), buf);
                }
            }
        }
    } else if (type->IsVariant()) {
        auto varType = AS_TYPE(TVariantType, type);
        ui16 index = (ui16)value.GetVariantIndex();
        if (varType->GetAlternativesCount() < 256) {
            buf.WriteMany((const char*)&index, 1);
        } else {
            buf.WriteMany((const char*)&index, sizeof(index));
        }

        if (varType->GetUnderlyingType()->IsTuple()) {
            auto tupleType = AS_TYPE(TTupleType, varType->GetUnderlyingType());
            WriteSkiffNativeYtValue(tupleType->GetElementType(index), nativeYtTypeFlags, value.GetVariantItem(), buf);
        } else {
            auto structType = AS_TYPE(TStructType, varType->GetUnderlyingType());
            if (auto cookie = structType->GetCookie()) {
                const std::vector<size_t>& reorder = *((const std::vector<size_t>*)cookie);
                index = reorder[index];
            }
            YQL_ENSURE(index < structType->GetMembersCount());

            WriteSkiffNativeYtValue(structType->GetMemberType(index), nativeYtTypeFlags, value.GetVariantItem(), buf);
        }
    } else if (type->IsVoid() || type->IsNull() || type->IsEmptyList() || type->IsEmptyDict()) {
    } else if (type->IsDict()) {
        auto dictType = AS_TYPE(TDictType, type);
        auto keyType = dictType->GetKeyType();
        auto payloadType = dictType->GetPayloadType();
        NUdf::TUnboxedValue key, payload;
        for (auto iter = value.GetDictIterator(); iter.NextPair(key, payload); ) {
            buf.Write('\0');
            WriteSkiffNativeYtValue(keyType, nativeYtTypeFlags, key, buf);
            WriteSkiffNativeYtValue(payloadType, nativeYtTypeFlags, payload, buf);
        }

        buf.Write('\xff');
    } else {
        YQL_ENSURE(false, "Unsupported type: " << type->GetKindAsStr());
    }
}

extern "C" void WriteContainerNativeYtValue(TType* type, ui64 nativeYtTypeFlags, const NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    WriteSkiffNativeYtValue(type, nativeYtTypeFlags, value, buf);
}

} // NYql
