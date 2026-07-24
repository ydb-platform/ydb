#include <ydb/core/engine/kikimr_program_builder.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/scheme_types/scheme_types_defs.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

namespace {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

constexpr size_t MaxPrograms = 80;

TRuntimeNode MakeKeyNode(TKikimrProgramBuilder& builder, FuzzedDataProvider& provider, ui32 index) {
    switch (index % 3) {
        case 0:
            return builder.TProgramBuilder::NewDataLiteral<ui32>(provider.ConsumeIntegral<ui32>());
        case 1:
            if (provider.ConsumeBool()) {
                return builder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id);
            }
            return builder.NewOptional(builder.TProgramBuilder::NewDataLiteral<ui64>(provider.ConsumeIntegral<ui64>()));
        default:
            return builder.NewOptional(builder.NewDataLiteral<NUdf::EDataSlot::String>(
                provider.ConsumeRandomLengthString(24)));
    }
}

TVector<NScheme::TTypeInfo> KeyTypes(size_t components = 3) {
    TVector<NScheme::TTypeInfo> all = {
        NScheme::TTypeInfo(NUdf::TDataType<ui32>::Id),
        NScheme::TTypeInfo(NUdf::TDataType<ui64>::Id),
        NScheme::TTypeInfo(NUdf::TDataType<char*>::Id),
    };
    all.resize(components);
    return all;
}

TVector<TRuntimeNode> MakeKey(TKikimrProgramBuilder& builder, FuzzedDataProvider& provider, size_t components = 3) {
    TVector<TRuntimeNode> key;
    key.reserve(components);
    for (ui32 i = 0; i < components; ++i) {
        key.push_back(MakeKeyNode(builder, provider, i));
    }
    return key;
}

TVector<TSelectColumn> Columns() {
    TVector<TSelectColumn> columns;
    columns.emplace_back("key", 1, NScheme::TTypeInfo(NUdf::TDataType<ui32>::Id), EColumnTypeConstraint::Nullable);
    columns.emplace_back("value", 2, NScheme::TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
    return columns;
}

void CheckCellVecRoundTrip(TConstArrayRef<TCell> cells) {
    const TString serialized = TSerializedCellVec::Serialize(cells);
    TSerializedCellVec parsed;
    Y_ABORT_UNLESS(TSerializedCellVec::TryParse(serialized, parsed));
    Y_ABORT_UNLESS(parsed.GetCells().size() == cells.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        Y_ABORT_UNLESS(parsed.GetCells()[i].IsNull() == cells[i].IsNull());
        Y_ABORT_UNLESS(parsed.GetCells()[i].AsBuf() == cells[i].AsBuf());
    }
}

void CheckDesc(const TKeyDesc& desc) {
    Y_ABORT_UNLESS(desc.TableId.PathId.OwnerId >= 1);
    Y_ABORT_UNLESS(desc.TableId.PathId.LocalPathId >= 1);
    Y_ABORT_UNLESS(!desc.KeyColumnTypes.empty());
    CheckCellVecRoundTrip(desc.Range.From);
    CheckCellVecRoundTrip(desc.Range.To);
    if (desc.Range.Point) {
        Y_ABORT_UNLESS(desc.Range.InclusiveFrom);
        Y_ABORT_UNLESS(desc.Range.InclusiveTo);
    }
    for (const auto& column : desc.Columns) {
        Y_ABORT_UNLESS(column.Column != TKeyDesc::EColumnIdInvalid);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    for (size_t i = 0; i < MaxPrograms && provider.remaining_bytes(); ++i) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto guard = env.BindAllocator();
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TKikimrProgramBuilder builder(env, *functionRegistry);

        const TTableId tableId(
            provider.ConsumeIntegralInRange<ui64>(1, 8),
            provider.ConsumeIntegralInRange<ui64>(1, 32),
            provider.ConsumeIntegralInRange<ui64>(0, 4));
        auto keyTypes = KeyTypes();
        const TArrayRef<NScheme::TTypeInfo> keyTypesRef(keyTypes.data(), keyTypes.size());
        const auto columns = Columns();
        const TArrayRef<const TSelectColumn> columnsRef(columns.data(), columns.size());
        auto program = builder.NewEmptyListOfVoid();

        switch (provider.ConsumeIntegralInRange<ui8>(0, 3)) {
            case 0: {
                auto key = MakeKey(builder, provider);
                const TKeyColumnValues keyRef(key.data(), key.size());
                program = builder.Append(program, builder.SetResult("r", builder.SelectRow(tableId, keyTypesRef, columnsRef, keyRef)));
                break;
            }
            case 1: {
                TTableRangeOptions options(builder.GetDefaultTableRangeOptions());
                auto from = MakeKey(builder, provider, provider.ConsumeIntegralInRange<size_t>(1, 3));
                auto to = MakeKey(builder, provider, provider.ConsumeIntegralInRange<size_t>(1, 3));
                auto rangeKeyTypes = KeyTypes(std::max(from.size(), to.size()));
                const TArrayRef<NScheme::TTypeInfo> rangeKeyTypesRef(rangeKeyTypes.data(), rangeKeyTypes.size());
                options.FromColumns = TKeyColumnValues(from.data(), from.size());
                options.ToColumns = TKeyColumnValues(to.data(), to.size());
                ui32 flags = provider.ConsumeBool()
                    ? TReadRangeOptions::TFlags::ExcludeInitValue
                    : TReadRangeOptions::TFlags::IncludeInitValue;
                flags |= provider.ConsumeBool()
                    ? TReadRangeOptions::TFlags::ExcludeTermValue
                    : TReadRangeOptions::TFlags::IncludeTermValue;
                options.Flags = builder.TProgramBuilder::NewDataLiteral<ui32>(flags);
                options.ItemsLimit = builder.TProgramBuilder::NewDataLiteral<ui64>(provider.ConsumeIntegralInRange<ui64>(0, 32));
                options.BytesLimit = builder.TProgramBuilder::NewDataLiteral<ui64>(provider.ConsumeIntegralInRange<ui64>(0, 512));
                if (provider.ConsumeBool()) {
                    options.Reverse = builder.TProgramBuilder::NewDataLiteral<bool>(provider.ConsumeBool());
                }
                program = builder.Append(program, builder.SetResult("r", builder.SelectRange(tableId, rangeKeyTypesRef, columnsRef, options)));
                break;
            }
            case 2: {
                auto key = MakeKey(builder, provider);
                const TKeyColumnValues keyRef(key.data(), key.size());
                auto update = builder.GetUpdateRowBuilder();
                update.SetColumn(2, NScheme::TTypeInfo(NUdf::TDataType<ui64>::Id),
                    builder.TProgramBuilder::NewDataLiteral<ui64>(provider.ConsumeIntegral<ui64>()));
                if (provider.ConsumeBool()) {
                    update.EraseColumn(3);
                }
                program = builder.Append(program, builder.UpdateRow(tableId, keyTypesRef, keyRef, update));
                break;
            }
            case 3: {
                auto key = MakeKey(builder, provider);
                const TKeyColumnValues keyRef(key.data(), key.size());
                program = builder.Append(program, builder.EraseRow(tableId, keyTypesRef, keyRef));
                break;
            }
        }

        auto* root = builder.Build(program, TKikimrProgramBuilder::TBindFlags::DisableOptimization).GetNode();
        const TString serialized = SerializeNode(root, env);
        TNode* parsedRoot = DeserializeNode(serialized, env);
        TExploringNodeVisitor explorer;
        explorer.Walk(parsedRoot, env);
        auto keys = ExtractTableKeys(explorer, env);
        Y_ABORT_UNLESS(keys.size() == 1);
        CheckDesc(*keys.front());
    }

    return 0;
}
