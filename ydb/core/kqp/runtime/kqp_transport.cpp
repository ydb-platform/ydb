#include "kqp_transport.h"
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NKikimr {
namespace NKqp {

using namespace NMiniKQL;
using namespace NYql;

TKqpProtoBuilder::TSelfHosted::TSelfHosted(const IFunctionRegistry& funcRegistry)
    : Alloc(__LOCATION__, TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
    , TypeEnv(Alloc)
    , MemInfo("KqpProtoBuilder")
    , HolderFactory(Alloc.Ref(), MemInfo)
{
}

TKqpProtoBuilder::TKqpProtoBuilder(const IFunctionRegistry& funcRegistry)
    : SelfHosted(MakeHolder<TSelfHosted>(funcRegistry))
{
    Alloc = &SelfHosted->Alloc;
    TypeEnv = &SelfHosted->TypeEnv;
    HolderFactory = &SelfHosted->HolderFactory;

    Alloc->Release();
}

TKqpProtoBuilder::TKqpProtoBuilder(TScopedAlloc* alloc, TTypeEnvironment* typeEnv, THolderFactory* holderFactory)
    : Alloc(alloc)
    , TypeEnv(typeEnv)
    , HolderFactory(holderFactory)
{
}

TKqpProtoBuilder::~TKqpProtoBuilder() {
    if (SelfHosted) {
        SelfHosted->Alloc.Acquire();
    }
}

void TKqpProtoBuilder::BuildYdbResultSet(
    Ydb::ResultSet& resultSet,
    TVector<NYql::NDq::TDqSerializedBatch>&& data,
    NKikimr::NMiniKQL::TType* mkqlSrcRowType,
    const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints)
{
    YQL_ENSURE(mkqlSrcRowType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct);
    const auto* mkqlSrcRowStructType = static_cast<const TStructType*>(mkqlSrcRowType);

    TColumnOrder order = columnHints ? TColumnOrder(*columnHints) : TColumnOrder{};
    for (ui32 idx = 0; idx < mkqlSrcRowStructType->GetMembersCount(); ++idx) {
        auto* column = resultSet.add_columns();
        ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? idx : (*columnOrder)[idx];
        column->set_name(TString(columnHints && columnHints->size() ? order.at(idx).LogicalName : mkqlSrcRowStructType->GetMemberName(memberIndex)));
        ExportTypeToProto(mkqlSrcRowStructType->GetMemberType(memberIndex), *column->mutable_type());
    }

    THolder<TGuard<TScopedAlloc>> guard;
    if (SelfHosted) {
        guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
    }

    auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    if (!data.empty()) {
        transportVersion = static_cast<NDqProto::EDataTransportVersion>(data.front().Proto.GetTransportVersion());
    }
    NDq::TDqDataSerializer dataSerializer(*TypeEnv, *HolderFactory, transportVersion);
    for (auto& part : data) {
        if (part.ChunkCount()) {
            TUnboxedValueBatch rows(mkqlSrcRowType);
            dataSerializer.Deserialize(std::move(part), mkqlSrcRowType, rows);
            rows.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                ExportValueToProto(mkqlSrcRowType, value, *resultSet.add_rows(), columnOrder);
            });
        }
    }
}

void TKqpProtoBuilder::BuildArrow(
    std::shared_ptr<arrow::RecordBatch>& batch,
    TVector<NYql::NDq::TDqSerializedBatch>&& data,
    NKikimr::NMiniKQL::TType* mkqlSrcRowType,
    const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints)
{
    YQL_ENSURE(mkqlSrcRowType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct);
    const auto* mkqlSrcRowStructType = static_cast<const TStructType*>(mkqlSrcRowType);

    std::vector<std::pair<TString, NScheme::TTypeInfo>> columns;
    std::set<std::string> notNullColumns;

    TColumnOrder order = columnHints ? TColumnOrder(*columnHints) : TColumnOrder{};
    for (ui32 idx = 0; idx < mkqlSrcRowStructType->GetMembersCount(); ++idx) {
        ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? idx : (*columnOrder)[idx];

        auto name = TString(columnHints && columnHints->size()
            ? order.at(idx).LogicalName
            : mkqlSrcRowStructType->GetMemberName(memberIndex));

        auto* type = mkqlSrcRowStructType->GetMemberType(memberIndex);
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromMiniKQLType(type);

        columns.emplace_back(std::move(name), std::move(typeInfo));
        if (type->GetKind() != TType::EKind::Pg && !type->IsOptional()) {
            notNullColumns.insert(name);
        }
    }

    NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::UNCOMPRESSED, notNullColumns);
    YQL_ENSURE(batchBuilder.Start(columns).ok());

    THolder<TGuard<TScopedAlloc>> guard;
    if (SelfHosted) {
        guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
    }

    auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    if (!data.empty()) {
        transportVersion = static_cast<NDqProto::EDataTransportVersion>(data.front().Proto.GetTransportVersion());
    }

    NDq::TDqDataSerializer dataSerializer(*TypeEnv, *HolderFactory, transportVersion);

    for (auto& part : data) {
        if (!part.ChunkCount()) {
            continue;
        }

        TUnboxedValueBatch rows(mkqlSrcRowType);
        dataSerializer.Deserialize(std::move(part), mkqlSrcRowType, rows);

        rows.ForEachRow([&](const NUdf::TUnboxedValue& value) {
            TVector<TCell> cells(columns.size());
            for (ui32 i = 0; i < cells.size(); ++i) {
                const auto& unboxedValue = value.GetElement(i);
                const auto& [colName, colType] = columns[i];

                if (!notNullColumns.contains(colName) && !unboxedValue) {
                    cells[i] = TCell();
                } else {
                    cells[i] = MakeCell(colType, unboxedValue, *TypeEnv);
                }
            }

            batchBuilder.AddRow(cells);
        });
    }

    batch = batchBuilder.FlushBatch(false);
}

} // namespace NKqp
} // namespace NKikimr
