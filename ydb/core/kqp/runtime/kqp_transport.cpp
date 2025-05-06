#include "kqp_transport.h"
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/kqp_row_builder.h>
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
    Ydb::ResultSet::Type resultSetType,
    const TVector<ui32>* columnOrder,
    const TVector<TString>* columnHints)
{
    YQL_ENSURE(mkqlSrcRowType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct);
    const auto* mkqlSrcRowStructType = static_cast<const TStructType*>(mkqlSrcRowType);

    resultSet.set_type(resultSetType);

    std::vector<std::pair<TString, NScheme::TTypeInfo>> arrowSchema;
    std::set<std::string> arrowNotNullColumns;

    TColumnOrder order = columnHints ? TColumnOrder(*columnHints) : TColumnOrder{};
    for (ui32 idx = 0; idx < mkqlSrcRowStructType->GetMembersCount(); ++idx) {
        auto* column = resultSet.add_columns();
        ui32 memberIndex = (!columnOrder || columnOrder->empty()) ? idx : (*columnOrder)[idx];

        auto columnName = TString(columnHints && columnHints->size() ? order.at(idx).LogicalName : mkqlSrcRowStructType->GetMemberName(memberIndex));
        auto* columnType = mkqlSrcRowStructType->GetMemberType(memberIndex);

        column->set_name(columnName);
        ExportTypeToProto(columnType, *column->mutable_type());

        if (resultSetType == Ydb::ResultSet::ARROW) {
            if (columnType->GetKind() != TType::EKind::Pg && !columnType->IsOptional()) {
                arrowNotNullColumns.insert(columnName);
            }

            NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromMiniKQLType(columnType);
            arrowSchema.emplace_back(std::move(columnName), std::move(typeInfo));
        }
    }

    THolder<TGuard<TScopedAlloc>> guard;
    if (SelfHosted) {
        guard = MakeHolder<TGuard<TScopedAlloc>>(*Alloc);
    }

    auto transportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_VERSION_UNSPECIFIED;
    if (!data.empty()) {
        transportVersion = static_cast<NDqProto::EDataTransportVersion>(data.front().Proto.GetTransportVersion());
    }

    NArrow::TArrowBatchBuilder batchBuilder(arrow::Compression::UNCOMPRESSED, arrowNotNullColumns);
    YQL_ENSURE(batchBuilder.Start(arrowSchema).ok());

    TRowBuilder rowBuilder(arrowSchema.size());

    NDq::TDqDataSerializer dataSerializer(*TypeEnv, *HolderFactory, transportVersion);
    for (auto& part : data) {
        if (part.ChunkCount()) {
            TUnboxedValueBatch rows(mkqlSrcRowType);
            dataSerializer.Deserialize(std::move(part), mkqlSrcRowType, rows);

            switch (resultSetType) {
                case Ydb::ResultSet::UNSPECIFIED:
                    resultSet.set_type(Ydb::ResultSet::MESSAGE);
                case Ydb::ResultSet::MESSAGE:{
                    rows.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                        ExportValueToProto(mkqlSrcRowType, value, *resultSet.add_rows(), columnOrder);
                    });
                    break;
                }
                case Ydb::ResultSet::ARROW:{
                    rows.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                        for (size_t i = 0; i < arrowSchema.size(); ++i) {
                            const auto& [name, type] = arrowSchema[i];
                            rowBuilder.AddCell(i, type, value.GetElement(i), type.GetPgTypeMod(name));
                        }

                        auto cells = rowBuilder.BuildCells();
                        batchBuilder.AddRow(cells);
                    });

                    std::shared_ptr<arrow::RecordBatch> batch = batchBuilder.FlushBatch(false);

                    TString serializedBatch = NArrow::SerializeBatchNoCompression(batch);
                    YQL_ENSURE(serializedBatch);

                    TString serializedSchema = NArrow::SerializeSchema(*batch->schema());
                    YQL_ENSURE(serializedSchema);

                    resultSet.set_data(std::move(serializedBatch));
                    resultSet.mutable_arrow_batch_settings()->set_schema(std::move(serializedSchema));
                    break;
                }
                default:
                    break;
            }
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
