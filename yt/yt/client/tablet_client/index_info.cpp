#include "index_info.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

namespace NYT::NTabletClient {

using NYT::ToProto;
using NYT::FromProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TUnfoldedColumns::TUnfoldedColumns(std::string tableColumn, std::string indexColumn)
    : TUnfoldedColumns()
{
    // Classes inheriting yson struct lite have nontrivial initialization hidden
    // in their default constructors.
    TableColumn = std::move(tableColumn);
    IndexColumn = std::move(indexColumn);
}

void TUnfoldedColumns::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, TableColumn);
    Persist(context, IndexColumn);
}

void TUnfoldedColumns::Register(TRegistrar registrar)
{
    registrar.Parameter("table_column", &TThis::TableColumn);
    registrar.Parameter("index_column", &TThis::IndexColumn);
}

void ToProto(NProto::TUnfoldedColumns* serialized, const TUnfoldedColumns& original)
{
    ToProto(serialized->mutable_table_column(), original.TableColumn);
    ToProto(serialized->mutable_index_column(), original.IndexColumn);
}

void FromProto(TUnfoldedColumns* original, const NProto::TUnfoldedColumns& serialized)
{
    FromProto(&original->TableColumn, serialized.table_column());
    FromProto(&original->IndexColumn, serialized.index_column());
}

////////////////////////////////////////////////////////////////////////////////

void TIndexInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("kind", &TThis::Kind)
        .Default(ESecondaryIndexKind::FullSync);
    registrar.Parameter("index_object_id", &TThis::IndexObjectId)
        .Default();
    registrar.Parameter("correspondence", &TThis::Correspondence)
        .Default(ETableToIndexCorrespondence::Invalid);
    registrar.Parameter("predicate", &TThis::Predicate)
        .Optional();
    registrar.Parameter("unfolded_column", &TThis::UnfoldedColumns)
        .Optional();
    registrar.Parameter("evaluated_columns_schema", &TThis::EvaluatedColumnsSchema)
        .Optional();
}

void ToProto(NProto::TIndexInfo* serialized, const TIndexInfo& original)
{
    ToProto(serialized->mutable_index_object_id(), original.IndexObjectId);
    serialized->set_index_kind(ToProto(original.Kind));
    serialized->set_index_correspondence(ToProto(original.Correspondence));
    YT_OPTIONAL_TO_PROTO(serialized, predicate, original.Predicate);
    YT_OPTIONAL_TO_PROTO(serialized, unfolded_columns, original.UnfoldedColumns);
    // COMPAT(sabdenovch)
    if (original.UnfoldedColumns &&
        original.UnfoldedColumns->TableColumn == original.UnfoldedColumns->IndexColumn)
    {
        ToProto(serialized->mutable_unfolded_column(), original.UnfoldedColumns->TableColumn);
    }
    if (const auto& evaluatedColumnsSchema = original.EvaluatedColumnsSchema) {
        ToProto(serialized->mutable_evaluated_columns_schema(), evaluatedColumnsSchema);
    }
}

void FromProto(TIndexInfo* original, const NProto::TIndexInfo& serialized)
{
    FromProto(&original->IndexObjectId, serialized.index_object_id());
    FromProto(&original->Kind, serialized.index_kind());
    if (serialized.has_index_correspondence()) {
        FromProto(&original->Correspondence, serialized.index_correspondence());
    } else {
        original->Correspondence = ETableToIndexCorrespondence::Unknown;
    }
    original->Predicate = YT_OPTIONAL_FROM_PROTO(serialized, predicate);
    // COMPAT(sabdenovch)
    if (serialized.has_unfolded_column()) {
        original->UnfoldedColumns = TUnfoldedColumns(
            serialized.unfolded_column(),
            serialized.unfolded_column());
    } else {
        original->UnfoldedColumns = YT_OPTIONAL_FROM_PROTO(serialized, unfolded_columns, TUnfoldedColumns);
    }
    if (serialized.has_evaluated_columns_schema()) {
        original->EvaluatedColumnsSchema = New<NTableClient::TTableSchema>(
            FromProto<NTableClient::TTableSchema>(serialized.evaluated_columns_schema()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
