#include "table_client.h"

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NYTree;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

void TTableBackupManifest::Register(TRegistrar registrar)
{
    registrar.Parameter("source_path", &TThis::SourcePath);
    registrar.Parameter("destination_path", &TThis::DestinationPath);
    registrar.Parameter("ordered_mode", &TThis::OrderedMode)
        .Default(EOrderedTableBackupMode::Exact);
}

////////////////////////////////////////////////////////////////////////////////

void TBackupManifest::Register(TRegistrar registrar)
{
    registrar.Parameter("clusters", &TThis::Clusters);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateSecondaryIndex::Register(TRegistrar registrar)
{
    registrar.Parameter("kind", &TThis::Kind);
    registrar.Parameter("index_replication_card_id", &TThis::IndexReplicationCardId);
    registrar.Parameter("correspondence", &TThis::Correspondence)
        .Default(ETableToIndexCorrespondence::Invalid);
    registrar.Parameter("predicate", &TThis::Predicate)
        .Optional();
    registrar.Parameter("unfolded_columns", &TThis::UnfoldedColumns)
        .Optional();
    registrar.Parameter("evaluated_columns_schema", &TThis::EvaluatedColumnsSchema)
        .Optional();

    registrar.Postprocessor([] (TCreateSecondaryIndex* alteration) {
        THROW_ERROR_EXCEPTION_IF(
            alteration->UnfoldedColumns.has_value() != (alteration->Kind == ESecondaryIndexKind::Unfolding),
            "\"unfolded_columns\" option is mutually dependent with %Qlv index kind",
            ESecondaryIndexKind::Unfolding);

        THROW_ERROR_EXCEPTION_IF(alteration->Correspondence == ETableToIndexCorrespondence::Unknown,
            "Cannot create index with correspondence %Qlv",
            ETableToIndexCorrespondence::Unknown);
    });
}

void TProgressSecondaryIndexCorrespondence::Register(TRegistrar registrar)
{
    registrar.Parameter("index_replication_card_id", &TThis::IndexReplicationCardId);
    registrar.Parameter("new_correspondence", &TThis::NewCorrespondence)
        .Default(ETableToIndexCorrespondence::Invalid);

    registrar.Postprocessor([] (TProgressSecondaryIndexCorrespondence* alteration) {
        THROW_ERROR_EXCEPTION_UNLESS(
            alteration->NewCorrespondence == ETableToIndexCorrespondence::Injective ||
            alteration->NewCorrespondence == ETableToIndexCorrespondence::Bijective,
            "Cannot progress to %Qlv correspondence, expected %Qlv or %Qlv",
            alteration->NewCorrespondence,
            ETableToIndexCorrespondence::Injective,
            ETableToIndexCorrespondence::Bijective);
    });
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMultiTablePartition& partition, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("table_ranges").Value(partition.TableRanges)
            .DoIf(static_cast<bool>(partition.Cookie), [&] (TFluentMap fluent) {
                auto ysonString = NYson::ConvertToYsonString(partition.Cookie);
                fluent.Item("cookie").Value(ysonString.AsStringBuf());
            })
            .Item("aggregate_statistics").Value(partition.AggregateStatistics)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMultiTablePartitions& partitions, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("partitions").Value(partitions.Partitions)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
