#include "admin_commands.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDriver {

using namespace NYTree;
using namespace NConcurrency;
using namespace NChaosClient;
using namespace NObjectClient;

using NApi::TMaintenanceId;
using NApi::TMaintenanceFilter;
using NApi::EMaintenanceType;
using NApi::TMaintenanceCounts;
using NApi::TMaintenanceCountsPerTarget;
using NApi::TMaintenanceIdPerTarget;

////////////////////////////////////////////////////////////////////////////////

void TBuildSnapshotCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<NHydra::TCellId>(
        "cell_id",
        [] (TThis* command) -> auto& {
            return command->Options.CellId;
        });

    registrar.ParameterWithUniversalAccessor<bool>(
        "set_read_only",
        [] (TThis* command) -> auto& {
            return command->Options.SetReadOnly;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "wait_for_snapshot_completion",
        [] (TThis* command) -> auto& {
            return command->Options.WaitForSnapshotCompletion;
        })
        .Optional(/*init*/ false);
}

void TBuildSnapshotCommand::DoExecute(ICommandContextPtr context)
{
    auto snapshotId = WaitFor(context->GetClient()->BuildSnapshot(Options))
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("snapshot_id").Value(snapshotId)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TBuildMasterSnapshotsCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<bool>(
        "set_read_only",
        [] (TThis* command) -> auto& {
            return command->Options.SetReadOnly;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "wait_for_snapshot_completion",
        [] (TThis* command) -> auto& {
            return command->Options.WaitForSnapshotCompletion;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "retry",
        [] (TThis* command) -> auto& {
            return command->Options.Retry;
        })
        .Optional(/*init*/ false);
}

void TBuildMasterSnapshotsCommand::DoExecute(ICommandContextPtr context)
{
    auto cellIdToSnapshotId = WaitFor(context->GetClient()->BuildMasterSnapshots(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .DoListFor(cellIdToSnapshotId, [=] (TFluentList fluent, const auto& pair) {
            fluent
                .Item().BeginMap()
                    .Item("cell_id").Value(pair.first)
                    .Item("snapshot_id").Value(pair.second)
                .EndMap();
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TGetMasterConsistentStateCommand::Register(TRegistrar /*registrar*/)
{ }

void TGetMasterConsistentStateCommand::DoExecute(ICommandContextPtr context)
{
    auto cellIdToConsistentState = WaitFor(context->GetClient()->GetMasterConsistentState(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .DoListFor(cellIdToConsistentState, [=] (TFluentList fluent, const auto& pair) {
            fluent
                .Item().BeginMap()
                    .Item("cell_id").Value(pair.first)
                    .Item("sequence_number").Value(pair.second.SequenceNumber)
                    .Item("segment_id").Value(pair.second.SegmentId)
                .EndMap();
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TExitReadOnlyCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_id", &TThis::CellId_);
}

void TExitReadOnlyCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ExitReadOnly(CellId_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TMasterExitReadOnlyCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<bool>(
        "retry",
        [] (TThis* command) -> auto& {
            return command->Options.Retry;
        })
        .Optional(/*init*/ false);
}

void TMasterExitReadOnlyCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->MasterExitReadOnly(Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TDiscombobulateNonvotingPeersCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_id", &TThis::CellId_);
}

void TDiscombobulateNonvotingPeersCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->DiscombobulateNonvotingPeers(CellId_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TSwitchLeaderCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_id", &TThis::CellId_);
    registrar.Parameter("new_leader_address", &TThis::NewLeaderAddress_);
}

void TSwitchLeaderCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SwitchLeader(CellId_, NewLeaderAddress_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TResetStateHashCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_id", &TThis::CellId_);

    registrar.ParameterWithUniversalAccessor<std::optional<ui64>>(
        "new_state_hash",
        [] (TThis* command) -> auto& {
            return command->Options.NewStateHash;
        })
        .Optional(/*init*/ false);
}

void TResetStateHashCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResetStateHash(CellId_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void THealExecNodeCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address_);

    registrar.ParameterWithUniversalAccessor<std::vector<TString>>(
        "locations",
        [] (TThis* command) -> auto& {
            return command->Options.Locations;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::vector<TString>>(
        "alert_types_to_reset",
        [] (TThis* command) -> auto& {
            return command->Options.AlertTypesToReset;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force_reset",
        [] (TThis* command) -> auto& {
            return command->Options.ForceReset;
        })
        .Optional(/*init*/ false);
}

void THealExecNodeCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->HealExecNode(Address_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendCoordinatorCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("coordinator_cell_id", &TThis::CoordinatorCellId_);
}

void TSuspendCoordinatorCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendCoordinator(CoordinatorCellId_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TResumeCoordinatorCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("coordinator_cell_id", &TThis::CoordinatorCellId_);
}

void TResumeCoordinatorCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeCoordinator(CoordinatorCellId_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TMigrateReplicationCardsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("chaos_cell_id", &TThis::ChaosCellId_);

    registrar.ParameterWithUniversalAccessor<TCellId>(
        "destination_cell_id",
        [] (TThis* command) -> auto& {
            return command->Options.DestinationCellId;
        })
        .Optional(/*init*/ false);
    registrar.ParameterWithUniversalAccessor<std::vector<NChaosClient::TReplicationCardId>>(
        "replication_card_ids",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicationCardIds;
        })
        .Optional(/*init*/ false);
}

void TMigrateReplicationCardsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->MigrateReplicationCards(ChaosCellId_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendChaosCellsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_ids", &TThis::CellIds_);
}

void TSuspendChaosCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendChaosCells(CellIds_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TResumeChaosCellsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_ids", &TThis::CellIds_);
}

void TResumeChaosCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeChaosCells(CellIds_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendTabletCellsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_ids", &TThis::CellIds_);
}

void TSuspendTabletCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendTabletCells(CellIds_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TResumeTabletCellsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_ids", &TThis::CellIds_);
}

void TResumeTabletCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeTabletCells(CellIds_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TAddMaintenanceCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("component", &TThis::Component_);
    registrar.Parameter("address", &TThis::Address_);
    registrar.Parameter("type", &TThis::Type_);
    registrar.Parameter("comment", &TThis::Comment_);

    // COMPAT(kvk1920): For compatibility with pre-24.2 HTTP clients.
    registrar.Parameter("supports_per_target_response", &TThis::SupportsPerTargetResponse_)
        .Default(false);
}

void TAddMaintenanceCommand::DoExecute(ICommandContextPtr context)
{
    auto response = WaitFor(context->GetClient()->AddMaintenance(
        Component_,
        Address_,
        Type_,
        Comment_,
        Options))
        .ValueOrThrow();

    // COMPAT(kvk1920): Compatibility with pre-24.2 HTTP clients.
    if (!SupportsPerTargetResponse_) {
        ProduceSingleOutputValue(
            context,
            "id",
            response.size() == 1 ? response.begin()->second : TMaintenanceId{});
        return;
    }

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .DoFor(response, [] (auto fluent, const std::pair<std::string, TMaintenanceId>& targetAndId) {
                    fluent.Item(targetAndId.first).Value(targetAndId.second);
                })
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveMaintenanceCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("component", &TThis::Component_);
    registrar.Parameter("address", &TThis::Address_);

    registrar.Parameter("id", &TThis::Id_)
        .Default();

    registrar.Parameter("ids", &TThis::Ids_)
        .Default();

    registrar.Parameter("user", &TThis::User_)
        .Default();
    registrar.Parameter("mine", &TThis::Mine_)
        .Optional();

    registrar.Parameter("type", &TThis::Type_)
        .Optional();

    registrar.Parameter("all", &TThis::All_)
        .Optional();

    registrar.Parameter("supports_per_target_response", &TThis::SupportsPerTargetResponse_)
        .Default(false);

    registrar.Postprocessor([&] (TThis* config) {
        THROW_ERROR_EXCEPTION_IF(config->Id_ && config->Ids_,
            "At most one of {\"id\", \"ids\"} can be specified at the same time");

        THROW_ERROR_EXCEPTION_IF(config->Ids_ && config->Ids_->empty(),
            "\"ids\" must not be empty if specified");

        THROW_ERROR_EXCEPTION_IF(!config->Id_ && !config->Ids_ && !config->User_ && !config->Mine_ && !config->Type_ && !config->All_,
            "\"all\" must be specified explicitly");

        THROW_ERROR_EXCEPTION_IF(config->Mine_ && config->User_,
            "Cannot specify both \"user\" and \"mine\"");

        THROW_ERROR_EXCEPTION_IF(config->All_ && (config->User_ || config->Mine_ || config->Type_ || config->Id_ || config->Ids_),
            "\"all\" cannot be used with other options");
    });
}

void TRemoveMaintenanceCommand::DoExecute(ICommandContextPtr context)
{
    TMaintenanceFilter filter;

    if (Id_) {
        filter.Ids = {*Id_};
    } else if (Ids_) {
        filter.Ids = *Ids_;
    }

    if (Mine_) {
        filter.User = TMaintenanceFilter::TByUser::TMine{};
    } else if (User_) {
        filter.User = *User_;
    } else {
        filter.User = TMaintenanceFilter::TByUser::TAll{};
    }

    if (Type_) {
        filter.Type = *Type_;
    }

    auto response = WaitFor(context->GetClient()->RemoveMaintenance(
        Component_,
        Address_,
        filter,
        Options))
        .ValueOrThrow();

    auto produceCounts = [] (auto fluent, const TMaintenanceCounts& counts) {
        fluent
            .BeginMap()
                .DoFor(
                    TEnumTraits<NApi::EMaintenanceType>::GetDomainValues(),
                    [&] (auto fluent, EMaintenanceType type) {
                        if (counts[type] > 0) {
                            fluent.Item(Format("%lv", type)).Value(counts[type]);
                        }
                    })
            .EndMap();
    };

    // COMPAT(kvk1920): Compatibility with pre-24.2 HTTP clients.
    if (!SupportsPerTargetResponse_) {
        TMaintenanceCounts totalCounts;
        for (const auto& [target, counts] : response) {
            for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                totalCounts[type] += counts[type];
            }
        }
        ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
            produceCounts(BuildYsonFluently(consumer), totalCounts);
        });
        return;
    }

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .DoFor(
                    response,
                    [&] (auto fluent, const std::pair<std::string, TMaintenanceCounts>& targetWithCounts) {
                        produceCounts(fluent.Item(targetWithCounts.first), targetWithCounts.second);
                    })
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDisableChunkLocationsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("node_address", &TThis::NodeAddress_);
    registrar.Parameter("location_uuids", &TThis::LocationUuids_)
        .Default();
}

void TDisableChunkLocationsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->DisableChunkLocations(NodeAddress_, LocationUuids_, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("location_uuids")
                .BeginList()
                    .DoFor(result.LocationUuids, [&] (TFluentList fluent, const auto& uuid) {
                        fluent.Item().Value(uuid);
                    })
                .EndList()
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TDestroyChunkLocationsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("node_address", &TThis::NodeAddress_);
    registrar.Parameter("recover_unlinked_disks", &TThis::RecoverUnlinkedDisks_)
        .Default(false);
    registrar.Parameter("location_uuids", &TThis::LocationUuids_)
        .Default();
}

void TDestroyChunkLocationsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->DestroyChunkLocations(
        NodeAddress_,
        RecoverUnlinkedDisks_,
        LocationUuids_,
        Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("location_uuids")
                .BeginList()
                    .DoFor(result.LocationUuids, [&] (TFluentList fluent, const auto& uuid) {
                        fluent.Item().Value(uuid);
                    })
                .EndList()
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TResurrectChunkLocationsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("node_address", &TThis::NodeAddress_);
    registrar.Parameter("location_uuids", &TThis::LocationUuids_)
        .Default();
}

void TResurrectChunkLocationsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ResurrectChunkLocations(NodeAddress_, LocationUuids_, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("location_uuids")
                .BeginList()
                    .DoFor(result.LocationUuids, [&] (TFluentList fluent, const auto& uuid) {
                        fluent.Item().Value(uuid);
                    })
                .EndList()
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TRequestRestartCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("node_address", &TThis::NodeAddress_);
}

void TRequestRestartCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RequestRestart(NodeAddress_, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
