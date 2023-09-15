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

////////////////////////////////////////////////////////////////////////////////

TBuildSnapshotCommand::TBuildSnapshotCommand()
{
    RegisterParameter("cell_id", Options.CellId);
    RegisterParameter("set_read_only", Options.SetReadOnly)
        .Optional();
    RegisterParameter("wait_for_snapshot_completion", Options.WaitForSnapshotCompletion)
        .Optional();
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

TBuildMasterSnapshotsCommand::TBuildMasterSnapshotsCommand()
{
    RegisterParameter("set_read_only", Options.SetReadOnly)
        .Optional();
    RegisterParameter("wait_for_snapshot_completion", Options.WaitForSnapshotCompletion)
        .Optional();
    RegisterParameter("retry", Options.Retry)
        .Optional();
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
        })
    );
}

////////////////////////////////////////////////////////////////////////////////

TExitReadOnlyCommand::TExitReadOnlyCommand()
{
    RegisterParameter("cell_id", CellId_);
}

void TExitReadOnlyCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ExitReadOnly(CellId_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TMasterExitReadOnlyCommand::TMasterExitReadOnlyCommand()
{
    RegisterParameter("retry", Options.Retry)
        .Optional();
}

void TMasterExitReadOnlyCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->MasterExitReadOnly(Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TSwitchLeaderCommand::TSwitchLeaderCommand()
{
    RegisterParameter("cell_id", CellId_);
    RegisterParameter("new_leader_address", NewLeaderAddress_);
}

void TSwitchLeaderCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SwitchLeader(CellId_, NewLeaderAddress_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TResetStateHashCommand::TResetStateHashCommand()
{
    RegisterParameter("cell_id", CellId_);
    RegisterParameter("new_state_hash", Options.NewStateHash)
        .Optional();
}

void TResetStateHashCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResetStateHash(CellId_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

THealExecNodeCommand::THealExecNodeCommand()
{
    RegisterParameter("address", Address_);
    RegisterParameter("locations", Options.Locations)
        .Optional();
    RegisterParameter("alert_types_to_reset", Options.AlertTypesToReset)
        .Optional();
    RegisterParameter("force_reset", Options.ForceReset)
        .Optional();
}

void THealExecNodeCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->HealExecNode(Address_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TSuspendCoordinatorCommand::TSuspendCoordinatorCommand()
{
    RegisterParameter("coordinator_cell_id", CoordinatorCellId_);
}

void TSuspendCoordinatorCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendCoordinator(CoordinatorCellId_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TResumeCoordinatorCommand::TResumeCoordinatorCommand()
{
    RegisterParameter("coordinator_cell_id", CoordinatorCellId_);
}

void TResumeCoordinatorCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeCoordinator(CoordinatorCellId_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TMigrateReplicationCardsCommand::TMigrateReplicationCardsCommand()
{
    RegisterParameter("chaos_cell_id", ChaosCellId_);
    RegisterParameter("destination_cell_id", Options.DestinationCellId)
        .Optional();
    RegisterParameter("replication_card_ids", Options.ReplicationCardIds)
        .Optional();
}

void TMigrateReplicationCardsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->MigrateReplicationCards(ChaosCellId_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TSuspendChaosCellsCommand::TSuspendChaosCellsCommand()
{
    RegisterParameter("cell_ids", CellIds_);
}

void TSuspendChaosCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendChaosCells(CellIds_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TResumeChaosCellsCommand::TResumeChaosCellsCommand()
{
    RegisterParameter("cell_ids", CellIds_);
}

void TResumeChaosCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeChaosCells(CellIds_))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TSuspendTabletCellsCommand::TSuspendTabletCellsCommand()
{
    RegisterParameter("cell_ids", CellIds_);
}

void TSuspendTabletCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendTabletCells(CellIds_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TResumeTabletCellsCommand::TResumeTabletCellsCommand()
{
    RegisterParameter("cell_ids", CellIds_);
}

void TResumeTabletCellsCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeTabletCells(CellIds_, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAddMaintenanceCommand::TAddMaintenanceCommand()
{
    RegisterParameter("component", Component_);
    RegisterParameter("address", Address_);
    RegisterParameter("type", Type_);
    RegisterParameter("comment", Comment_);
}

void TAddMaintenanceCommand::DoExecute(ICommandContextPtr context)
{
    auto id = WaitFor(context->GetClient()->AddMaintenance(
        Component_,
        Address_,
        Type_,
        Comment_,
        Options))
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "id", id);
}

////////////////////////////////////////////////////////////////////////////////

TRemoveMaintenanceCommand::TRemoveMaintenanceCommand()
{
    RegisterParameter("component", Component_);
    RegisterParameter("address", Address_);

    RegisterParameter("id", Id_)
        .Default();

    RegisterParameter("ids", Ids_)
        .Default();

    RegisterParameter("user", User_)
        .Default();
    RegisterParameter("mine", Mine_)
        .Optional();

    RegisterParameter("type", Type_)
        .Optional();

    RegisterParameter("all", All_)
        .Optional();

    RegisterPostprocessor([&] {
        THROW_ERROR_EXCEPTION_IF(Id_ && Ids_,
            "At most one of {\"id\", \"ids\"} can be specified at the same time");

        THROW_ERROR_EXCEPTION_IF(Ids_ && Ids_->empty(),
            "\"ids\" must not be empty if specified");

        THROW_ERROR_EXCEPTION_IF(!Id_ && !Ids_ && !User_ && !Mine_ && !Type_ && !All_,
            "\"all\" must be specified explicitly");

        THROW_ERROR_EXCEPTION_IF(Mine_ && User_,
            "Cannot specify both \"user\" and \"mine\"");

        THROW_ERROR_EXCEPTION_IF(All_ && (User_ || Mine_ || Type_ || Id_ || Ids_),
            "\"all\" cannot be used with other options");
    });
}

namespace {

TStringBuf MaintenanceTypeToString(NApi::EMaintenanceType type)
{
    switch (type) {
        case NApi::EMaintenanceType::Ban:
            return "ban";
        case NApi::EMaintenanceType::Decommission:
            return "decommission";
        case NApi::EMaintenanceType::DisableSchedulerJobs:
            return "disable_scheduler_jobs";
        case NApi::EMaintenanceType::DisableWriteSessions:
            return "disable_write_sessions";
        case NApi::EMaintenanceType::DisableTabletCells:
            return "disable_tablet_cells";
        case NApi::EMaintenanceType::PendingRestart:
            return "pending_restart";
        default:
            YT_ABORT();
    }
}

} // namespace

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

    auto removedMaintenanceCounts = WaitFor(context->GetClient()->RemoveMaintenance(
        Component_,
        Address_,
        filter,
        Options))
        .ValueOrThrow();

    ProduceOutput(context, [&] (NYson::IYsonConsumer* consumer) {
        auto fluent = BuildYsonFluently(consumer)
            .BeginMap();
        for (auto type : TEnumTraits<NApi::EMaintenanceType>::GetDomainValues()) {
            if (removedMaintenanceCounts[type] > 0) {
                fluent = fluent.Item(MaintenanceTypeToString(type)).Value(removedMaintenanceCounts[type]);
            }
        }
        fluent.EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

TDisableChunkLocationsCommand::TDisableChunkLocationsCommand()
{
    RegisterParameter("node_address", NodeAddress_);
    RegisterParameter("location_uuids", LocationUuids_)
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

TDestroyChunkLocationsCommand::TDestroyChunkLocationsCommand()
{
    RegisterParameter("node_address", NodeAddress_);
    RegisterParameter("location_uuids", LocationUuids_)
        .Default();
}

void TDestroyChunkLocationsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->DestroyChunkLocations(NodeAddress_, LocationUuids_, Options))
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

TResurrectChunkLocationsCommand::TResurrectChunkLocationsCommand()
{
    RegisterParameter("node_address", NodeAddress_);
    RegisterParameter("location_uuids", LocationUuids_)
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

TRequestRestartCommand::TRequestRestartCommand()
{
    RegisterParameter("node_address", NodeAddress_);
}

void TRequestRestartCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RequestRestart(NodeAddress_, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
