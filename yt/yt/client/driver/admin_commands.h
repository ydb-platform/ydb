#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TBuildSnapshotCommand
    : public TTypedCommand<NApi::TBuildSnapshotOptions>
{
public:
    TBuildSnapshotCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TBuildMasterSnapshotsCommand
    : public TTypedCommand<NApi::TBuildMasterSnapshotsOptions>
{
public:
    TBuildMasterSnapshotsCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSwitchLeaderCommand
    : public TTypedCommand<NApi::TSwitchLeaderOptions>
{
public:
    TSwitchLeaderCommand();

private:
    NHydra::TCellId CellId_;
    TString NewLeaderAddress_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResetStateHashCommand
    : public TTypedCommand<NApi::TResetStateHashOptions>
{
public:
    TResetStateHashCommand();

private:
    NHydra::TCellId CellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class THealExecNodeCommand
    : public TTypedCommand<NApi::THealExecNodeOptions>
{
public:
    THealExecNodeCommand();

private:
    TString Address_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendCoordinatorCommand
    : public TTypedCommand<NApi::TSuspendCoordinatorOptions>
{
public:
    TSuspendCoordinatorCommand();

private:
    NObjectClient::TCellId CoordinatorCellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeCoordinatorCommand
    : public TTypedCommand<NApi::TResumeCoordinatorOptions>
{
public:
    TResumeCoordinatorCommand();

private:
    NObjectClient::TCellId CoordinatorCellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMigrateReplicationCardsCommand
    : public TTypedCommand<NApi::TMigrateReplicationCardsOptions>
{
public:
    TMigrateReplicationCardsCommand();

private:
    NObjectClient::TCellId ChaosCellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendChaosCellsCommand
    : public TTypedCommand<NApi::TSuspendChaosCellsOptions>
{
public:
    TSuspendChaosCellsCommand();

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeChaosCellsCommand
    : public TTypedCommand<NApi::TResumeChaosCellsOptions>
{
public:
    TResumeChaosCellsCommand();

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendTabletCellsCommand
    : public TTypedCommand<NApi::TSuspendTabletCellsOptions>
{
public:
    TSuspendTabletCellsCommand();

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeTabletCellsCommand
    : public TTypedCommand<NApi::TResumeTabletCellsOptions>
{
public:
    TResumeTabletCellsCommand();

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAddMaintenanceCommand
    : public TTypedCommand<NApi::TAddMaintenanceOptions>
{
public:
    TAddMaintenanceCommand();

private:
    NApi::EMaintenanceComponent Component_;
    TString Address_;
    NApi::EMaintenanceType Type_;
    TString Comment_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveMaintenanceCommand
    : public TTypedCommand<NApi::TRemoveMaintenanceOptions>
{
public:
    TRemoveMaintenanceCommand();

private:
    NApi::EMaintenanceComponent Component_;
    TString Address_;
    bool Mine_ = false;
    bool All_ = false;
    std::optional<TString> User_;
    std::optional<NApi::TMaintenanceId> Id_;
    std::optional<std::vector<NApi::TMaintenanceId>> Ids_;
    std::optional<NApi::EMaintenanceType> Type_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDisableChunkLocationsCommand
    : public TTypedCommand<NApi::TDisableChunkLocationsOptions>
{
public:
   TDisableChunkLocationsCommand();

private:
    TString NodeAddress_;
    std::vector<TGuid> LocationUuids_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDestroyChunkLocationsCommand
    : public TTypedCommand<NApi::TDestroyChunkLocationsOptions>
{
public:
   TDestroyChunkLocationsCommand();

private:
    TString NodeAddress_;
    std::vector<TGuid> LocationUuids_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResurrectChunkLocationsCommand
    : public TTypedCommand<NApi::TResurrectChunkLocationsOptions>
{
public:
   TResurrectChunkLocationsCommand();

private:
    TString NodeAddress_;
    std::vector<TGuid> LocationUuids_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

// Endpoint is necessary for manual configuration regeneration, disk partitioning and node restart.
// Important part of Hot Swap mechanic.
class TRequestRebootCommand
    : public TTypedCommand<NApi::TRequestRebootOptions>
{
public:
   TRequestRebootCommand();

private:
    TString NodeAddress_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
