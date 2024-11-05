#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TBuildSnapshotCommand
    : public TTypedCommand<NApi::TBuildSnapshotOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TBuildSnapshotCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TBuildMasterSnapshotsCommand
    : public TTypedCommand<NApi::TBuildMasterSnapshotsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TBuildMasterSnapshotsCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetMasterConsistentStateCommand
    : public TTypedCommand<NApi::TGetMasterConsistentStateOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetMasterConsistentStateCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TExitReadOnlyCommand
    : public TTypedCommand<NApi::TExitReadOnlyOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TExitReadOnlyCommand);

    static void Register(TRegistrar registrar);

private:
    NHydra::TCellId CellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMasterExitReadOnlyCommand
    : public TTypedCommand<NApi::TMasterExitReadOnlyOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TMasterExitReadOnlyCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDiscombobulateNonvotingPeersCommand
    : public TTypedCommand<NApi::TDiscombobulateNonvotingPeersOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TDiscombobulateNonvotingPeersCommand);

    static void Register(TRegistrar registrar);

private:
    NHydra::TCellId CellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSwitchLeaderCommand
    : public TTypedCommand<NApi::TSwitchLeaderOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSwitchLeaderCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TResetStateHashCommand);

    static void Register(TRegistrar registrar);

private:
    NHydra::TCellId CellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class THealExecNodeCommand
    : public TTypedCommand<NApi::THealExecNodeOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(THealExecNodeCommand);

    static void Register(TRegistrar registrar);

private:
    TString Address_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendCoordinatorCommand
    : public TTypedCommand<NApi::TSuspendCoordinatorOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSuspendCoordinatorCommand);

    static void Register(TRegistrar registrar);

private:
    NObjectClient::TCellId CoordinatorCellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeCoordinatorCommand
    : public TTypedCommand<NApi::TResumeCoordinatorOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TResumeCoordinatorCommand);

    static void Register(TRegistrar registrar);

private:
    NObjectClient::TCellId CoordinatorCellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TMigrateReplicationCardsCommand
    : public TTypedCommand<NApi::TMigrateReplicationCardsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TMigrateReplicationCardsCommand);

    static void Register(TRegistrar registrar);

private:
    NObjectClient::TCellId ChaosCellId_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendChaosCellsCommand
    : public TTypedCommand<NApi::TSuspendChaosCellsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSuspendChaosCellsCommand);

    static void Register(TRegistrar registrar);

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeChaosCellsCommand
    : public TTypedCommand<NApi::TResumeChaosCellsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TResumeChaosCellsCommand);

    static void Register(TRegistrar registrar);

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendTabletCellsCommand
    : public TTypedCommand<NApi::TSuspendTabletCellsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TSuspendTabletCellsCommand);

    static void Register(TRegistrar registrar);

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResumeTabletCellsCommand
    : public TTypedCommand<NApi::TResumeTabletCellsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TResumeTabletCellsCommand);

    static void Register(TRegistrar registrar);

private:
    std::vector<NObjectClient::TCellId> CellIds_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAddMaintenanceCommand
    : public TTypedCommand<NApi::TAddMaintenanceOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAddMaintenanceCommand);

    static void Register(TRegistrar registrar);

private:
    NApi::EMaintenanceComponent Component_;
    TString Address_;
    NApi::EMaintenanceType Type_;
    TString Comment_;
    // COMPAT(kvk1920): Compatibility with pre-24.2 HTTP clients.
    bool SupportsPerTargetResponse_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveMaintenanceCommand
    : public TTypedCommand<NApi::TRemoveMaintenanceOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRemoveMaintenanceCommand);

    static void Register(TRegistrar registrar);

private:
    NApi::EMaintenanceComponent Component_;
    TString Address_;
    bool Mine_ = false;
    bool All_ = false;
    std::optional<TString> User_;
    std::optional<NApi::TMaintenanceId> Id_;
    std::optional<std::vector<NApi::TMaintenanceId>> Ids_;
    std::optional<NApi::EMaintenanceType> Type_;
    // COMPAT(kvk1920): Compatibility with pre-24.2 HTTP clients.
    bool SupportsPerTargetResponse_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDisableChunkLocationsCommand
    : public TTypedCommand<NApi::TDisableChunkLocationsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TDisableChunkLocationsCommand);

    static void Register(TRegistrar registrar);

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
    REGISTER_YSON_STRUCT_LITE(TDestroyChunkLocationsCommand);

    static void Register(TRegistrar registrar);

private:
    TString NodeAddress_;
    bool RecoverUnlinkedDisks_;
    std::vector<TGuid> LocationUuids_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TResurrectChunkLocationsCommand
    : public TTypedCommand<NApi::TResurrectChunkLocationsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TResurrectChunkLocationsCommand);

    static void Register(TRegistrar registrar);

private:
    TString NodeAddress_;
    std::vector<TGuid> LocationUuids_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

// Endpoint is necessary for manual configuration regeneration, disk partitioning and node restart.
// Important part of Hot Swap mechanic.
class TRequestRestartCommand
    : public TTypedCommand<NApi::TRequestRestartOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRequestRestartCommand);

    static void Register(TRegistrar registrar);

private:
    TString NodeAddress_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
