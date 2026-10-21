#include "impl.h"
#include "config.h"
#include "select_groups.h"

#include <algorithm>
#include <vector>

namespace NKikimr::NBsController {

        class TBlobStorageController::TBlobStorageController::TTxConfigCmd
            : public TTransactionBase<TBlobStorageController>
        {
            const TActorId NotifyId;
            const ui64 Cookie;
            const NKikimrBlobStorage::TConfigRequest Cmd;
            const bool SelfHeal;
            const bool GroupLayoutSanitizer;
            std::optional<THostRecordMap> EnforceHostRecords;
            THolder<TEvBlobStorage::TEvControllerConfigResponse> Ev;
            NKikimrBlobStorage::TConfigResponse *Response;
            std::optional<TConfigState> State;
            bool Success = true;
            bool RollbackSuccess = false;
            TString Error;

        public:
            TTxConfigCmd(const NKikimrBlobStorage::TConfigRequest &cmd, const TActorId &notifyId, ui64 cookie,
                    bool selfHeal, bool groupLayoutSanitizer, std::optional<THostRecordMap> enforceHostRecords, TBlobStorageController *controller)
                : TTransactionBase(controller)
                , NotifyId(notifyId)
                , Cookie(cookie)
                , Cmd(cmd)
                , SelfHeal(selfHeal)
                , GroupLayoutSanitizer(groupLayoutSanitizer)
                , EnforceHostRecords(std::move(enforceHostRecords))
                , Ev(new TEvBlobStorage::TEvControllerConfigResponse())
                , Response(Ev->Record.MutableResponse())
            {}

            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_CONFIG_CMD; }

            template<typename TCallback>
            void WrapCommand(TCallback&& callback) {
                auto *status = Response->AddStatus();
                try {
                    callback();
                    status->SetSuccess(true);
                } catch (const TExError& e) {
                    Success = false;
                    Error = e.what();
                    e.FillInStatus(*status);
                } catch (const std::exception& e) {
                    Success = false;
                    Error = TStringBuilder() << "unknown exception: " << e.what();
                    status->SetErrorDescription(Error);
                }
            }

            void Rollback() {
                Y_ENSURE(Success);
                Success = false;
                RollbackSuccess = true;
                Error = "transaction rollback";
            }

            void Finish() {
                Response->SetSuccess(Success);
                Response->SetRollbackSuccess(RollbackSuccess);
                if (!Success) {
                    Response->SetErrorDescription(Error);
                }
            }

            bool IsSoleCommand(const NKikimrBlobStorage::TConfigRequest::TCommand& cmd) {
                switch (cmd.GetCommandCase()) {
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableSelfHeal:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableDonorMode:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetScrubPeriodicity:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetPDiskSpaceMarginPromille:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateSettings:
                        return true;
                    default:
                        return false;
                }
            }

            void ExecuteSoleCommand(const NKikimrBlobStorage::TConfigRequest::TCommand& cmd, TTransactionContext& txc) {
                NIceDb::TNiceDb db(txc.DB);
                switch (cmd.GetCommandCase()) {
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableSelfHeal:
                        Self->SelfHealEnable = cmd.GetEnableSelfHeal().GetEnable();
                        db.Table<Schema::State>().Key(true).Update<Schema::State::SelfHealEnable>(Self->SelfHealEnable);
                        return;

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableDonorMode:
                        Self->DonorMode = cmd.GetEnableDonorMode().GetEnable();
                        db.Table<Schema::State>().Key(true).Update<Schema::State::DonorModeEnable>(Self->DonorMode);
                        return;

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetScrubPeriodicity: {
                        const ui32 seconds = cmd.GetSetScrubPeriodicity().GetScrubPeriodicity();
                        Self->ScrubPeriodicity = TDuration::Seconds(seconds);
                        db.Table<Schema::State>().Key(true).Update<Schema::State::ScrubPeriodicity>(seconds);
                        Self->ScrubState.OnScrubPeriodicityChange();
                        return;
                    }

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetPDiskSpaceMarginPromille: {
                        const ui32 value = cmd.GetSetPDiskSpaceMarginPromille().GetPDiskSpaceMarginPromille();
                        Self->PDiskSpaceMarginPromille = value;
                        db.Table<Schema::State>().Key(true).Update<Schema::State::PDiskSpaceMarginPromille>(value);
                        return;
                    }

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateSettings: {
                        const auto& settings = cmd.GetUpdateSettings();
                        using T = Schema::State;

                        // validate database space thresholds before applying anything
                        auto databaseSpaceBlockColor = Self->DatabaseSpace.GetBlockColor();
                        auto databaseSpaceUnblockColor = Self->DatabaseSpace.GetUnblockColor();
                        for (auto value : settings.GetDatabaseSpaceBlockColor()) {
                            databaseSpaceBlockColor = static_cast<T::DatabaseSpaceBlockColor::Type>(value);
                        }
                        for (auto value : settings.GetDatabaseSpaceUnblockColor()) {
                            databaseSpaceUnblockColor = static_cast<T::DatabaseSpaceUnblockColor::Type>(value);
                        }
                        if (!TDatabaseSpaceTracker::IsValidThresholds(databaseSpaceBlockColor, databaseSpaceUnblockColor)) {
                            throw TExError() << "DatabaseSpaceUnblockColor must not be worse than DatabaseSpaceBlockColor";
                        }

                        for (ui32 value : settings.GetDefaultMaxSlots()) {
                            Self->DefaultMaxSlots = value;
                            db.Table<T>().Key(true).Update<T::DefaultMaxSlots>(Self->DefaultMaxSlots);
                        }
                        for (bool value : settings.GetEnableSelfHeal()) {
                            Self->SelfHealEnable = value;
                            db.Table<T>().Key(true).Update<T::SelfHealEnable>(Self->SelfHealEnable);
                        }
                        for (bool value : settings.GetEnableDonorMode()) {
                            Self->DonorMode = value;
                            db.Table<T>().Key(true).Update<T::DonorModeEnable>(Self->DonorMode);
                            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
                            ev->DonorMode = Self->DonorMode;
                            Self->Send(Self->SelfHealId, ev.release());
                        }
                        for (ui64 value : settings.GetScrubPeriodicitySeconds()) {
                            Self->ScrubPeriodicity = TDuration::Seconds(value);
                            db.Table<T>().Key(true).Update<T::ScrubPeriodicity>(Self->ScrubPeriodicity.Seconds());
                            Self->ScrubState.OnScrubPeriodicityChange();
                        }
                        for (ui32 value : settings.GetPDiskSpaceMarginPromille()) {
                            Self->PDiskSpaceMarginPromille = value;
                            db.Table<T>().Key(true).Update<T::PDiskSpaceMarginPromille>(Self->PDiskSpaceMarginPromille);
                        }
                        for (ui32 value : settings.GetGroupReserveMin()) {
                            Self->GroupReserveMin = value;
                            db.Table<T>().Key(true).Update<T::GroupReserveMin>(Self->GroupReserveMin);
                            Self->SysViewChangedSettings = true;
                        }
                        for (ui32 value : settings.GetGroupReservePartPPM()) {
                            Self->GroupReservePart = value;
                            db.Table<T>().Key(true).Update<T::GroupReservePart>(Self->GroupReservePart);
                            Self->SysViewChangedSettings = true;
                        }
                        for (ui32 value : settings.GetMaxScrubbedDisksAtOnce()) {
                            Self->MaxScrubbedDisksAtOnce = value;
                            db.Table<T>().Key(true).Update<T::MaxScrubbedDisksAtOnce>(Self->MaxScrubbedDisksAtOnce);
                            Self->ScrubState.OnMaxScrubbedDisksAtOnceChange();
                        }
                        for (auto value : settings.GetPDiskSpaceColorBorder()) {
                            Self->PDiskSpaceColorBorder = static_cast<T::PDiskSpaceColorBorder::Type>(value);
                            db.Table<T>().Key(true).Update<T::PDiskSpaceColorBorder>(Self->PDiskSpaceColorBorder);
                        }
                        if (settings.DatabaseSpaceBlockColorSize() || settings.DatabaseSpaceUnblockColorSize()) {
                            db.Table<T>().Key(true).Update<T::DatabaseSpaceBlockColor, T::DatabaseSpaceUnblockColor>(
                                databaseSpaceBlockColor, databaseSpaceUnblockColor);
                            Self->DatabaseSpace.SetThresholds(databaseSpaceBlockColor, databaseSpaceUnblockColor);
                            Self->CommitDatabaseSpaceChanges(db);
                        }
                        for (bool value : settings.GetEnableGroupLayoutSanitizer()) {
                            Self->GroupLayoutSanitizerEnabled = value;
                            db.Table<T>().Key(true).Update<T::GroupLayoutSanitizer>(Self->GroupLayoutSanitizerEnabled);
                            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
                            ev->GroupLayoutSanitizerEnabled = Self->GroupLayoutSanitizerEnabled;
                            Self->Send(Self->SelfHealId, ev.release());
                        }
                        for (bool value : settings.GetAllowMultipleRealmsOccupation()) {
                            Self->AllowMultipleRealmsOccupation = value;
                            db.Table<T>().Key(true).Update<T::AllowMultipleRealmsOccupation>(Self->AllowMultipleRealmsOccupation);
                            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
                            ev->AllowMultipleRealmsOccupation = Self->AllowMultipleRealmsOccupation;
                            Self->Send(Self->SelfHealId, ev.release());
                        }
                        for (bool value : settings.GetUseSelfHealLocalPolicy()) {
                            Self->UseSelfHealLocalPolicy = value;
                            db.Table<T>().Key(true).Update<T::UseSelfHealLocalPolicy>(Self->UseSelfHealLocalPolicy);
                            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
                            ev->UseSelfHealLocalPolicy = Self->UseSelfHealLocalPolicy;
                            Self->Send(Self->SelfHealId, ev.release());
                        }
                        for (bool value : settings.GetTryToRelocateBrokenDisksLocallyFirst()) {
                            Self->TryToRelocateBrokenDisksLocallyFirst = value;
                            db.Table<T>().Key(true).Update<T::TryToRelocateBrokenDisksLocallyFirst>(Self->TryToRelocateBrokenDisksLocallyFirst);
                            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
                            ev->TryToRelocateBrokenDisksLocallyFirst = Self->TryToRelocateBrokenDisksLocallyFirst;
                            Self->Send(Self->SelfHealId, ev.release());
                        }
                        return;
                    }

                    default:
                        throw TExError() << "unsupported sole command " << static_cast<int>(cmd.GetCommandCase());
                }
            }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_CONFIG_USEC);
                THPTimer timer;

                // check if there is some special sole command
                if (Cmd.CommandSize() == 1 && IsSoleCommand(Cmd.GetCommand(0))) {
                    WrapCommand([&] {
                        if (Cmd.GetRollback()) {
                            Rollback();
                        } else {
                            ExecuteSoleCommand(Cmd.GetCommand(0), txc);
                        }
                    });
                    Finish();
                    LogCommand(txc, TDuration::Seconds(timer.Passed()));
                    return true;
                }

                const auto& hostRecords = EnforceHostRecords ? *EnforceHostRecords : Self->HostRecords;
                std::optional<NKikimrBlobStorage::TStorageConfig> storageConfig;
                if (Cmd.HasStorageConfig() && Self->SelfManagementEnabled) {
                    Cmd.GetStorageConfig().UnpackTo(&storageConfig.emplace());
                }
                State.emplace(*Self, hostRecords, TActivationContext::Now(), TActivationContext::Monotonic(),
                    storageConfig ? &storageConfig.value() : nullptr);
                // Traverse payloads rather than maintaining a separate list of commands carrying Kind.
                if (std::any_of(Cmd.GetCommand().begin(), Cmd.GetCommand().end(),
                        [](const auto& command) { return command.HasDefinePDiskKind(); })) {
                    ReservePDiskKindIds(Cmd, State->ReservedPDiskKindIds);
                }
                State->CheckConsistency();

                TString m;
                google::protobuf::TextFormat::Printer printer;
                printer.SetSingleLineMode(true);
                printer.PrintToString(Cmd, &m);
                YDB_LOG_INFO_COMP(BS_CONTROLLER_AUDIT, "Generic command",
                    {"marker", "BSCA02"},
                    {"uniqueId", State->UniqueId},
                    {"request", Cmd},
                    {"selfHeal", SelfHeal});

                for (size_t stepIndex = 0; stepIndex < Cmd.CommandSize(); ++stepIndex) {
                    const auto& step = Cmd.GetCommand(stepIndex);
                    WrapCommand([&] {
                        THPTimer timer;
                        auto& status = *Response->MutableStatus()->rbegin();
                        ExecuteStep(*State, step, status);
                        const bool lastStep = stepIndex + 1 == Cmd.CommandSize();
                        const bool finishBatch = lastStep ||
                            !GetCommandInfo(Cmd.GetCommand(stepIndex + 1)).CanDeferFitting;
                        if ((finishBatch && !State->ChangedPDiskKinds.empty()) || step.HasDeletePDiskKind()) {
                            // Finish deferred work in the defining batch's status, before the next command starts.
                            State->ValidatePDiskKindReferences();
                            State->PreparePDiskKindUpdates();
                        }
                        State->CheckConsistency();
                        if (State->ChangedPDiskKinds.empty()) {
                            Self->FitPDisksForUserConfig(*State);

                            std::deque<ui64> expectedSlotSize;
                            if (step.GetCommandCase() == NKikimrBlobStorage::TConfigRequest::TCommand::kDefineStoragePool) {
                                const auto& cmd = step.GetDefineStoragePool();
                                for (ui64 size : cmd.GetExpectedGroupSlotSize()) {
                                    expectedSlotSize.push_back(size);
                                }
                            }
                            const auto availabilityDomainId = AppData()->DomainsInfo->GetDomain()->DomainUid;
                            Self->FitGroupsForUserConfig(*State, availabilityDomainId, Cmd, std::move(expectedSlotSize), status, !SelfHeal && !GroupLayoutSanitizer);
                        }

                        const TDuration passed = TDuration::Seconds(timer.Passed());
                        switch (step.GetCommandCase()) {
#define MAP_TIMING(CMD, NAME) \
                            case NKikimrBlobStorage::TConfigRequest::TCommand::k ## CMD: \
                                Self->TabletCounters->Cumulative()[NBlobStorageController::COUNTER_CONFIGCMD_## NAME ##_USEC].Increment(passed.MicroSeconds()); \
                                break;
                            MAP_TIMING(DefineHostConfig, DEFINE_HOST_CONFIG)
                            MAP_TIMING(ReadHostConfig, READ_HOST_CONFIG)
                            MAP_TIMING(DeleteHostConfig, DELETE_HOST_CONFIG)
                            MAP_TIMING(DefineBox, DEFINE_BOX)
                            MAP_TIMING(ReadBox, READ_BOX)
                            MAP_TIMING(DeleteBox, DELETE_BOX)
                            MAP_TIMING(DefineStoragePool, DEFINE_STORAGE_POOL)
                            MAP_TIMING(ReadStoragePool, READ_STORAGE_POOL)
                            MAP_TIMING(DeleteStoragePool, DELETE_STORAGE_POOL)
                            MAP_TIMING(UpdateDriveStatus, UPDATE_DRIVE_STATUS)
                            MAP_TIMING(ReadDriveStatus, READ_DRIVE_STATUS)
                            MAP_TIMING(ProposeStoragePools, PROPOSE_STORAGE_POOLS)
                            MAP_TIMING(ReadSettings, READ_SETTINGS)
                            MAP_TIMING(QueryBaseConfig, QUERY_BASE_CONFIG)
                            MAP_TIMING(MergeBoxes, MERGE_BOXES)
                            MAP_TIMING(MoveGroups, MOVE_GROUPS)
                            MAP_TIMING(AddMigrationPlan, ADD_MIGRATION_PLAN)
                            MAP_TIMING(DeleteMigrationPlan, DELETE_MIGRATION_PLAN)
                            MAP_TIMING(DeclareIntent, DECLARE_INTENT)
                            MAP_TIMING(ReadIntent, READ_INTENT)
                            MAP_TIMING(DropDonorDisk, DROP_DONOR_DISK)
                            MAP_TIMING(ReassignGroupDisk, REASSIGN_GROUP_DISK)
                            MAP_TIMING(AllocateVirtualGroup, ALLOCATE_VIRTUAL_GROUP)
                            MAP_TIMING(DecommitGroups, DECOMMIT_GROUPS)
                            MAP_TIMING(WipeVDisk, WIPE_VDISK)
                            MAP_TIMING(SanitizeGroup, SANITIZE_GROUP)
                            MAP_TIMING(CancelVirtualGroup, CANCEL_VIRTUAL_GROUP)
                            MAP_TIMING(ChangeGroupSizeInUnits, CHANGE_GROUP_SIZE_IN_UNITS)
                            MAP_TIMING(ReconfigureVirtualGroup, RECONFIGURE_VIRTUAL_GROUP)
                            MAP_TIMING(RecommissionGroups, RECOMMISSION_GROUPS)
                            MAP_TIMING(DefineDDiskPool, DEFINE_DDISK_POOL)
                            MAP_TIMING(ReadDDiskPool, READ_DDISK_POOL)
                            MAP_TIMING(DeleteDDiskPool, DELETE_DDISK_POOL)
                            MAP_TIMING(MoveDDisk, MOVE_DDISK)
                            MAP_TIMING(DeleteSpecificGroups, DELETE_SPECIFIC_GROUPS)

                            default:
                                break;
                        }
                    });
                    if (!Success) {
                        break;
                    }
                }

                if (Success && SelfHeal && !Self->SelfHealEnable) {
                    Success = false;
                    Error = "SelfHeal is disabled, transaction rollback";
                }

                const bool hasChanges = Success && State->Changed() && !Cmd.GetRollback();
                const TValidateConfigUpdatesParameters validationParameters{
                    .SuppressFailModelChecking = Cmd.GetIgnoreGroupFailModelChecks(),
                    .SuppressDegradedGroupsChecking = Cmd.GetIgnoreDegradedGroupsChecks(),
                    .SuppressDisintegratedGroupsChecking = Cmd.GetIgnoreDisintegratedGroupsChecks(),
                    .AllowDegradedWithSinglePhantomsOnly = SelfHeal,
                };
                Success = Success && Self->ValidateConfigUpdates(*State, validationParameters, &Error, Response);

                if (Success && Cmd.GetRollback()) {
                    Rollback();
                } else if (Success) {
                    Self->CommitConfigUpdates(*State, txc);
                }

                Finish();
                if (Success && hasChanges) {
                    LogCommand(txc, TDuration::Seconds(timer.Passed()));
                }

                YDB_LOG_INFO_COMP(BS_CONTROLLER_AUDIT, "Transaction ended",
                    {"marker", "BSCA03"},
                    {"uniqueId", State->UniqueId},
                    {"status", Success ? "commit" : "rollback"},
                    {"error", Error});

                if (SelfHeal) {
                    const auto counter = Success
                        ? NBlobStorageController::COUNTER_SELFHEAL_REASSIGN_BSC_OK
                        : NBlobStorageController::COUNTER_SELFHEAL_REASSIGN_BSC_ERR;
                    Self->TabletCounters->Cumulative()[counter].Increment(1);
                }
                if (GroupLayoutSanitizer) {
                    const auto counter = Success
                        ? NBlobStorageController::COUNTER_GROUP_LAYOUT_SANITIZER_BSC_OK
                        : NBlobStorageController::COUNTER_GROUP_LAYOUT_SANITIZER_BSC_ERR;
                    Self->TabletCounters->Cumulative()[counter].Increment(1);
                }

                if (!Success) {
                    // rollback transaction
                    std::exchange(State, std::nullopt)->Rollback();
                }

                return true;
            }

            void LogCommand(TTransactionContext& txc, TDuration executionTime) {
                ui64 operationLogIndex = Self->NextOperationLogIndex;
                // update operation log for write transaction
                NIceDb::TNiceDb db(txc.DB);
                TString requestBuffer, responseBuffer;
                Y_PROTOBUF_SUPPRESS_NODISCARD Cmd.SerializeToString(&requestBuffer);
                Y_PROTOBUF_SUPPRESS_NODISCARD Response->SerializeToString(&responseBuffer);
                db.Table<Schema::OperationLog>().Key(operationLogIndex).Update(
                    NIceDb::TUpdate<Schema::OperationLog::Timestamp>(TActivationContext::Now()),
                    NIceDb::TUpdate<Schema::OperationLog::Request>(requestBuffer),
                    NIceDb::TUpdate<Schema::OperationLog::Response>(responseBuffer),
                    NIceDb::TUpdate<Schema::OperationLog::ExecutionTime>(executionTime));
                db.Table<Schema::State>().Key(true).Update(
                    NIceDb::TUpdate<Schema::State::NextOperationLogIndex>(++Self->NextOperationLogIndex));

                YDB_LOG_INFO_COMP(BS_CONTROLLER_AUDIT, "Finished processing command",
                    {"marker", "BSCA10"},
                    {"request", Cmd.DebugString()},
                    {"response", Response->DebugString()},
                    {"executionTime", executionTime},
                    {"operationLogIndex", operationLogIndex});
            }

            // Numeric Kind fields identify PDisks; storage-pool Kind is a string and is not reserved.
            // Following message fields also covers new commands and nested drive/filter collections.
            // Numeric PDisk kinds carried by commands of this request; a kind defined in the same request must not
            // be allocated one of these ids. Exactly the fields that BSC reads as a PDisk kind, nothing inferred.
            static void ReservePDiskKindIds(const NKikimrBlobStorage::TConfigRequest& request, THashSet<TPDiskKindId>& ids) {
                const auto reserve = [&](ui64 kind) {
                    if (kind) {
                        ids.insert(kind);
                    }
                };
                const auto reserveFilters = [&](const auto& filters) {
                    for (const auto& filter : filters) {
                        // A filter naming a kind has its numeric Kind checked against that kind, not reserved.
                        if (AnyOf(filter.GetProperty(), [](const auto& property) { return property.HasKindName(); })) {
                            continue;
                        }
                        for (const auto& property : filter.GetProperty()) {
                            if (property.HasKind()) {
                                reserve(property.GetKind());
                            }
                        }
                    }
                };
                using TCommand = NKikimrBlobStorage::TConfigRequest::TCommand;
                for (const auto& command : request.GetCommand()) {
                    switch (command.GetCommandCase()) {
                        case TCommand::kDefineHostConfig:
                            for (const auto& drive : command.GetDefineHostConfig().GetDrive()) {
                                if (drive.GetKindName().empty()) {
                                    reserve(drive.GetKind());
                                }
                            }
                            break;
                        case TCommand::kAddDriveSerial:
                            if (const auto& cmd = command.GetAddDriveSerial(); cmd.GetKindName().empty()) {
                                reserve(cmd.GetKind());
                            }
                            break;
                        case TCommand::kDefineStoragePool:
                            reserveFilters(command.GetDefineStoragePool().GetPDiskFilter());
                            break;
                        case TCommand::kDefineDDiskPool:
                            reserveFilters(command.GetDefineDDiskPool().GetPDiskFilter());
                            break;
                        default:
                            break;
                    }
                }
            }

            struct TCommandInfo {
                void (*Execute)(TConfigState&, const NKikimrBlobStorage::TConfigRequest::TCommand&,
                    NKikimrBlobStorage::TConfigResponse::TStatus&) = nullptr;
                bool CanDeferFitting = false;
            };

            static TCommandInfo GetCommandInfo(const NKikimrBlobStorage::TConfigRequest::TCommand& cmd) {
                switch (cmd.GetCommandCase()) {
#define HANDLE_COMMAND(NAME, CAN_DEFER_FITTING) \
                    case NKikimrBlobStorage::TConfigRequest::TCommand::k ## NAME: \
                        return {[](TConfigState& state, const auto& command, auto& status) { \
                            state.ExecuteStep(command.Get ## NAME(), status); \
                        }, CAN_DEFER_FITTING};

                    HANDLE_COMMAND(DefineHostConfig, true)
                    HANDLE_COMMAND(ReadHostConfig, false)
                    HANDLE_COMMAND(DeleteHostConfig, true)
                    HANDLE_COMMAND(DefinePDiskKind, true)
                    HANDLE_COMMAND(DeletePDiskKind, false)
                    HANDLE_COMMAND(DefineBox, true)
                    HANDLE_COMMAND(ReadBox, false)
                    HANDLE_COMMAND(DeleteBox, true)
                    HANDLE_COMMAND(DefineStoragePool, false)
                    HANDLE_COMMAND(ReadStoragePool, false)
                    HANDLE_COMMAND(DeleteStoragePool, false)
                    HANDLE_COMMAND(UpdateDriveStatus, false)
                    HANDLE_COMMAND(ReadDriveStatus, false)
                    HANDLE_COMMAND(ProposeStoragePools, false)
                    HANDLE_COMMAND(ReadSettings, false)
                    HANDLE_COMMAND(QueryBaseConfig, false)
                    HANDLE_COMMAND(ReassignGroupDisk, false)
                    HANDLE_COMMAND(MergeBoxes, false)
                    HANDLE_COMMAND(MoveGroups, false)
                    HANDLE_COMMAND(ChangeGroupSizeInUnits, false)
                    HANDLE_COMMAND(DropDonorDisk, false)
                    HANDLE_COMMAND(AddDriveSerial, true)
                    HANDLE_COMMAND(RemoveDriveSerial, true)
                    HANDLE_COMMAND(ForgetDriveSerial, true)
                    HANDLE_COMMAND(MigrateToSerial, false)
                    HANDLE_COMMAND(AllocateVirtualGroup, false)
                    HANDLE_COMMAND(DecommitGroups, false)
                    HANDLE_COMMAND(WipeVDisk, false)
                    HANDLE_COMMAND(SanitizeGroup, false)
                    HANDLE_COMMAND(CancelVirtualGroup, false)
                    HANDLE_COMMAND(SetVDiskReadOnly, false)
                    HANDLE_COMMAND(RestartPDisk, false)
                    HANDLE_COMMAND(SetPDiskReadOnly, false)
                    HANDLE_COMMAND(StopPDisk, false)
                    HANDLE_COMMAND(GetInterfaceVersion, false)
                    HANDLE_COMMAND(MovePDisk, false)
                    HANDLE_COMMAND(PopulatePDisk, false)
                    HANDLE_COMMAND(UpdateBridgeGroupInfo, false)
                    HANDLE_COMMAND(ReconfigureVirtualGroup, false)
                    HANDLE_COMMAND(RecommissionGroups, false)
                    HANDLE_COMMAND(DefineDDiskPool, false)
                    HANDLE_COMMAND(ReadDDiskPool, false)
                    HANDLE_COMMAND(DeleteDDiskPool, false)
                    HANDLE_COMMAND(MoveDDisk, false)
                    HANDLE_COMMAND(DeleteSpecificGroups, false)
                    HANDLE_COMMAND(UpdateStoragePoolSettings, false)
#undef HANDLE_COMMAND
                    default: return {};
                }
            }

            void ExecuteStep(TConfigState& state, const NKikimrBlobStorage::TConfigRequest::TCommand& cmd,
                    NKikimrBlobStorage::TConfigResponse::TStatus& status) {
                state.SanitizingRequests.clear();
                state.ExplicitReconfigureMap.clear();
                state.SuppressDonorMode.clear();
                if (const auto info = GetCommandInfo(cmd); info.Execute) {
                    info.Execute(state, cmd, status);
                } else if (IsSoleCommand(cmd)) {
                    throw TExError() << "command must be sole";
                } else {
                    throw TExError() << "unsupported command " << static_cast<int>(cmd.GetCommandCase());
                }
            }

            void Complete(const TActorContext&) override {
                if (auto state = std::exchange(State, std::nullopt)) {
                    ui64 configTxSeqNo = state->ApplyConfigUpdates();
                    YDB_LOG_INFO_COMP(BS_CONTROLLER_AUDIT, "Transaction complete",
                        {"marker", "BSCA09"},
                        {"uniqueId", state->UniqueId},
                        {"nextConfigTxSeqNo", configTxSeqNo});
                    Ev->Record.MutableResponse()->SetConfigTxSeqNo(configTxSeqNo);
                } else {
                    Ev->Record.MutableResponse()->SetConfigTxSeqNo(Self->NextConfigTxSeqNo - 1);
                }
                TActivationContext::Send(new IEventHandle(NotifyId, Self->SelfId(), Ev.Release(), 0, Cookie));
                Self->UpdatePDisksCounters();
            }
        };

        void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerConfigRequest::TPtr &ev) {
            TabletCounters->Cumulative()[NBlobStorageController::COUNTER_CONFIG_COUNT].Increment(1);
            if (ev->Get()->SelfHeal) {
                TabletCounters->Cumulative()[NBlobStorageController::COUNTER_SELFHEAL_REASSIGN_BSC_REQUESTS].Increment(1);
            }
            if (ev->Get()->GroupLayoutSanitizer) {
                TabletCounters->Cumulative()[NBlobStorageController::COUNTER_GROUP_LAYOUT_SANITIZER_BSC_REQUESTS].Increment(1);
            }

            NKikimrBlobStorage::TEvControllerConfigRequest& record(ev->Get()->Record);
            const NKikimrBlobStorage::TConfigRequest& request = record.GetRequest();
            YDB_LOG_DEBUG_COMP(BS_CONTROLLER, "Execute TEvControllerConfigRequest",
                {"marker", "BSCTXCC01"},
                {"request", request});
            Execute(new TTxConfigCmd(request, ev->Sender, ev->Cookie, ev->Get()->SelfHeal, ev->Get()->GroupLayoutSanitizer, ev->Get()->EnforceHostRecords, this));
        }

} // NKikimr::NBsController
