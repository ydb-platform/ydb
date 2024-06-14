#include "impl.h"
#include "config.h"
#include "select_groups.h"

namespace NKikimr::NBsController {

        class TBlobStorageController::TBlobStorageController::TTxConfigCmd
            : public TTransactionBase<TBlobStorageController>
        {
            const TActorId NotifyId;
            const ui64 Cookie;
            const NKikimrBlobStorage::TConfigRequest Cmd;
            const bool SelfHeal;
            const bool GroupLayoutSanitizer;
            THolder<TEvBlobStorage::TEvControllerConfigResponse> Ev;
            NKikimrBlobStorage::TConfigResponse *Response;
            std::optional<TConfigState> State;
            bool Success = true;
            TString Error;

        public:
            TTxConfigCmd(const NKikimrBlobStorage::TConfigRequest &cmd, const TActorId &notifyId, ui64 cookie,
                    bool selfHeal, bool groupLayoutSanitizer, TBlobStorageController *controller)
                : TTransactionBase(controller)
                , NotifyId(notifyId)
                , Cookie(cookie)
                , Cmd(cmd)
                , SelfHeal(selfHeal)
                , GroupLayoutSanitizer(groupLayoutSanitizer)
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

            void Finish() {
                Response->SetSuccess(Success);
                if (!Success) {
                    Response->SetErrorDescription(Error);
                }
            }

            bool ExecuteSoleCommand(const NKikimrBlobStorage::TConfigRequest::TCommand& cmd, TTransactionContext& txc) {
                NIceDb::TNiceDb db(txc.DB);
                switch (cmd.GetCommandCase()) {
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableSelfHeal:
                        Self->SelfHealEnable = cmd.GetEnableSelfHeal().GetEnable();
                        db.Table<Schema::State>().Key(true).Update<Schema::State::SelfHealEnable>(Self->SelfHealEnable);
                        return true;

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableDonorMode:
                        Self->DonorMode = cmd.GetEnableDonorMode().GetEnable();
                        db.Table<Schema::State>().Key(true).Update<Schema::State::DonorModeEnable>(Self->DonorMode);
                        return true;

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetScrubPeriodicity: {
                        const ui32 seconds = cmd.GetSetScrubPeriodicity().GetScrubPeriodicity();
                        Self->ScrubPeriodicity = TDuration::Seconds(seconds);
                        db.Table<Schema::State>().Key(true).Update<Schema::State::ScrubPeriodicity>(seconds);
                        Self->ScrubState.OnScrubPeriodicityChange();
                        return true;
                    }

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetPDiskSpaceMarginPromille: {
                        const ui32 value = cmd.GetSetPDiskSpaceMarginPromille().GetPDiskSpaceMarginPromille();
                        Self->PDiskSpaceMarginPromille = value;
                        db.Table<Schema::State>().Key(true).Update<Schema::State::PDiskSpaceMarginPromille>(value);
                        return true;
                    }

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateSettings: {
                        const auto& settings = cmd.GetUpdateSettings();
                        using T = Schema::State;
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
                        }
                        for (bool value : settings.GetTryToRelocateBrokenDisksLocallyFirst()) {
                            Self->TryToRelocateBrokenDisksLocallyFirst = value;
                            db.Table<T>().Key(true).Update<T::TryToRelocateBrokenDisksLocallyFirst>(Self->TryToRelocateBrokenDisksLocallyFirst);
                        }
                        return true;
                    }

                    default:
                        return false;
                }
            }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_CONFIG_USEC);
                THPTimer timer;

                // check if there is some special sole command
                if (Cmd.CommandSize() == 1) {
                    bool res = true;
                    WrapCommand([&] {
                        res = ExecuteSoleCommand(Cmd.GetCommand(0), txc);
                    });
                    if (res) {
                        Finish();
                        LogCommand(txc, TDuration::Seconds(timer.Passed()));
                        return true;
                    }
                    Y_ABORT_UNLESS(Success);
                    Response->MutableStatus()->RemoveLast();
                }

                State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
                State->CheckConsistency();

                TString m;
                google::protobuf::TextFormat::Printer printer;
                printer.SetSingleLineMode(true);
                printer.PrintToString(Cmd, &m);
                STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA02, "Generic command",
                    (UniqueId, State->UniqueId),
                    (Request, Cmd),
                    (SelfHeal, SelfHeal));

                for (const auto& step : Cmd.GetCommand()) {
                    WrapCommand([&] {
                        THPTimer timer;
                        auto& status = *Response->MutableStatus()->rbegin();
                        ExecuteStep(*State, step, status);
                        State->CheckConsistency();
                        Self->FitPDisksForUserConfig(*State);

                        std::deque<ui64> expectedSlotSize;
                        if (step.GetCommandCase() == NKikimrBlobStorage::TConfigRequest::TCommand::kDefineStoragePool) {
                            const auto& cmd = step.GetDefineStoragePool();
                            for (ui64 size : cmd.GetExpectedGroupSlotSize()) {
                                expectedSlotSize.push_back(size);
                            }
                        }
                        const auto availabilityDomainId = AppData()->DomainsInfo->GetDomain()->DomainUid;
                        Self->FitGroupsForUserConfig(*State, availabilityDomainId, Cmd, std::move(expectedSlotSize), status);

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

                            default:
                                break;
                        }
                    });
                    if (!Success) {
                        break;
                    }
                }

                if (Success && Cmd.GetRollback()) {
                    Success = false;
                    Error = "transaction rollback";
                }

                if (Success && SelfHeal && !Self->SelfHealEnable) {
                    Success = false;
                    Error = "SelfHeal is disabled, transaction rollback";
                }

                const bool doLogCommand = Success && State->Changed();
                Success = Success && Self->CommitConfigUpdates(*State, Cmd.GetIgnoreGroupFailModelChecks(),
                    Cmd.GetIgnoreDegradedGroupsChecks(), Cmd.GetIgnoreDisintegratedGroupsChecks(), txc, &Error,
                    Response);

                Finish();
                if (doLogCommand) {
                    LogCommand(txc, TDuration::Seconds(timer.Passed()));
                }

                STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA03, "Transaction ended",
                    (UniqueId, State->UniqueId),
                    (Status, Success ? "commit" : "rollback"),
                    (Error, Error));

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
                // update operation log for write transaction
                NIceDb::TNiceDb db(txc.DB);
                TString requestBuffer, responseBuffer;
                Y_PROTOBUF_SUPPRESS_NODISCARD Cmd.SerializeToString(&requestBuffer);
                Y_PROTOBUF_SUPPRESS_NODISCARD Response->SerializeToString(&responseBuffer);
                db.Table<Schema::OperationLog>().Key(Self->NextOperationLogIndex).Update(
                    NIceDb::TUpdate<Schema::OperationLog::Timestamp>(TActivationContext::Now()),
                    NIceDb::TUpdate<Schema::OperationLog::Request>(requestBuffer),
                    NIceDb::TUpdate<Schema::OperationLog::Response>(responseBuffer),
                    NIceDb::TUpdate<Schema::OperationLog::ExecutionTime>(executionTime));
                db.Table<Schema::State>().Key(true).Update(
                    NIceDb::TUpdate<Schema::State::NextOperationLogIndex>(++Self->NextOperationLogIndex));
            }

            void ExecuteStep(TConfigState& state, const NKikimrBlobStorage::TConfigRequest::TCommand& cmd,
                    NKikimrBlobStorage::TConfigResponse::TStatus& status) {
                state.SanitizingRequests.clear();
                state.ExplicitReconfigureMap.clear();
                state.SuppressDonorMode.clear();
                switch (cmd.GetCommandCase()) {
#define HANDLE_COMMAND(NAME) \
                    case NKikimrBlobStorage::TConfigRequest::TCommand::k ## NAME: return state.ExecuteStep(cmd.Get ## NAME(), status);

                    HANDLE_COMMAND(DefineHostConfig)
                    HANDLE_COMMAND(ReadHostConfig)
                    HANDLE_COMMAND(DeleteHostConfig)
                    HANDLE_COMMAND(DefineBox)
                    HANDLE_COMMAND(ReadBox)
                    HANDLE_COMMAND(DeleteBox)
                    HANDLE_COMMAND(DefineStoragePool)
                    HANDLE_COMMAND(ReadStoragePool)
                    HANDLE_COMMAND(DeleteStoragePool)
                    HANDLE_COMMAND(UpdateDriveStatus)
                    HANDLE_COMMAND(ReadDriveStatus)
                    HANDLE_COMMAND(ProposeStoragePools)
                    HANDLE_COMMAND(ReadSettings)
                    HANDLE_COMMAND(QueryBaseConfig)
                    HANDLE_COMMAND(ReassignGroupDisk)
                    HANDLE_COMMAND(MergeBoxes)
                    HANDLE_COMMAND(MoveGroups)
                    HANDLE_COMMAND(DropDonorDisk)
                    HANDLE_COMMAND(AddDriveSerial)
                    HANDLE_COMMAND(RemoveDriveSerial)
                    HANDLE_COMMAND(ForgetDriveSerial)
                    HANDLE_COMMAND(MigrateToSerial)
                    HANDLE_COMMAND(AllocateVirtualGroup)
                    HANDLE_COMMAND(DecommitGroups)
                    HANDLE_COMMAND(WipeVDisk)
                    HANDLE_COMMAND(SanitizeGroup)
                    HANDLE_COMMAND(CancelVirtualGroup)
                    HANDLE_COMMAND(SetVDiskReadOnly)
                    HANDLE_COMMAND(RestartPDisk)

                    case NKikimrBlobStorage::TConfigRequest::TCommand::kAddMigrationPlan:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeleteMigrationPlan:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kDeclareIntent:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kReadIntent:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableSelfHeal:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kEnableDonorMode:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetScrubPeriodicity:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kSetPDiskSpaceMarginPromille:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::kUpdateSettings:
                    case NKikimrBlobStorage::TConfigRequest::TCommand::COMMAND_NOT_SET:
                        throw TExError() << "unsupported command";
                }

                Y_ABORT();
            }

            void Complete(const TActorContext&) override {
                if (auto state = std::exchange(State, std::nullopt)) {
                    ui64 configTxSeqNo = state->ApplyConfigUpdates();
                    STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA09, "Transaction complete", (UniqueId, state->UniqueId),
                            (NextConfigTxSeqNo, configTxSeqNo));
                    Ev->Record.MutableResponse()->SetConfigTxSeqNo(configTxSeqNo);
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
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXCC01, "Execute TEvControllerConfigRequest", (Request, request));
            Execute(new TTxConfigCmd(request, ev->Sender, ev->Cookie, ev->Get()->SelfHeal, ev->Get()->GroupLayoutSanitizer, this));
        }

} // NKikimr::NBsController
