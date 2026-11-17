#include "ydb_bridge.h"

#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <algorithm>

namespace NYdb::NConsoleClient {
    namespace {

        TString PileStateToString(NBridge::EPileState state) {
            switch (state) {
                case NBridge::EPileState::UNSPECIFIED:
                    return "UNSPECIFIED";
                case NBridge::EPileState::DISCONNECTED:
                    return "DISCONNECTED";
                case NBridge::EPileState::NOT_SYNCHRONIZED:
                    return "NOT_SYNCHRONIZED";
                case NBridge::EPileState::SYNCHRONIZED:
                    return "SYNCHRONIZED";
                case NBridge::EPileState::PROMOTED:
                    return "PROMOTED";
                case NBridge::EPileState::PRIMARY:
                    return "PRIMARY";
                case NBridge::EPileState::SUSPENDED:
                    return "SUSPENDED";
                case NBridge::EPileState::INACTIVE:
                    return "NOT_PROVISIONED";
            }
            return "UNKNOWN";
        }

        TString PendingPileOperationKindToString(NBridge::TPendingPileOperation::EKind kind) {
            switch (kind) {
                case NBridge::TPendingPileOperation::EKind::UNSPECIFIED:
                    return "UNSPECIFIED";
                case NBridge::TPendingPileOperation::EKind::ADD:
                    return "ADD";
                case NBridge::TPendingPileOperation::EKind::REMOVE:
                    return "REMOVE";
                case NBridge::TPendingPileOperation::EKind::ROLLBACK:
                    return "ROLLBACK";
            }
            return "UNKNOWN";
        }

        TString SyncStatusToString(NBridge::ESynchronizationStatus status) {
            switch (status) {
                case NBridge::ESynchronizationStatus::Synced:
                    return "SYNCED";
                case NBridge::ESynchronizationStatus::Syncing:
                    return "SYNCING";
                case NBridge::ESynchronizationStatus::Pending:
                    return "PENDING";
                case NBridge::ESynchronizationStatus::NotRequired:
                    return "NON_BRIDGED";
                default:
                    return "STATUS_UNAVAILABLE";
            }
        }

        NBridge::TStoragePoolId ParseStoragePoolId(TStringBuf value) {
            TStringBuf box;
            TStringBuf pool;
            const auto decimal = [](TStringBuf part) {
                return !part.empty() && std::all_of(part.begin(), part.end(), [](char c) { return c >= '0' && c <= '9'; });
            };
            NBridge::TStoragePoolId id;
            if (!value.TrySplit(':', box, pool) || !decimal(box) || !decimal(pool) ||
                !TryFromString(box, id.BoxId) || !TryFromString(pool, id.PoolId) || !id.PoolId) {
                throw TMisuseException() << "--pool-id must be BOX:POOL with unsigned 64-bit IDs and a nonzero POOL.";
            }
            return id;
        }

        TString DatabaseSyncLabel(const NBridge::TGroupSyncCounts& groups) {
            if (!groups.Required) {
                return "NON_BRIDGED";
            }
            if (groups.Synced == groups.Required) {
                return "SYNCED";
            }
            if (groups.Syncing) {
                return "SYNCING";
            }
            return groups.Unknown ? "STATUS_UNAVAILABLE" : "PENDING";
        }

        TString StorageModeToString(NBridge::EStorageMode mode) {
            switch (mode) {
                case NBridge::EStorageMode::Bridged:
                    return "bridged";
                case NBridge::EStorageMode::Ordinary:
                    return "non-bridged";
                case NBridge::EStorageMode::Mixed:
                    return "mixed";
                default:
                    return "unknown";
            }
        }

        TString SyncRatioToString(const NBridge::TGroupSyncCounts& groups) {
            return ToString(groups.Synced) + "/" + ToString(groups.Required);
        }

        TString DatabaseSyncRatioToString(const NBridge::TDatabaseSyncCounts& databases) {
            return ToString(databases.Synced) + "/" + ToString(databases.Total);
        }

        TString OptionalName(const std::optional<std::string>& name, bool csv) {
            if (!name) {
                return csv ? TString{} : TString("—");
            }
            return !csv && name->empty() ? TString("\"\"") : TString(*name);
        }

        TString PrettySyncPhase(const std::string& phase) {
            if (phase == "WRITE_KEEP") {
                return "KEEP";
            }
            if (phase == "WRITE_KEEP_BARRIER_DONOTKEEP") {
                return "GC";
            }
            if (phase == "WRITE_KEEP_BARRIER_DONOTKEEP_DATA") {
                return "DATA";
            }
            return TString(phase);
        }

        TString SyncErrorMessage(const std::optional<NBridge::TSynchronizationError>& error) {
            return error ? TString(error->Message) : TString{};
        }

        TString SyncErrorTime(const std::optional<NBridge::TSynchronizationError>& error, bool csv) {
            if (error && error->OccurredAt) {
                return error->OccurredAt->ToString();
            }
            return csv ? TString{} : TString("—");
        }

        void WriteCsvRow(const TVector<TString>& columns) {
            bool first = true;
            for (const auto& column : columns) {
                if (!first) {
                    Cout << ',';
                }
                first = false;
                if (column.find_first_of(",\"\r\n") == TString::npos) {
                    Cout << column;
                } else {
                    Cout << '"';
                    for (char c : column) {
                        if (c == '"') {
                            Cout << '"';
                        }
                        Cout << c;
                    }
                    Cout << '"';
                }
            }
            Cout << Endl;
        }

        const TVector<TString>& PileColumns(bool all = true) {
            static const TVector<TString> base = {"Pile", "PileState", "DatabasesSync", "Operation", "LastError"};
            static const TVector<TString> full = {"Pile", "PileState", "DatabasesSync", "DatabasesSyncing", "DatabasesPending",
                                                 "DatabasesStatusUnavailable", "DatabasesNonBridged", "Operation", "LastError"};
            return all ? full : base;
        }

        const TVector<TString>& DatabaseColumns(bool all = true) {
            static const TVector<TString> base = {"Database", "Mode", "Pile", "DataSync", "GroupsSync", "LastError"};
            static const TVector<TString> full = {"Database", "Mode", "Pile", "DataSync", "GroupsSync", "Pools", "GroupsSyncing",
                                                 "GroupsPending", "GroupsStatusUnavailable", "LastError", "LastErrorAt"};
            return all ? full : base;
        }

        const TVector<TString>& GroupColumns(bool all = true) {
            static const TVector<TString> base = {"GroupId", "Pool", "Pile", "DataSync", "Phase", "LastError"};
            static const TVector<TString> full = {"GroupId", "Pool", "Pile", "DataSync", "Phase", "PhysicalGroupId", "PoolId",
                                                 "LastError", "LastErrorAt"};
            return all ? full : base;
        }

        TString OptionalText(const TString& text, bool csv) {
            return !csv && text.empty() ? TString("—") : text;
        }

        void PrintRows(const TVector<TString>& allColumns, const TVector<TVector<TString>>& rows,
                       const TVector<TString>& columns, bool csv) {
            TVector<size_t> indices;
            for (const auto& column : columns) {
                indices.push_back(std::find(allColumns.begin(), allColumns.end(), column) - allColumns.begin());
            }
            if (csv) {
                WriteCsvRow(columns);
                for (const auto& row : rows) {
                    TVector<TString> selected;
                    for (const auto index : indices) {
                        selected.push_back(row[index]);
                    }
                    WriteCsvRow(selected);
                }
                return;
            }
            TPrettyTable table(columns, TPrettyTableConfig().WithoutRowDelimiters());
            for (const auto& row : rows) {
                auto& out = table.AddRow();
                for (size_t i = 0; i < indices.size(); ++i) {
                    out.Column(i, row[indices[i]]);
                }
            }
            Cout << table;
        }

        NJson::TJsonValue StoragePoolToJson(const NBridge::TStoragePool& pool) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            NJson::TJsonValue id(NJson::JSON_MAP);
            id.InsertValue("box_id", pool.Id.BoxId);
            id.InsertValue("pool_id", pool.Id.PoolId);
            json.InsertValue("id", id);
            json.InsertValue("name", pool.Name);
            return json;
        }

        NJson::TJsonValue StoragePoolsToJson(const std::vector<NBridge::TStoragePool>& pools) {
            NJson::TJsonValue json(NJson::JSON_ARRAY);
            for (const auto& pool : pools) {
                json.AppendValue(StoragePoolToJson(pool));
            }
            return json;
        }

        TString PoolNames(const std::vector<NBridge::TStoragePool>& pools) {
            TString result;
            for (const auto& pool : pools) {
                if (!result.empty()) {
                    result += ", ";
                }
                result += pool.Name;
            }
            return result;
        }

        NJson::TJsonValue SyncCountsToJson(const NBridge::TGroupSyncCounts& groups) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("required", groups.Required);
            json.InsertValue("synced", groups.Synced);
            json.InsertValue("syncing", groups.Syncing);
            json.InsertValue("pending", groups.Pending);
            json.InsertValue("status_unavailable", groups.Unknown);
            return json;
        }

        NJson::TJsonValue DatabaseCountsToJson(const NBridge::TDatabaseSyncCounts& databases) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("total", databases.Total);
            json.InsertValue("synced", databases.Synced);
            json.InsertValue("syncing", databases.Syncing);
            json.InsertValue("pending", databases.Pending);
            json.InsertValue("non_bridged", databases.NotRequired);
            json.InsertValue("status_unavailable", databases.Unknown);
            return json;
        }

        NJson::TJsonValue SyncErrorToJson(const NBridge::TSynchronizationError& error) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("message", error.Message);
            if (error.OccurredAt) {
                json.InsertValue("occurred_at", error.OccurredAt->ToString());
            }
            return json;
        }

        NJson::TJsonValue SummaryToJson(const NBridge::TListPileSynchronizationResult& summary, const std::optional<TString>& pileName) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("cluster_state_generation", summary.GetClusterStateGeneration());
            json.InsertValue("observed_at", summary.GetObservedAt().ToString());
            json.InsertValue("unattributed_storage_pools", summary.GetUnattributedStoragePools());
            NJson::TJsonValue piles(NJson::JSON_ARRAY);
            for (const auto& pile : summary.GetPiles()) {
                if (pileName && (!pile.PileName || *pile.PileName != *pileName)) {
                    continue;
                }
                NJson::TJsonValue item(NJson::JSON_MAP);
                if (pile.PileName) {
                    item.InsertValue("pile_name", *pile.PileName);
                }
                item.InsertValue("databases", DatabaseCountsToJson(pile.Databases));
                piles.AppendValue(item);
            }
            json.InsertValue("piles", piles);
            return json;
        }

        NJson::TJsonValue ClusterStateToJson(const NBridge::TGetClusterStateResult& result, const std::vector<NBridge::TPileStateUpdate>& state) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("generation", result.GetGeneration());
            NJson::TJsonValue piles(NJson::JSON_ARRAY);
            for (const auto& pile : state) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("pile_name", pile.PileName);
                item.InsertValue("state", PileStateToString(pile.State));
                piles.AppendValue(item);
            }
            json.InsertValue("piles", piles);
            json.InsertValue("conversion_in_progress", result.IsConversionInProgress());
            if (const auto& op = result.GetPendingPileOperation()) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("pile_name", op->PileName);
                item.InsertValue("kind", PendingPileOperationKindToString(op->Kind));
                item.InsertValue("error_reason", op->ErrorReason);
                json.InsertValue("pending_pile_operation", item);
            }
            return json;
        }

        NJson::TJsonValue DatabasesToJson(const std::vector<NBridge::TDatabaseSynchronization>& databases) {
            NJson::TJsonValue json(NJson::JSON_ARRAY);
            for (const auto& database : databases) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                if (database.DatabasePath) {
                    item.InsertValue("database_path", *database.DatabasePath);
                }
                item.InsertValue("storage_pools", StoragePoolsToJson(database.StoragePools));
                item.InsertValue("storage_mode", StorageModeToString(database.StorageMode));
                if (database.PileName) {
                    item.InsertValue("pile_name", *database.PileName);
                }
                item.InsertValue("groups", SyncCountsToJson(database.Groups));
                if (database.LastSyncError) {
                    item.InsertValue("last_sync_error", SyncErrorToJson(*database.LastSyncError));
                }
                json.AppendValue(item);
            }
            return json;
        }

        NJson::TJsonValue GroupsToJson(const std::vector<NBridge::TGroupSynchronization>& groups) {
            NJson::TJsonValue json(NJson::JSON_ARRAY);
            for (const auto& group : groups) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("group_id", group.GroupId);
                if (group.PhysicalGroupId) {
                    item.InsertValue("physical_group_id", *group.PhysicalGroupId);
                }
                if (group.StoragePool) {
                    item.InsertValue("storage_pool", StoragePoolToJson(*group.StoragePool));
                }
                if (group.PileName) {
                    item.InsertValue("pile_name", *group.PileName);
                }
                item.InsertValue("sync_status", SyncStatusToString(group.SyncStatus));
                item.InsertValue("sync_phase", group.SyncPhase);
                if (group.LastSyncError) {
                    item.InsertValue("last_sync_error", SyncErrorToJson(*group.LastSyncError));
                }
                json.AppendValue(item);
            }
            return json;
        }

        NJson::TJsonValue ListMetadataToJson(uint64_t generation, TInstant observedAt, const std::optional<std::string>& owner) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("cluster_state_generation", generation);
            json.InsertValue("observed_at", observedAt.ToString());
            if (owner) {
                json.InsertValue("storage_owner_database_path", *owner);
            }
            return json;
        }

        void PrintJson(const NJson::TJsonValue& json) {
            NJson::WriteJson(&Cout, &json, true);
            Cout << Endl;
        }

        void PrintUnbridgedStatus(const NBridge::TUnbridgedStatus& status, EDataFormat format, bool allColumns,
                                  const TVector<TString>& columns) {
            const auto& bridgeNodes = status.BridgeConfigNodeIds;
            const auto& unreportedNodes = status.UnreportedStaticNodeIds;
            if (format == EDataFormat::Json) {
                NJson::TJsonValue json(NJson::JSON_MAP);
                json.InsertValue("piles", NJson::TJsonValue(NJson::JSON_ARRAY));
                NJson::TJsonValue report(NJson::JSON_MAP);
                for (const auto& [key, nodeIds] : {
                        std::make_pair("bridge_config_node_ids", &bridgeNodes),
                        std::make_pair("unreported_static_node_ids", &unreportedNodes)}) {
                    NJson::TJsonValue ids(NJson::JSON_ARRAY);
                    for (const auto id : *nodeIds) {
                        ids.AppendValue(id);
                    }
                    report.InsertValue(key, std::move(ids));
                }
                json.InsertValue("unbridged_status", std::move(report));
                PrintJson(json);
                return;
            }
            if (format == EDataFormat::Csv) {
                WriteCsvRow(columns);
                Cerr << "Node configuration status is available with --format json." << Endl;
                return;
            }

            Cout << "Cluster mode: non-bridged" << Endl;
            if (!bridgeNodes.empty()) {
                Cout << "Bridge configuration: present on " << bridgeNodes.size() << " nodes" << Endl;
                if (allColumns) {
                    Cout << "Node IDs with bridge configuration: " << MakeRangeJoiner(", ", bridgeNodes) << Endl;
                }
            }
            if (!unreportedNodes.empty()) {
                Cout << "Bridge configuration: status unavailable on " << unreportedNodes.size() << " static nodes" << Endl;
                if (allColumns) {
                    Cout << "Unreported static node IDs: " << MakeRangeJoiner(", ", unreportedNodes) << Endl;
                }
            }
        }

        void PrintDatabaseRows(const std::vector<NBridge::TDatabaseSynchronization>& databases,
                               const TVector<TString>& columns, bool csv) {
            TVector<TVector<TString>> rows;
            for (const auto& item : databases) {
                rows.push_back({OptionalName(item.DatabasePath, csv), StorageModeToString(item.StorageMode),
                                OptionalName(item.PileName, csv), DatabaseSyncLabel(item.Groups),
                                item.Groups.Required ? SyncRatioToString(item.Groups) : OptionalText({}, csv),
                                OptionalText(PoolNames(item.StoragePools), csv), ToString(item.Groups.Syncing),
                                ToString(item.Groups.Pending), ToString(item.Groups.Unknown),
                                OptionalText(SyncErrorMessage(item.LastSyncError), csv), SyncErrorTime(item.LastSyncError, csv)});
            }
            PrintRows(DatabaseColumns(), rows, columns, csv);
        }

        void PrintGroupRows(const std::vector<NBridge::TGroupSynchronization>& groups, const TVector<TString>& columns, bool csv) {
            TVector<TVector<TString>> rows;
            for (const auto& item : groups) {
                const TString poolId = item.StoragePool
                                       ? ToString(item.StoragePool->Id.BoxId) + ":" + ToString(item.StoragePool->Id.PoolId)
                                       : TString{};
                rows.push_back({ToString(item.GroupId), item.StoragePool ? TString(item.StoragePool->Name) : OptionalText({}, csv),
                                OptionalName(item.PileName, csv), SyncStatusToString(item.SyncStatus),
                                OptionalText(PrettySyncPhase(item.SyncPhase), csv),
                                item.PhysicalGroupId ? ToString(*item.PhysicalGroupId) : OptionalText({}, csv), OptionalText(poolId, csv),
                                OptionalText(SyncErrorMessage(item.LastSyncError), csv), SyncErrorTime(item.LastSyncError, csv)});
            }
            PrintRows(GroupColumns(), rows, columns, csv);
        }

        const NBridge::TPileSynchronization* FindPile(const NBridge::TListPileSynchronizationResult* summary, const std::string& name) {
            if (summary) {
                const auto& piles = summary->GetPiles();
                const auto found = std::find_if(piles.begin(), piles.end(), [&](const auto& pile) { return pile.PileName && *pile.PileName == name; });
                if (found != piles.end()) {
                    return &*found;
                }
            }
            return nullptr;
        }

        TString PileOperationToString(NBridge::TPendingPileOperation::EKind kind) {
            switch (kind) {
                case NBridge::TPendingPileOperation::EKind::ADD:
                    return "provision-pile (in_progress)";
                case NBridge::TPendingPileOperation::EKind::REMOVE:
                    return "destroy-pile (in_progress)";
                default:
                    return PendingPileOperationKindToString(kind) + " (in_progress)";
            }
        }

        void PrintPiles(const NBridge::TGetClusterStateResult& result, const std::vector<NBridge::TPileStateUpdate>& state,
                        const NBridge::TListPileSynchronizationResult* summary, const TVector<TString>& columns, bool csv) {
            const auto& operation = result.GetPendingPileOperation();
            const bool rollback = operation && operation->Kind == NBridge::TPendingPileOperation::EKind::ROLLBACK;
            TVector<TVector<TString>> rows;
            for (const auto& pile : state) {
                const auto* counts = pile.State == NBridge::EPileState::INACTIVE ? nullptr : FindPile(summary, pile.PileName);
                const bool hasOperation = operation && !rollback && operation->PileName == pile.PileName;
                rows.push_back({OptionalName(pile.PileName, csv), PileStateToString(pile.State),
                                counts ? DatabaseSyncRatioToString(counts->Databases) : OptionalText({}, csv),
                                counts ? ToString(counts->Databases.Syncing) : OptionalText({}, csv),
                                counts ? ToString(counts->Databases.Pending) : OptionalText({}, csv),
                                counts ? ToString(counts->Databases.Unknown) : OptionalText({}, csv),
                                counts ? ToString(counts->Databases.NotRequired) : OptionalText({}, csv),
                                hasOperation ? PileOperationToString(operation->Kind) : OptionalText({}, csv),
                                hasOperation ? OptionalText(TString(operation->ErrorReason), csv) : OptionalText({}, csv)});
            }
            PrintRows(PileColumns(), rows, columns, csv);
            if (csv) {
                return;
            }
            Cout << Endl << "Generation: " << result.GetGeneration() << Endl;
            if (summary && summary->GetUnattributedStoragePools()) {
                Cout << "Database counts exclude storage pools without database attribution: "
                     << summary->GetUnattributedStoragePools() << Endl;
            }
            if (summary) {
                for (const auto& pile : summary->GetPiles()) {
                    if (!pile.PileName && pile.Databases.Total) {
                        Cout << "Databases with unavailable pile placement: " << pile.Databases.Total << Endl;
                    }
                }
            }
            if (result.IsConversionInProgress()) {
                Cout << "Conversion: storage in progress; piles cannot be provisioned yet" << Endl;
            }
            if (rollback) {
                Cout << "Rollback: storage in progress" << Endl;
                if (!operation->ErrorReason.empty()) {
                    Cout << "Rollback last error (being retried): " << operation->ErrorReason << Endl;
                }
            }
        }

        void PrintSharedStorage(const TString& requestedPath, const std::optional<std::string>& owner) {
            if (!requestedPath.empty() && owner && *owner != NormalizePath(requestedPath)) {
                Cout << "Shared storage: " << *owner << Endl;
            }
        }

    } // namespace

    TCommandBridgeList::TCommandBridgeList(bool allowEmptyDatabase)
        : TYdbReadOnlyCommand("list", {}, "List bridge state, storage synchronization and rollback progress.")
        , AllowEmptyDatabase(allowEmptyDatabase)
    {
    }

    void TCommandBridgeList::Config(TConfig& config) {
        TYdbReadOnlyCommand::Config(config);
        config.AllowEmptyDatabase = AllowEmptyDatabase;
        config.SetFreeArgsNum(0, 1);
        SetFreeArgTitle(0, "[piles|databases|groups]", "What to list. Defaults to piles.");
        config.Opts->GetOpts().GetFreeArgSpec(0).Completer(NLastGetopt::NComp::Choice({
            {"piles", "Pile state"},
            {"databases", "Database synchronization by pile"},
            {"groups", "Storage group synchronization by pile"},
        }));

        AddOutputFormats(config, {EDataFormat::Pretty, EDataFormat::Json, EDataFormat::Csv});
        config.Opts->AddLongOption('A', "all", "Show all table columns, or node IDs when the cluster is non-bridged.")
            .StoreTrue(&AllColumns);
        TVector<NLastGetopt::NComp::TChoice> columnChoices;
        for (const auto* columns : {&PileColumns(), &DatabaseColumns(), &GroupColumns()}) {
            for (const auto& column : *columns) {
                const auto found = std::find_if(columnChoices.begin(), columnChoices.end(), [&](const auto& choice) {
                    return choice.Choice == column;
                });
                if (found == columnChoices.end()) {
                    columnChoices.emplace_back(column);
                }
            }
        }
        config.Opts->AddLongOption("columns", "Comma-separated columns in output order (pretty/csv). Incompatible with -A.")
            .RequiredArgument("COLUMN,...").StoreResult(&ColumnsOption)
            .Completer(NLastGetopt::NComp::Choice(std::move(columnChoices)));
        config.Opts->MutuallyExclusive("all", "columns");
        config.Opts->AddLongOption("pile", "Show only this pile. An empty name is valid.")
            .RequiredArgument("NAME")
            .Handler([this](const TString& name) { PileName = name; });
        config.Opts->AddLongOption("database-path", "For databases and groups, filter by absolute database path. "
            "Serverless paths select shared storage.")
            .RequiredArgument("PATH")
            .StoreResult(&DatabasePath);
        config.Opts->AddLongOption("not-synced", "For databases and groups, show incomplete required synchronization, "
            "including unavailable statuses.")
            .StoreTrue(&NotSynced);
        config.Opts->AddLongOption("pool", "For groups, select a uniquely named storage pool.")
            .RequiredArgument("NAME")
            .StoreResult(&StoragePool);
        config.Opts->AddLongOption("pool-id", "For groups, select a storage pool by BOX:POOL ID. BOX may be zero and POOL must be nonzero.")
            .RequiredArgument("BOX:POOL")
            .Handler([this](const TString& value) { StoragePoolId = ParseStoragePoolId(value); });
        config.Opts->AddLongOption("mode", "For databases, filter by the mode of all database pools (bridged, non-bridged or mixed).")
            .RequiredArgument("MODE")
            .ChoicesWithCompletion({{"bridged", "Bridge synchronization enabled"},
                                    {"non-bridged", "Bridge synchronization disabled"},
                                    {"mixed", "Database has both bridged and non-bridged pools"}})
            .Handler([this](const TString& value) {
                if (value == "bridged") {
                    StorageModeFilter = NBridge::EStorageMode::Bridged;
                } else if (value == "non-bridged") {
                    StorageModeFilter = NBridge::EStorageMode::Ordinary;
                } else if (value == "mixed") {
                    StorageModeFilter = NBridge::EStorageMode::Mixed;
                } else {
                    throw TMisuseException() << "--mode must be bridged, non-bridged or mixed.";
                }
            });
        config.Opts->MutuallyExclusive("database-path", "pool");
        config.Opts->MutuallyExclusive("database-path", "pool-id");
        config.Opts->MutuallyExclusive("pool", "pool-id");
    }

    void TCommandBridgeList::Parse(TConfig& config) {
        TClientCommand::Parse(config);
        ParseOutputFormats();

        const auto args = config.ParseResult->GetFreeArgs();
        if (args.empty() || args.front() == "piles") {
            View = EView::Piles;
        } else if (args.front() == "databases") {
            View = EView::Databases;
        } else if (args.front() == "groups") {
            View = EView::Groups;
        } else {
            throw TMisuseException() << "Unknown view \"" << args.front() << "\". Use piles, databases or groups.";
        }

        const auto& allColumns = View == EView::Piles
                                 ? PileColumns()
                                 : View == EView::Databases ? DatabaseColumns() : GroupColumns();
        if (config.ParseResult->Has("columns")) {
            if (OutputFormat == EDataFormat::Json) {
                throw TMisuseException() << "--columns requires --format pretty or csv.";
            }
            Columns = StringSplitter(ColumnsOption).Split(',').ToList<TString>();
            for (const auto& column : Columns) {
                if (std::find(allColumns.begin(), allColumns.end(), column) == allColumns.end()) {
                    throw TMisuseException() << "Unknown column \"" << column << "\". Available columns: " << JoinSeq(", ", allColumns) << ".";
                }
            }
            if (Columns.empty()) {
                throw TMisuseException() << "--columns requires at least one column. Available columns: " << JoinSeq(", ", allColumns) << ".";
            }
        } else {
            Columns = View == EView::Piles
                      ? PileColumns(AllColumns)
                      : View == EView::Databases ? DatabaseColumns(AllColumns) : GroupColumns(AllColumns);
        }

        if (config.ParseResult->Has("database-path")) {
            if (View == EView::Piles) {
                throw TMisuseException() << "--database-path requires databases or groups.";
            }
            if (DatabasePath.empty() || !DatabasePath.StartsWith('/')) {
                throw TMisuseException() << "--database-path must be an absolute database path.";
            }
        }
        const bool hasPool = config.ParseResult->Has("pool");
        if (View != EView::Groups && (hasPool || StoragePoolId)) {
            throw TMisuseException() << "--pool and --pool-id require groups.";
        }
        if (hasPool && StoragePool.empty()) {
            throw TMisuseException() << "--pool requires a non-empty pool name.";
        }
        if (View == EView::Piles && NotSynced) {
            throw TMisuseException() << "--not-synced requires databases or groups.";
        }
        if (View != EView::Databases && StorageModeFilter) {
            throw TMisuseException() << "--mode requires databases.";
        }
    }

    int TCommandBridgeList::Run(TConfig& config) {
        auto driver = CreateDriver(config);
        NBridge::TBridgeClient client(driver);
        const bool csv = OutputFormat == EDataFormat::Csv;
        if (View == EView::Piles) {
            const auto cluster = client.GetClusterState(NBridge::TGetClusterStateSettings()
                .IncludeUnbridgedStatus(!PileName)).GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(cluster);
            if (const auto& unbridged = cluster.GetUnbridgedStatus()) {
                PrintUnbridgedStatus(*unbridged, OutputFormat, AllColumns, Columns);
                return EXIT_SUCCESS;
            }
            auto state = cluster.GetState();
            if (PileName) {
                std::erase_if(state, [&](const auto& pile) { return pile.PileName != *PileName; });
                if (state.empty()) {
                    throw TMisuseException() << "Unknown pile: " << *PileName;
                }
            }
            std::optional<NBridge::TListPileSynchronizationResult> summary;
            TString incompleteReason;
            const bool needsSummary = std::any_of(Columns.begin(), Columns.end(), [](const TString& column) {
                return column.StartsWith("Databases");
            });
            if (needsSummary) {
                auto response = client.ListPileSynchronization().GetValueSync();
                if (!response.IsSuccess()) {
                    incompleteReason = TStringBuilder() << "Synchronization summary unavailable: " << response.GetIssues().ToString();
                } else if (response.GetClusterStateGeneration() != cluster.GetGeneration()) {
                    incompleteReason = TStringBuilder() << "Synchronization summary generation " << response.GetClusterStateGeneration()
                                                        << " differs from cluster-state generation " << cluster.GetGeneration() << "; retry the query";
                } else {
                    NStatusHelpers::ThrowOnErrorOrPrintIssues(response);
                    summary.emplace(std::move(response));
                }
            }
            if (OutputFormat == EDataFormat::Json) {
                auto json = ClusterStateToJson(cluster, state);
                if (summary) {
                    json.InsertValue("synchronization_summary", SummaryToJson(*summary, PileName));
                }
                if (!incompleteReason.empty()) {
                    json.InsertValue("incomplete_reason", incompleteReason);
                }
                PrintJson(json);
            } else {
                PrintPiles(cluster, state, summary ? &*summary : nullptr, Columns, csv);
            }
            if (!incompleteReason.empty()) {
                Cerr << "Bridge diagnostics incomplete: " << incompleteReason << Endl;
                return EXIT_FAILURE;
            }
            return EXIT_SUCCESS;
        }
        if (View == EView::Databases) {
            auto settings = NBridge::TListDatabaseSynchronizationSettings().OnlyIncomplete(NotSynced);
            if (StorageModeFilter) {
                settings.StorageMode(*StorageModeFilter);
            }
            if (!DatabasePath.empty()) {
                settings.DatabasePath(DatabasePath);
            }
            if (PileName) {
                settings.PileName(*PileName);
            }
            const auto result = client.ListDatabaseSynchronization(settings).GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
            if (OutputFormat == EDataFormat::Json) {
                auto json = ListMetadataToJson(result.GetClusterStateGeneration(), result.GetObservedAt(), result.GetStorageOwnerDatabasePath());
                json.InsertValue("databases", DatabasesToJson(result.GetDatabases()));
                PrintJson(json);
            } else {
                PrintDatabaseRows(result.GetDatabases(), Columns, csv);
                if (!csv) {
                    Cout << Endl << "Generation: " << result.GetClusterStateGeneration() << Endl;
                    PrintSharedStorage(DatabasePath, result.GetStorageOwnerDatabasePath());
                }
            }
            return EXIT_SUCCESS;
        }
        NBridge::TListGroupSynchronizationSettings settings;
        if (PileName) {
            settings.PileName(*PileName);
        }
        if (NotSynced) {
            settings.Statuses({NBridge::ESynchronizationStatus::Syncing, NBridge::ESynchronizationStatus::Pending, NBridge::ESynchronizationStatus::Unknown});
        }
        const auto scope = StoragePoolId           ? NBridge::TGroupSynchronizationScope::PoolId(*StoragePoolId)
                           : !DatabasePath.empty() ? NBridge::TGroupSynchronizationScope::Database(DatabasePath)
                           : !StoragePool.empty()  ? NBridge::TGroupSynchronizationScope::PoolName(StoragePool)
                                                   : NBridge::TGroupSynchronizationScope::All();
        const auto result = client.ListGroupSynchronization(scope, settings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        if (OutputFormat == EDataFormat::Json) {
            auto json = ListMetadataToJson(result.GetClusterStateGeneration(), result.GetObservedAt(), result.GetStorageOwnerDatabasePath());
            json.InsertValue("groups", GroupsToJson(result.GetGroups()));
            PrintJson(json);
        } else {
            PrintGroupRows(result.GetGroups(), Columns, csv);
            if (!csv) {
                Cout << Endl << "Generation: " << result.GetClusterStateGeneration() << Endl;
                PrintSharedStorage(DatabasePath, result.GetStorageOwnerDatabasePath());
            }
        }
        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
