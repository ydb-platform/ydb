#include "ydb_bridge.h"

#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/cast.h>
#include <util/string/join.h>

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
                    return "INACTIVE";
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
                    return "NOT_REQUIRED";
                default:
                    return "UNKNOWN";
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
                return "NOT_REQUIRED";
            }
            if (groups.Synced == groups.Required) {
                return "SYNCED";
            }
            if (groups.Syncing) {
                return "SYNCING";
            }
            return groups.Unknown ? "UNKNOWN" : "PENDING";
        }

        TString StorageModeToString(NBridge::EStorageMode mode) {
            switch (mode) {
                case NBridge::EStorageMode::Bridged:
                    return "bridged";
                case NBridge::EStorageMode::Ordinary:
                    return "ordinary";
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

        void PrintRows(const TVector<TString>& headers, const TVector<TVector<TString>>& rows, bool csv, const TString& note = {}) {
            if (csv) {
                WriteCsvRow(headers);
                for (const auto& row : rows) {
                    WriteCsvRow(row);
                }
                return;
            }
            TPrettyTable table(headers, TPrettyTableConfig().WithoutRowDelimiters());
            for (const auto& row : rows) {
                auto& out = table.AddRow();
                for (size_t i = 0; i < row.size(); ++i) {
                    out.Column(i, row[i]);
                }
            }
            if (!note.empty()) {
                table.AddRow().FreeText(note);
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

        TString GroupPoolToString(const std::optional<NBridge::TStoragePool>& pool, bool csv) {
            if (!pool) {
                return csv ? TString{} : TString("—");
            }
            return csv ? NJson::WriteJson(StoragePoolToJson(*pool), false, true) : TString(pool->Name);
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
            json.InsertValue("unknown", groups.Unknown);
            return json;
        }

        NJson::TJsonValue DatabaseCountsToJson(const NBridge::TDatabaseSyncCounts& databases) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("total", databases.Total);
            json.InsertValue("synced", databases.Synced);
            json.InsertValue("syncing", databases.Syncing);
            json.InsertValue("pending", databases.Pending);
            json.InsertValue("not_required", databases.NotRequired);
            json.InsertValue("unknown", databases.Unknown);
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

        TVector<TString> PileCsvHeaders(bool detailed) {
            if (!detailed) {
                return {"pile_name", "state", "generation"};
            }
            return {"pile_name", "pile_name_present", "state", "databases_synced", "databases_total", "databases_syncing",
                    "databases_pending", "databases_not_required", "databases_unknown", "cluster_unattributed_storage_pools", "generation"};
        }

        void PrintUnbridgedStatus(const NBridge::TUnbridgedStatus& status, EDataFormat format, bool detailed) {
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
                WriteCsvRow(PileCsvHeaders(detailed));
                Cerr << "Node configuration status is available with --format json." << Endl;
                return;
            }

            if (bridgeNodes.empty() && unreportedNodes.empty()) {
                Cout << "Cluster mode: ordinary" << Endl;
                return;
            }
            Cout << (bridgeNodes.empty() ? "Bridge configuration status incomplete" : "Bridge configuration still in use") << Endl;
            const auto printNodes = [detailed](TStringBuf countLabel, TStringBuf idsLabel, const std::vector<uint32_t>& nodes) {
                if (nodes.empty()) {
                    return;
                }
                Cout << (detailed ? idsLabel : countLabel) << ": ";
                if (detailed) {
                    Cout << MakeRangeJoiner(", ", nodes);
                } else {
                    Cout << nodes.size();
                }
                Cout << Endl;
            };
            printNodes("Nodes with bridge config", "Node IDs with bridge config", bridgeNodes);
            printNodes("Unreported static nodes", "Unreported static node IDs", unreportedNodes);
            if (!bridgeNodes.empty()) {
                Cout << "To return to ordinary mode, remove bridge configuration and restart the nodes." << Endl;
            }
        }

        void PrintDatabaseRows(const std::vector<NBridge::TDatabaseSynchronization>& databases, bool detailed, bool csv) {
            TVector<TString> headers = csv
                                           ? TVector<TString>{"database_path", "storage_pools", "storage_mode", "pile_name", "pile_name_present", "groups_synced", "groups_required"}
                                           : TVector<TString>{"Database", "Pools", "Mode", "Pile", "Sync", "Synced groups"};
            if (detailed) {
                const TVector<TString> extras = csv
                                                    ? TVector<TString>{"groups_syncing", "groups_pending", "groups_unknown", "last_sync_error", "last_sync_error_occurred_at"}
                                                    : TVector<TString>{"Syncing groups", "Pending groups", "Unknown groups", "Last sync error", "Last error time"};
                headers.insert(headers.end(), extras.begin(), extras.end());
            }
            TVector<TVector<TString>> rows;
            for (const auto& item : databases) {
                TVector<TString> row{OptionalName(item.DatabasePath, csv),
                                     csv ? NJson::WriteJson(StoragePoolsToJson(item.StoragePools), false, true) : PoolNames(item.StoragePools),
                                     StorageModeToString(item.StorageMode), OptionalName(item.PileName, csv)};
                if (csv) {
                    row.insert(row.end(), {item.PileName ? "true" : "false", ToString(item.Groups.Synced), ToString(item.Groups.Required)});
                } else {
                    row.insert(row.end(), {DatabaseSyncLabel(item.Groups), item.Groups.Required ? SyncRatioToString(item.Groups) : TString("—")});
                }
                if (detailed) {
                    row.insert(row.end(), {ToString(item.Groups.Syncing), ToString(item.Groups.Pending), ToString(item.Groups.Unknown),
                                           SyncErrorMessage(item.LastSyncError), SyncErrorTime(item.LastSyncError, csv)});
                }
                rows.push_back(std::move(row));
            }
            PrintRows(headers, rows, csv);
        }

        void PrintGroupRows(const std::vector<NBridge::TGroupSynchronization>& groups, bool detailed, bool csv) {
            TVector<TString> headers = csv
                                           ? TVector<TString>{"group_id", "storage_pool", "pile_name", "pile_name_present", "sync_status"}
                                           : TVector<TString>{"Group", "Pool", "Pile", "Sync"};
            if (detailed) {
                const TVector<TString> extras = csv
                                                    ? TVector<TString>{"physical_group_id", "sync_phase", "last_sync_error", "last_sync_error_occurred_at"}
                                                    : TVector<TString>{"Physical group", "Phase", "Last sync error", "Last error time"};
                headers.insert(headers.end(), extras.begin(), extras.end());
            }
            TVector<TVector<TString>> rows;
            for (const auto& item : groups) {
                TVector<TString> row{ToString(item.GroupId),
                                     GroupPoolToString(item.StoragePool, csv),
                                     OptionalName(item.PileName, csv)};
                if (csv) {
                    row.push_back(item.PileName ? "true" : "false");
                }
                row.push_back(SyncStatusToString(item.SyncStatus));
                if (detailed) {
                    row.insert(row.end(), {item.PhysicalGroupId ? ToString(*item.PhysicalGroupId) : csv ? TString{}
                                                                                                        : TString("—"),
                                           csv ? TString(item.SyncPhase) : PrettySyncPhase(item.SyncPhase), SyncErrorMessage(item.LastSyncError),
                                           SyncErrorTime(item.LastSyncError, csv)});
                }
                rows.push_back(std::move(row));
            }
            PrintRows(headers, rows, csv);
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

        void PrintPiles(const NBridge::TGetClusterStateResult& result, const std::vector<NBridge::TPileStateUpdate>& state,
                        const NBridge::TListPileSynchronizationResult* summary, const std::optional<TString>& pileFilter, bool detailed, bool csv) {
            if (csv && !detailed) {
                WriteCsvRow(PileCsvHeaders(detailed));
                for (const auto& pile : state) {
                    WriteCsvRow({TString(pile.PileName), PileStateToString(pile.State), ToString(result.GetGeneration())});
                }
                return;
            }
            if (summary || csv) {
                const TVector<TString> headers = csv
                    ? PileCsvHeaders(detailed)
                    : TVector<TString>{"Pile", "State", "Synced/total databases", "Syncing databases", "Pending databases",
                                       "Not required databases", "Unknown databases"};
                TVector<TVector<TString>> rows;
                auto addRow = [&](const std::optional<std::string>& name, const TString& pileState,
                                  const NBridge::TPileSynchronization* counts) {
                    TVector<TString> row{OptionalName(name, csv)};
                    if (csv) {
                        row.push_back(name ? "true" : "false");
                        row.push_back(pileState);
                        if (counts) {
                            row.insert(row.end(), {ToString(counts->Databases.Synced), ToString(counts->Databases.Total),
                                                   ToString(counts->Databases.Syncing), ToString(counts->Databases.Pending),
                                                   ToString(counts->Databases.NotRequired), ToString(counts->Databases.Unknown)});
                        } else {
                            row.insert(row.end(), 6, TString{});
                        }
                        row.insert(row.end(), {summary ? ToString(summary->GetUnattributedStoragePools()) : TString{},
                                               ToString(result.GetGeneration())});
                    } else {
                        row.insert(row.end(), {name ? pileState : TString("—"),
                                               counts ? DatabaseSyncRatioToString(counts->Databases) : TString("—"),
                                               counts ? ToString(counts->Databases.Syncing) : TString("—"),
                                               counts ? ToString(counts->Databases.Pending) : TString("—"),
                                               counts ? ToString(counts->Databases.NotRequired) : TString("—"),
                                               counts ? ToString(counts->Databases.Unknown) : TString("—")});
                    }
                    rows.push_back(std::move(row));
                };
                for (const auto& pile : state) {
                    addRow(pile.PileName, PileStateToString(pile.State), FindPile(summary, pile.PileName));
                }
                if (summary && !pileFilter) {
                    for (const auto& pile : summary->GetPiles()) {
                        if (!pile.PileName) {
                            addRow(std::nullopt, {}, &pile);
                        }
                    }
                }
                TString note;
                if (summary && summary->GetUnattributedStoragePools()) {
                    note = TStringBuilder() << "Database counts exclude storage pools without database attribution: "
                                            << summary->GetUnattributedStoragePools();
                }
                PrintRows(headers, rows, csv, note);
            } else {
                for (const auto& pile : state) {
                    Cout << "Pile " << pile.PileName << ": " << PileStateToString(pile.State) << Endl;
                }
            }
            if (csv) {
                return;
            }
            Cout << "Generation: " << result.GetGeneration() << Endl;
            if (result.IsConversionInProgress()) {
                Cout << "Conversion from non-bridged cluster is in progress, piles can't be added yet" << Endl;
            }
            if (const auto& op = result.GetPendingPileOperation()) {
                if (op->Kind == NBridge::TPendingPileOperation::EKind::ROLLBACK) {
                    Cout << "Rollback: storage in progress" << Endl;
                } else {
                    Cout << "Pending operation on pile " << op->PileName << ": " << PendingPileOperationKindToString(op->Kind) << Endl;
                }
                if (!op->ErrorReason.empty()) {
                    Cout << "Last attempt has failed (being retried): " << op->ErrorReason << Endl;
                }
            }
        }

        void PrintSelectedDatabase(const TString& requestedPath, const std::optional<std::string>& owner) {
            if (!requestedPath.empty()) {
                Cout << "Database: " << requestedPath << Endl;
                if (owner && *owner != NormalizePath(requestedPath)) {
                    Cout << "Shared storage: " << *owner << Endl;
                }
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
        config.Opts->AddLongOption("detailed", "Show database counts for piles or node IDs when bridge state is absent. "
            "Show group counts and the last error for databases, "
            "or physical group IDs, synchronization phases and the last error for groups. "
            "Database and group JSON includes available details by default.")
            .StoreTrue(&Detailed);
        config.Opts->AddLongOption("pile", "Show only this pile. An empty name is valid.")
            .RequiredArgument("NAME")
            .Handler([this](const TString& name) { PileName = name; });
        config.Opts->AddLongOption("database-path", "For databases and groups, filter by absolute database path. "
            "Serverless paths select shared storage.")
            .RequiredArgument("PATH")
            .StoreResult(&DatabasePath);
        config.Opts->AddLongOption("not-synced", "For databases and groups, show incomplete required synchronization, including unknown states.")
            .StoreTrue(&NotSynced);
        config.Opts->AddLongOption("pool", "For groups, select a uniquely named storage pool.")
            .RequiredArgument("NAME")
            .StoreResult(&StoragePool);
        config.Opts->AddLongOption("pool-id", "For groups, select a storage pool by BOX:POOL ID. BOX may be zero and POOL must be nonzero.")
            .RequiredArgument("BOX:POOL")
            .Handler([this](const TString& value) { StoragePoolId = ParseStoragePoolId(value); });
        config.Opts->AddLongOption("mode", "For databases, filter by the mode of all database pools (bridged, ordinary or mixed).")
            .RequiredArgument("MODE")
            .ChoicesWithCompletion({{"bridged", "Bridge synchronization enabled"},
                                    {"ordinary", "Bridge synchronization disabled"},
                                    {"mixed", "Database has both bridged and ordinary pools"}})
            .Handler([this](const TString& value) {
                if (value == "bridged") {
                    StorageModeFilter = NBridge::EStorageMode::Bridged;
                } else if (value == "ordinary") {
                    StorageModeFilter = NBridge::EStorageMode::Ordinary;
                } else if (value == "mixed") {
                    StorageModeFilter = NBridge::EStorageMode::Mixed;
                } else {
                    throw TMisuseException() << "--mode must be bridged, ordinary or mixed.";
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
            if (!PileName && cluster.GetStatus() == EStatus::NOT_FOUND) {
                // Servers predating IncludeUnbridgedStatus expose the node report through GetRollbackStatus.
                const auto rollback = client.GetRollbackStatus().GetValueSync();
                if (rollback.GetStatus() != EStatus::CLIENT_CALL_UNIMPLEMENTED) {
                    NStatusHelpers::ThrowOnErrorOrPrintIssues(rollback);
                    if (rollback.IsBridged() || rollback.IsRollbackInProgress()) {
                        Cerr << "Bridge cluster state changed. Retry the request." << Endl;
                        return EXIT_FAILURE;
                    }
                    const NBridge::TUnbridgedStatus unbridged{
                        .BridgeConfigNodeIds = rollback.GetBridgeConfigNodeIds(),
                        .UnreportedStaticNodeIds = rollback.GetUnreportedStaticNodeIds(),
                    };
                    if (!rollback.IsComplete() && unbridged.BridgeConfigNodeIds.empty() && unbridged.UnreportedStaticNodeIds.empty()) {
                        Cerr << "Unbridged status report is incomplete." << Endl;
                        return EXIT_FAILURE;
                    }
                    PrintUnbridgedStatus(unbridged, OutputFormat, Detailed);
                    return EXIT_SUCCESS;
                }
            }
            NStatusHelpers::ThrowOnErrorOrPrintIssues(cluster);
            if (const auto& unbridged = cluster.GetUnbridgedStatus()) {
                PrintUnbridgedStatus(*unbridged, OutputFormat, Detailed);
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
            if (Detailed) {
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
                PrintPiles(cluster, state, summary ? &*summary : nullptr, PileName, Detailed, csv);
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
                if (!csv) {
                    PrintSelectedDatabase(DatabasePath, result.GetStorageOwnerDatabasePath());
                }
                PrintDatabaseRows(result.GetDatabases(), Detailed, csv);
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
            if (!csv) {
                PrintSelectedDatabase(DatabasePath, result.GetStorageOwnerDatabasePath());
            }
            PrintGroupRows(result.GetGroups(), Detailed, csv);
        }
        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
