#include "ydb_bridge.h"

#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/cast.h>
#include <util/string/join.h>

#include <algorithm>
#include <set>

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
            }
            return "UNKNOWN";
        }

        TString CopySyncStateToString(NBridge::ECopySyncState state, bool pretty = false) {
            switch (state) {
                case NBridge::ECopySyncState::Synced:
                    return "SYNCED";
                case NBridge::ECopySyncState::Syncing:
                    return "SYNCING";
                case NBridge::ECopySyncState::Pending:
                    return "PENDING";
                case NBridge::ECopySyncState::NotRequired:
                    return pretty ? "N/A" : "NOT_REQUIRED";
                default:
                    return "UNKNOWN";
            }
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

        TString GroupsSyncedToString(const NBridge::TGroupSyncCounts& counts) {
            return ToString(counts.Synced) + "/" + ToString(counts.Total);
        }

        TString LastErrorTimeToString(uint64_t timestampUs, bool csv) {
            if (csv) {
                return ToString(timestampUs);
            }
            return timestampUs ? TInstant::MicroSeconds(timestampUs).ToString() : "—";
        }

        TString PrettySyncStage(const std::string& stage) {
            if (stage == "WRITE_KEEP") {
                return "KEEP";
            }
            if (stage == "WRITE_KEEP_BARRIER_DONOTKEEP") {
                return "GC";
            }
            if (stage == "WRITE_KEEP_BARRIER_DONOTKEEP_DATA") {
                return "DATA";
            }
            return TString(stage);
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

        void PrintRows(const TVector<TString>& headers, const TVector<TVector<TString>>& rows, bool csv) {
            if (csv) {
                WriteCsvRow(headers);
                for (const auto& row : rows) {
                    WriteCsvRow(row);
                }
            } else {
                TPrettyTable table(headers, TPrettyTableConfig().WithoutRowDelimiters());
                for (const auto& row : rows) {
                    auto& out = table.AddRow();
                    for (size_t i = 0; i < row.size(); ++i) {
                        out.Column(i, row[i]);
                    }
                }
                Cout << table;
            }
        }

        NJson::TJsonValue StoragePoolsToJson(const std::vector<std::string>& pools) {
            NJson::TJsonValue json(NJson::JSON_ARRAY);
            for (const auto& pool : pools) {
                json.AppendValue(pool);
            }
            return json;
        }

        NJson::TJsonValue SyncCountsToJson(const NBridge::TGroupSyncCounts& counts) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("total", counts.Total);
            json.InsertValue("synced", counts.Synced);
            json.InsertValue("running", counts.Running);
            json.InsertValue("pending", counts.Pending);
            json.InsertValue("unknown", counts.Unknown);
            return json;
        }

        NJson::TJsonValue StorageStateToJson(const NBridge::TStorageState& state) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json.InsertValue("generation", state.Generation);
            json.InsertValue("collected_at_us", state.CollectedAtUs);
            json.InsertValue("database_path", state.DatabasePath);
            json.InsertValue("storage_owner_database", state.StorageOwnerDatabase);
            json.InsertValue("unavailable_reason", state.UnavailableReason);
            json.InsertValue("next_page_token", state.NextPageToken);
            NJson::TJsonValue piles(NJson::JSON_ARRAY);
            for (const auto& pile : state.Piles) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("pile_name", pile.PileName);
                item.InsertValue("user_groups", SyncCountsToJson(pile.UserGroups));
                item.InsertValue("system_groups", SyncCountsToJson(pile.SystemGroups));
                piles.AppendValue(item);
            }
            json.InsertValue("piles", piles);
            NJson::TJsonValue databases(NJson::JSON_ARRAY);
            for (const auto& database : state.Databases) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("database_path", database.DatabasePath);
                item.InsertValue("storage_pools", StoragePoolsToJson(database.StoragePools));
                item.InsertValue("mode", StorageModeToString(database.Mode));
                item.InsertValue("pile_name", database.PileName);
                item.InsertValue("groups", SyncCountsToJson(database.Groups));
                item.InsertValue("state", CopySyncStateToString(database.State));
                item.InsertValue("last_error", database.LastError);
                item.InsertValue("last_error_timestamp_us", database.LastErrorTimestampUs);
                databases.AppendValue(item);
            }
            json.InsertValue("databases", databases);
            NJson::TJsonValue groups(NJson::JSON_ARRAY);
            for (const auto& group : state.Groups) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item.InsertValue("group_id", group.GroupId);
                item.InsertValue("part_group_id", group.PartGroupId);
                item.InsertValue("storage_pool_name", group.StoragePoolName);
                item.InsertValue("pile_name", group.PileName);
                item.InsertValue("state", CopySyncStateToString(group.State));
                item.InsertValue("stage", group.Stage);
                item.InsertValue("last_error", group.LastError);
                item.InsertValue("last_error_timestamp_us", group.LastErrorTimestampUs);
                groups.AppendValue(item);
            }
            json.InsertValue("groups", groups);
            return json;
        }

        void PrintJson(const NBridge::TGetClusterStateResult& result, const std::vector<NBridge::TPileStateUpdate>& state,
                       const NBridge::TStorageState* storage, const TString& incompleteReason) {
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
            if (storage) {
                json.InsertValue("storage_state", StorageStateToJson(*storage));
            }
            if (!incompleteReason.empty()) {
                json.InsertValue("incomplete_reason", incompleteReason);
            }
            NJson::WriteJson(&Cout, &json, true);
            Cout << Endl;
        }

        void PrintDatabaseRows(const NBridge::TStorageState& storage, bool detailed, bool csv) {
            TVector<TString> headers = csv
                                           ? TVector<TString>{"database_path", "storage_pools", "mode", "pile_name", "state", "groups_synced", "groups_total"}
                                           : TVector<TString>{"Database", "Pools", "Mode", "Pile", "Sync", "Groups synced"};
            if (detailed) {
                const TVector<TString> detailHeaders = csv
                                                           ? TVector<TString>{"running", "pending", "unknown", "last_error", "last_error_timestamp_us"}
                                                           : TVector<TString>{"Running", "Pending", "Unknown", "Last sync error", "Last error time"};
                headers.insert(headers.end(), detailHeaders.begin(), detailHeaders.end());
            }
            TVector<TVector<TString>> rows;
            for (const auto& item : storage.Databases) {
                TVector<TString> row{TString(item.DatabasePath), csv ? NJson::WriteJson(StoragePoolsToJson(item.StoragePools), false) : JoinSeq(", ", item.StoragePools),
                                     StorageModeToString(item.Mode), TString(item.PileName), CopySyncStateToString(item.State, !csv)};
                if (csv) {
                    row.insert(row.end(), {ToString(item.Groups.Synced), ToString(item.Groups.Total)});
                } else {
                    row.push_back(item.State == NBridge::ECopySyncState::NotRequired ? "—" : GroupsSyncedToString(item.Groups));
                }
                if (detailed) {
                    row.insert(row.end(), {ToString(item.Groups.Running), ToString(item.Groups.Pending),
                                           ToString(item.Groups.Unknown), TString(item.LastError), LastErrorTimeToString(item.LastErrorTimestampUs, csv)});
                }
                rows.push_back(std::move(row));
            }
            PrintRows(headers, rows, csv);
        }

        void PrintGroupRows(const NBridge::TStorageState& storage, bool detailed, bool csv) {
            TVector<TString> headers = csv
                                           ? TVector<TString>{"group_id", "part_group_id", "storage_pool_name", "pile_name", "state"}
                                           : TVector<TString>{"Group", "Part group", "Pool", "Pile", "Sync"};
            if (detailed) {
                const TVector<TString> detailHeaders = csv
                                                           ? TVector<TString>{"stage", "last_error", "last_error_timestamp_us"}
                                                           : TVector<TString>{"Stage", "Last sync error", "Last error time"};
                headers.insert(headers.end(), detailHeaders.begin(), detailHeaders.end());
            }
            TVector<TVector<TString>> rows;
            for (const auto& item : storage.Groups) {
                TVector<TString> row{ToString(item.GroupId), ToString(item.PartGroupId), TString(item.StoragePoolName),
                                     TString(item.PileName), CopySyncStateToString(item.State, !csv)};
                if (detailed) {
                    row.insert(row.end(), {csv ? TString(item.Stage) : PrettySyncStage(item.Stage), TString(item.LastError),
                                           LastErrorTimeToString(item.LastErrorTimestampUs, csv)});
                }
                rows.push_back(std::move(row));
            }
            PrintRows(headers, rows, csv);
        }

        const NBridge::TPileStorageState* FindPileStorage(const NBridge::TStorageState* storage, const std::string& pileName) {
            if (storage && storage->UnavailableReason.empty()) {
                const auto found = std::find_if(storage->Piles.begin(), storage->Piles.end(), [&](const auto& pile) {
                    return pile.PileName == pileName;
                });
                if (found != storage->Piles.end()) {
                    return &*found;
                }
            }
            return nullptr;
        }

        void PrintPiles(const NBridge::TGetClusterStateResult& result, const std::vector<NBridge::TPileStateUpdate>& state,
                        const NBridge::TStorageState* storage, bool detailed, bool csv) {
            if (csv) {
                WriteCsvRow(detailed
                                ? TVector<TString>{"pile_name", "state", "groups_synced", "groups_total", "running", "pending", "unknown",
                                                   "system_groups_synced", "system_groups_total", "generation"}
                                : TVector<TString>{"pile_name", "state", "generation"});
                for (const auto& pile : state) {
                    TVector<TString> row{TString(pile.PileName), PileStateToString(pile.State)};
                    if (detailed) {
                        if (const auto* counts = FindPileStorage(storage, pile.PileName)) {
                            row.insert(row.end(), {ToString(counts->UserGroups.Synced), ToString(counts->UserGroups.Total),
                                                   ToString(counts->UserGroups.Running), ToString(counts->UserGroups.Pending), ToString(counts->UserGroups.Unknown),
                                                   ToString(counts->SystemGroups.Synced), ToString(counts->SystemGroups.Total)});
                        } else {
                            row.insert(row.end(), 7, TString{});
                        }
                    }
                    row.push_back(ToString(result.GetGeneration()));
                    WriteCsvRow(row);
                }
                return;
            }
            if (detailed && storage && storage->UnavailableReason.empty()) {
                TVector<TVector<TString>> rows;
                for (const auto& pile : state) {
                    if (const auto* counts = FindPileStorage(storage, pile.PileName)) {
                        rows.push_back({TString(pile.PileName), PileStateToString(pile.State), GroupsSyncedToString(counts->UserGroups),
                                        ToString(counts->UserGroups.Running), ToString(counts->UserGroups.Pending), ToString(counts->UserGroups.Unknown),
                                        GroupsSyncedToString(counts->SystemGroups)});
                    }
                }
                PrintRows({"Pile", "State", "Groups synced", "Running", "Pending", "Unknown", "System groups synced"}, rows, false);
            } else {
                for (const auto& pile : state) {
                    Cout << "Pile " << pile.PileName << ": " << PileStateToString(pile.State) << Endl;
                }
            }
            Cout << "Generation: " << result.GetGeneration() << Endl;
            if (result.IsConversionInProgress()) {
                Cout << "Conversion from non-bridged cluster is in progress, piles can't be added yet" << Endl;
            }
            if (const auto& op = result.GetPendingPileOperation()) {
                Cout << "Pending operation on pile " << op->PileName << ": " << PendingPileOperationKindToString(op->Kind) << Endl;
                if (!op->ErrorReason.empty()) {
                    Cout << "Last attempt has failed (being retried): " << op->ErrorReason << Endl;
                }
            }
        }

        struct TListingResult {
            NBridge::TGetClusterStateResult Result;
            TString IncompleteReason;
        };

        TListingResult FetchListing(NBridge::TBridgeClient& client, const std::optional<NBridge::TStorageQuery>& storageQuery) {
            NBridge::TGetClusterStateSettings settings;
            if (storageQuery) {
                settings.StorageQuery(*storageQuery);
            }
            auto result = client.GetClusterState(settings).GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
            if (!storageQuery || storageQuery->View == NBridge::EStorageView::Piles) {
                return {std::move(result), {}};
            }

            // Bounded controller scans may produce empty pages. Preserve the cursor if searching for a matching row stops.
            auto query = *storageQuery;
            const auto deadline = TDeadline::AfterDuration(TDuration::Seconds(30));
            settings.Deadline(deadline);
            std::set<std::string> visitedTokens{query.PageToken};
            size_t emptyPages = 0;
            while (const auto& page = result.GetStorageState()) {
                if (!page->UnavailableReason.empty() || page->NextPageToken.empty() ||
                    (query.View == NBridge::EStorageView::Databases ? !page->Databases.empty() : !page->Groups.empty())) {
                    break;
                }
                if (!visitedTokens.insert(page->NextPageToken).second) {
                    return {std::move(result), "Server returned a repeated storage page token"};
                }
                if (++emptyPages >= 32 || deadline <= TDeadline::Now()) {
                    return {std::move(result), "Storage listing search is incomplete; continue with the returned page token"};
                }
                query.PageToken = page->NextPageToken;
                settings.StorageQuery(query);
                auto next = client.GetClusterState(settings).GetValueSync();
                if (!next.IsSuccess()) {
                    return {std::move(result), TStringBuilder() << "Storage page request failed: " << next.GetIssues().ToString()};
                }
                NStatusHelpers::ThrowOnErrorOrPrintIssues(next);
                result = std::move(next);
            }
            return {std::move(result), {}};
        }

    } // namespace

    TCommandBridgeList::TCommandBridgeList(bool allowEmptyDatabase)
        : TYdbReadOnlyCommand("list", {}, "List state of each pile in bridge mode")
        , AllowEmptyDatabase(allowEmptyDatabase)
    {
    }

    void TCommandBridgeList::Config(TConfig& config) {
        TYdbReadOnlyCommand::Config(config);
        config.AllowEmptyDatabase = AllowEmptyDatabase;
        config.SetFreeArgsNum(0);
        AddOutputFormats(config, {EDataFormat::Pretty, EDataFormat::Json, EDataFormat::Csv});
        config.Opts->AddLongOption("detailed", "Show group synchronization counts for piles, or additional fields for databases and groups.")
            .StoreTrue(&Detailed);
        config.Opts->AddLongOption("databases", "Show database storage copies by pile. Serverless databases use their shared database's storage.")
            .NoArgument();
        config.Opts->AddLongOption("groups", "Show group copies in the database or pool selected with --db-path or --pool.")
            .NoArgument();
        config.Opts->AddLongOption("db-path", "Filter database or group copies by absolute database path. Does not change the connection database.")
            .RequiredArgument("PATH")
            .StoreResult(&DatabasePath);
        config.Opts->AddLongOption("pool", "With --groups, select this storage pool instead of a database.")
            .RequiredArgument("NAME")
            .StoreResult(&StoragePool);
        config.Opts->AddLongOption("pile", "Show only this pile.")
            .RequiredArgument("NAME")
            .StoreResult(&PileName);
        config.Opts->AddLongOption("not-synced", "With --databases or --groups, show required copies that are not confirmed synchronized, including unknown states.")
            .StoreTrue(&NotSynced);
        config.Opts->AddLongOption("page-size", "Maximum database or group copy rows to return (default: 1000; maximum: 10000).")
            .RequiredArgument("NUM")
            .StoreResult(&PageSize);
        config.Opts->AddLongOption("page-token", "Continue a database or group listing from this page token.")
            .RequiredArgument("STRING")
            .StoreResult(&PageToken);
        config.Opts->MutuallyExclusive("databases", "groups");
        config.Opts->MutuallyExclusive("db-path", "pool");
    }

    void TCommandBridgeList::Parse(TConfig& config) {
        TClientCommand::Parse(config);
        ParseOutputFormats();
        View = config.ParseResult->Has("groups")      ? NBridge::EStorageView::Groups
               : config.ParseResult->Has("databases") ? NBridge::EStorageView::Databases
                                                      : NBridge::EStorageView::Piles;
        if (View == NBridge::EStorageView::Groups && DatabasePath.empty() && StoragePool.empty()) {
            throw TMisuseException() << "--groups requires --db-path or --pool.";
        }
        if (config.ParseResult->Has("db-path") && View == NBridge::EStorageView::Piles) {
            throw TMisuseException() << "--db-path requires --databases or --groups.";
        }
        if (config.ParseResult->Has("db-path") && (DatabasePath.empty() || !DatabasePath.StartsWith('/'))) {
            throw TMisuseException() << "--db-path must be an absolute database path.";
        }
        if (config.ParseResult->Has("pool") && (View != NBridge::EStorageView::Groups || StoragePool.empty())) {
            throw TMisuseException() << "--pool requires --groups and a non-empty pool name.";
        }
        if (config.ParseResult->Has("pile") && PileName.empty()) {
            throw TMisuseException() << "--pile must not be empty.";
        }
        if (NotSynced && View == NBridge::EStorageView::Piles) {
            throw TMisuseException() << "--not-synced requires --databases or --groups.";
        }
        if ((config.ParseResult->Has("page-size") || config.ParseResult->Has("page-token")) && View == NBridge::EStorageView::Piles) {
            throw TMisuseException() << "--page-size and --page-token require --databases or --groups.";
        }
        if (PageSize == 0 || PageSize > 10000) {
            throw TMisuseException() << "--page-size must be between 1 and 10000.";
        }
    }

    int TCommandBridgeList::Run(TConfig& config) {
        auto driver = CreateDriver(config);
        NBridge::TBridgeClient client(driver);
        std::optional<NBridge::TStorageQuery> query;
        if (Detailed || View != NBridge::EStorageView::Piles) {
            query.emplace(NBridge::TStorageQuery{
                .View = View,
                .DatabasePath = DatabasePath,
                .StoragePool = StoragePool,
                .PileName = PileName,
                .NotSynced = NotSynced,
                .PageSize = PageSize,
                .PageToken = PageToken,
            });
        }
        const auto listing = FetchListing(client, query);
        const auto& result = listing.Result;
        auto state = result.GetState();
        if (!PileName.empty()) {
            std::erase_if(state, [&](const auto& pile) { return pile.PileName != PileName; });
            if (state.empty()) {
                throw TMisuseException() << "Unknown pile: " << PileName;
            }
        }
        const auto& storageState = result.GetStorageState();
        if (query && !storageState) {
            throw yexception() << "Server does not support bridge storage diagnostics";
        }
        const auto* storage = query ? &*storageState : nullptr;
        const bool csv = OutputFormat == EDataFormat::Csv;
        if (OutputFormat == EDataFormat::Json) {
            PrintJson(result, state, storage, listing.IncompleteReason);
        } else if (View == NBridge::EStorageView::Piles) {
            PrintPiles(result, state, storage, Detailed, csv);
        } else {
            if (!csv) {
                if (!storage->DatabasePath.empty()) {
                    Cout << "Database: " << storage->DatabasePath << Endl;
                }
                if (!storage->StorageOwnerDatabase.empty() && storage->StorageOwnerDatabase != storage->DatabasePath) {
                    Cout << "Shared storage: " << storage->StorageOwnerDatabase << Endl;
                }
            }
            if (View == NBridge::EStorageView::Databases) {
                PrintDatabaseRows(*storage, Detailed, csv);
            } else {
                PrintGroupRows(*storage, Detailed, csv);
            }
        }

        if (storage && !storage->NextPageToken.empty()) {
            if (csv) {
                Cerr << "Next page token: " << storage->NextPageToken << Endl;
            } else if (OutputFormat != EDataFormat::Json) {
                Cout << Endl << "Next page token: " << storage->NextPageToken << Endl;
            }
        }
        if (storage && !storage->UnavailableReason.empty()) {
            Cerr << "Bridge storage diagnostics unavailable: " << storage->UnavailableReason << Endl;
            return EXIT_FAILURE;
        }
        if (!listing.IncompleteReason.empty()) {
            Cerr << "Bridge storage listing incomplete: " << listing.IncompleteReason << Endl;
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
