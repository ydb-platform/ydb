#include "split_merge.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/string/split.h>

#include <algorithm>

#include <atomic>

namespace NYdbWorkload {

namespace {

TString PayloadString(ui64 len) {
    // Incompressible-ish payload: repeated pattern, cheap to build.
    TString result;
    result.reserve(len);
    const char pattern[] = "0123456789abcdef";
    for (ui64 i = 0; i < len; ++i) {
        result.push_back(pattern[i % (sizeof(pattern) - 1)]);
    }
    return result;
}

} // namespace

void TSplitMergeWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    opts.AddLongOption('p', "path", "Table name / prefix")
        .Optional()
        .DefaultValue(TableName)
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            while (arg.SkipPrefix("/"));
            while (arg.ChopSuffix("/"));
            TableName = arg;
        });

    switch (commandType) {
    case ECommandType::Init:
        opts.AddLongOption("tables", "Number of tables to create (<path>0..<path>N-1)")
            .DefaultValue(TablesCnt).StoreResult(&TablesCnt);
        opts.AddLongOption("initial-partitions", "UNIFORM_PARTITIONS per table: single N for all tables, or comma-separated list (e.g. \"1,512\")")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                InitialPartitions.clear();
                for (const auto& part : StringSplitter(arg).Split(',')) {
                    ui64 value;
                    if (!TryFromString(part, value)) {
                        throw yexception() << "Invalid initial-partitions value: " << TString(part);
                    }
                    InitialPartitions.push_back(value);
                }
            });
        opts.AddLongOption("min-partitions", "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT: single N for all tables, or comma-separated list (e.g. \"1,64\")")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                MinPartitions.clear();
                for (const auto& part : StringSplitter(arg).Split(',')) {
                    ui64 value;
                    if (!TryFromString(part, value)) {
                        throw yexception() << "Invalid min-partitions value: " << TString(part);
                    }
                    MinPartitions.push_back(value);
                }
            });
        opts.AddLongOption("max-partitions", "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT: single N for all tables, or comma-separated list")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                MaxPartitions.clear();
                for (const auto& part : StringSplitter(arg).Split(',')) {
                    ui64 value;
                    if (!TryFromString(part, value)) {
                        throw yexception() << "Invalid max-partitions value: " << TString(part);
                    }
                    MaxPartitions.push_back(value);
                }
            });
        opts.AddLongOption("partition-size", "AUTO_PARTITIONING_PARTITION_SIZE_MB")
            .DefaultValue(PartitionSizeMb).StoreResult(&PartitionSizeMb);
        opts.AddLongOption("auto-partition", "AUTO_PARTITIONING_BY_LOAD (0 or 1): single N for all tables, or comma-separated list (e.g. \"1,0\")")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                AutoPartition.clear();
                for (const auto& part : StringSplitter(arg).Split(',')) {
                    ui64 value;
                    if (!TryFromString(part, value)) {
                        throw yexception() << "Invalid auto-partition value: " << TString(part);
                    }
                    AutoPartition.push_back(value);
                }
            });
        opts.AddLongOption("cpu-threshold", "Split-by-load CPU threshold (not a YQL table setting on this server; accepted as a no-op placeholder)")
            .DefaultValue(CpuThreshold).Handler1T<ui64>([this](ui64 arg) {
                if (arg != SplitMergeWorkloadConstants::CPU_THRESHOLD) {
                    Cerr << "warning: --cpu-threshold is a no-op placeholder on this server "
                         << "(the server-side default CPU threshold applies); the value is ignored" << Endl;
                }
                CpuThreshold = arg;
            });
        break;
    case ECommandType::Run:
        opts.AddLongOption("tables", "Number of tables (must match init; used by multi-table-split)")
            .DefaultValue(TablesCnt).StoreResult(&TablesCnt);
        opts.AddLongOption("table", "Run against an existing table (repeatable / comma-separated) instead of the init-created <path>N tables; the table must have a single Uint64 primary key and a writable payload column. The tool writes data and issues ALTERs on it")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                for (auto part : StringSplitter(arg).Split(',')) {
                    if (part.empty()) {
                        continue;
                    }
                    // Keep the leading slash: external tables are full paths and
                    // FullTablePath() must distinguish absolute from relative.
                    while (part.ChopSuffix("/"));
                    ExternalTables.push_back(TString(part));
                }
            });
        opts.AddLongOption("key-column", "Name of the single Uint64 primary-key column (external-table mode)")
            .DefaultValue(KeyColumn).StoreResult(&KeyColumn);
        opts.AddLongOption("payload-column", "Name of the payload column (external-table mode); default: payload")
            .DefaultValue(PayloadColumn).StoreResult(&PayloadColumn);
        opts.AddLongOption("payload-type", "Type of the payload column: string, utf8, uint64, int64 (external-table mode)")
            .DefaultValue(PayloadType)
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                PayloadType = arg;
                if (PayloadType != "string" && PayloadType != "utf8" && PayloadType != "uint64" && PayloadType != "int64") {
                    throw yexception() << "Invalid payload-type: " << arg;
                }
            });
        opts.AddLongOption("start-key", "First key for sequential writes (external-table mode; avoids overwriting existing rows)")
            .DefaultValue(StartKey).StoreResult(&StartKey);
        opts.AddLongOption("initial-partitions", "UNIFORM_PARTITIONS from init (positions the targeted hot key range)")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                InitialPartitions.clear();
                for (const auto& part : StringSplitter(arg).Split(',')) {
                    ui64 value;
                    if (!TryFromString(part, value)) {
                        throw yexception() << "Invalid initial-partitions value: " << TString(part);
                    }
                    InitialPartitions.push_back(value);
                }
            });
        opts.AddLongOption("min-partitions", "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT (merge-phase floor): single N or comma-separated list")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                MinPartitions.clear();
                for (const auto& part : StringSplitter(arg).Split(',')) {
                    ui64 value;
                    if (!TryFromString(part, value)) {
                        throw yexception() << "Invalid min-partitions value: " << TString(part);
                    }
                    MinPartitions.push_back(value);
                }
            });
        opts.AddLongOption("partition-size", "Split-phase partition size, MB (flap)")
            .DefaultValue(PartitionSizeMb).StoreResult(&PartitionSizeMb);
        opts.AddLongOption("rows", "Rows per write query")
            .DefaultValue(RowsCnt).StoreResult(&RowsCnt);
        opts.AddLongOption("len", "Payload string length")
            .DefaultValue(StringLen).StoreResult(&StringLen);
        opts.AddLongOption("key-distribution", "Write key distribution: sequential (default), targeted, striped")
            .DefaultValue(KeyDistribution)
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                KeyDistribution = arg;
                if (KeyDistribution != "sequential" && KeyDistribution != "targeted" && KeyDistribution != "striped") {
                    throw yexception() << "Invalid key-distribution: " << arg;
                }
            });
        opts.AddLongOption("target-shards", "Shards to concentrate traffic on (targeted mode): comma-separated indices or \"random\"")
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                TargetShards.clear();
                if (arg == "random") {
                    TargetShards.push_back(Max<ui64>()); // sentinel: pick one at random at resolve time
                    return;
                }
                for (const auto& part : StringSplitter(arg).Split(',')) {
                    ui64 value;
                    if (!TryFromString(part, value)) {
                        throw yexception() << "Invalid target-shards value: " << TString(part);
                    }
                    TargetShards.push_back(value);
                }
            });
        opts.AddLongOption("target-share", "Share of each target shard's key range receiving traffic, percent (1..100)")
            .DefaultValue(TargetSharePct)
            .Handler1T<ui64>([this](ui64 arg) {
                if (arg < 1 || arg > 100) {
                    throw yexception() << "target-share must be in 1..100, got " << arg;
                }
                TargetSharePct = arg;
            });
        opts.AddLongOption("partition-size-high", "Merge-phase partition size, MB")
            .DefaultValue(PartitionSizeHighMb).StoreResult(&PartitionSizeHighMb);
        opts.AddLongOption("concurrent-phases", "Flap: run phases concurrently (merge ALTER while writes continue)")
            .NoArgument().SetFlag(&ConcurrentPhases);
        opts.AddLongOption("phase-time", "Max duration of one phase, seconds")
            .DefaultValue(PhaseTimeSec).StoreResult(&PhaseTimeSec);
        opts.AddLongOption("cycles", "Number of flap cycles")
            .DefaultValue(CyclesCnt).StoreResult(&CyclesCnt);
        opts.AddLongOption("max-rows", "Hard stop on total rows written")
            .DefaultValue(MaxRows).StoreResult(&MaxRows);
        break;
    case ECommandType::Clean:
        opts.AddLongOption("tables", "Number of tables to remove; pass a value >= the number created to remove them all (missing tables are skipped)")
            .DefaultValue(TablesCnt).StoreResult(&TablesCnt);
        break;
    default:
        break;
    }
    Y_UNUSED(workloadType);
}

TString TSplitMergeWorkloadParams::GetWorkloadName() const {
    return "splitmerge";
}

THolder<IWorkloadQueryGenerator> TSplitMergeWorkloadParams::CreateGenerator() const {
    return MakeHolder<TSplitMergeWorkloadGenerator>(this);
}

TSplitMergeWorkloadGenerator::TSplitMergeWorkloadGenerator(const TSplitMergeWorkloadParams* params)
    : TBase(params)
    , BigString(PayloadString(Params.StringLen))
    , NextFirstKey(params->StartKey)
{
    const size_t tableCount = ExternalTableCount();
    const auto checkList = [tableCount](const TVector<ui64>& list, const char* name) {
        if (!list.empty() && list.size() != 1 && list.size() != tableCount) {
            throw yexception() << name << " list size (" << list.size()
                << ") must match the table count (" << tableCount << ") or be a single value for all tables";
        }
    };
    checkList(Params.InitialPartitions, "initial-partitions");
    checkList(Params.MinPartitions, "min-partitions");
    checkList(Params.MaxPartitions, "max-partitions");
    checkList(Params.AutoPartition, "auto-partition");

    if (Params.RowsCnt == 0) {
        throw yexception() << "--rows must be at least 1";
    }
    if (Params.MaxRows == 0) {
        // A zero budget would make TryReserveRows always fail: every write
        // mode would silently return an empty list and the run would exit
        // immediately with no explanation.
        throw yexception() << "--max-rows must be at least 1";
    }
    if (Params.PhaseTimeSec == 0) {
        throw yexception() << "--phase-time must be at least 1 second";
    }
    if (!ExternalMode()
            && (Params.KeyColumn != "k" || Params.PayloadColumn != "payload" || Params.PayloadType != "string")) {
        // The init-created schema is always (k Uint64, payload String);
        // --key-column/--payload-column/--payload-type only make sense with
        // --table (external mode) and would make every generated query fail.
        throw yexception() << "--key-column/--payload-column/--payload-type apply only to external-table mode (--table); "
            << "the init-created schema is always (k Uint64, payload String)";
    }
    if (!ExternalMode() && !Params.TargetShards.empty()) {
        // In external mode the check runs after validation, when the actual
        // partition counts are known (see ValidateExternalTables).
        const ui64 partitions = HotKeySpacePartitions();
        for (const ui64 shard : Params.TargetShards) {
            if (shard != Max<ui64>() && shard >= partitions) {
                throw yexception() << "target-shards index " << shard
                    << " is out of range: the table has " << partitions << " initial partitions";
            }
        }
    }
}

size_t TSplitMergeWorkloadGenerator::ExternalTableCount() const {
    return Params.ExternalTables.empty() ? Params.TablesCnt : Params.ExternalTables.size();
}

bool TSplitMergeWorkloadGenerator::ExternalMode() const {
    return !Params.ExternalTables.empty();
}

TString TSplitMergeWorkloadGenerator::FullTablePath(size_t tableIdx) const {
    if (ExternalMode()) {
        // External tables are given as full paths (possibly nested); the
        // database prefix is prepended only when the path is relative.
        const TString& path = Params.ExternalTables[tableIdx];
        if (path.StartsWith("/")) {
            return path;
        }
        std::stringstream ss;
        ss << Params.DbPath << "/" << path;
        return ss.str();
    }
    std::stringstream ss;
    ss << Params.DbPath << "/" << Params.TableName << tableIdx;
    return ss.str();
}

ui64 TSplitMergeWorkloadGenerator::HotKeySpacePartitions(size_t tableIdx) const {
    if (ExternalMode()) {
        // Actual partition count of the given table observed at validation
        // time; reads happen only after ExternalValidated is acquired, which
        // orders them after the claimer's writes. Empty (validation not done
        // yet) falls back to a safe minimum of 1.
        if (tableIdx < ExternalPartitionsCounts.size()) {
            return Max<ui64>(ExternalPartitionsCounts[tableIdx], 1);
        }
        return 1;
    }
    return Max<ui64>(InitialPartitionsFor(0), 1);
}

ui64 TSplitMergeWorkloadGenerator::TryReserveRows(ui64 rowsToWrite) {
    // Reserve the row budget atomically: a compare-and-swap loop guarantees
    // the total across all workers never exceeds --max-rows. The budget counts
    // attempted rows: it is reserved before the query runs and is not
    // refunded if the query fails. A partial reservation (fewer rows than
    // requested) is returned so the caller can shrink its query to the
    // remaining budget instead of overshooting --max-rows.
    ui64 written = RowsWritten.load(std::memory_order_relaxed);
    while (true) {
        if (written >= Params.MaxRows) {
            return 0;
        }
        const ui64 next = Min<ui64>(Params.MaxRows, written + rowsToWrite);
        if (RowsWritten.compare_exchange_weak(written, next, std::memory_order_relaxed)) {
            return next - written;
        }
    }
}

TQueryInfoList TSplitMergeWorkloadGenerator::ValidateExternalTables() {
    // One worker performs the validation; the others wait it out with the
    // keepalive query. A schema-validation failure latches and stops the
    // run; a transient failure releases the claim so the next GetWorkload()
    // call re-attempts the validation. A waiter that outlives the claim
    // timeout steals the claim: if the claimant's worker died (retries
    // exhausted), the claim would otherwise never be released and the run
    // would spin on keepalives against unvalidated tables forever.
    i64 myGeneration;
    {
        std::lock_guard<std::mutex> guard(ValidationMutex);
        if (ExternalValidated.load(std::memory_order_acquire)
            || ExternalValidationFailed.load(std::memory_order_acquire)) {
            return PhaseWaitQuery("ExternalValidationWait");
        }
        const i64 current = ValidationClaimGeneration.load(std::memory_order_relaxed);
        const i64 claimUs = ValidationClaimTimeUs.load(std::memory_order_relaxed);
        if (current != 0 && (claimUs == 0
            || Now().MicroSeconds() - claimUs <= ValidationClaimTimeoutSec * 1000000)) {
            return PhaseWaitQuery("ExternalValidationWait");
        }
        // Claim or steal while holding the publication lock. The previous
        // claimant may finish later, but can no longer publish its result.
        myGeneration = ValidationClaimTicket.fetch_add(1, std::memory_order_relaxed) + 1;
        ValidationClaimTimeUs.store(Now().MicroSeconds(), std::memory_order_relaxed);
        ValidationClaimGeneration.store(myGeneration, std::memory_order_release);
    }
    TQueryInfo info;
    info.QueryName = "ExternalValidation";
    info.TableOperation = [this, myGeneration](NYdb::NTable::TTableClient& tableClient) -> NYdb::TStatus {
        // Release the claim, but only if it is still ours: a stale claimant
        // (whose claim was stolen after the timeout) must not release the
        // stealer's claim.
        const auto releaseClaim = [this, myGeneration]() {
            i64 current = myGeneration;
            ValidationClaimGeneration.compare_exchange_strong(
                current, 0, std::memory_order_acq_rel);
        };
        const auto fail = [this, myGeneration](NYdb::EStatus status, const TString& msg) {
            std::lock_guard<std::mutex> guard(ValidationMutex);
            if (ValidationClaimGeneration.load(std::memory_order_relaxed) != myGeneration
                || ExternalValidated.load(std::memory_order_relaxed)) {
                return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
            }
            ExternalValidationFailed.store(true, std::memory_order_release);
            Cerr << "external-table validation failed: " << msg << Endl;
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(NYdb::NIssue::TIssue(msg));
            return NYdb::TStatus(status, std::move(issues));
        };
        auto sessionResult = tableClient.GetSession().GetValueSync();
        if (!sessionResult.IsSuccess()) {
            // Transient failure: do not latch; release the claim so the next
            // GetWorkload() call re-attempts the validation.
            releaseClaim();
            NYdb::NIssue::TIssues issues = sessionResult.GetIssues();
            return NYdb::TStatus(sessionResult.GetStatus(), std::move(issues));
        }
        TVector<ui64> partitionsCounts(Params.ExternalTables.size(), 0);
        for (size_t tableIdx = 0; tableIdx < Params.ExternalTables.size(); ++tableIdx) {
            if (ValidationClaimGeneration.load(std::memory_order_acquire) != myGeneration) {
                return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
            }
            const TString& path = Params.ExternalTables[tableIdx];
            auto descResult = sessionResult.GetSession().DescribeTable(
                FullTablePath(tableIdx),
                NYdb::NTable::TDescribeTableSettings()
                    .WithTableStatistics(true)).GetValueSync();
            if (!descResult.IsSuccess()) {
                const auto transient = [](NYdb::EStatus status) {
                    return status == NYdb::EStatus::UNAVAILABLE
                        || status == NYdb::EStatus::OVERLOADED
                        || status == NYdb::EStatus::ABORTED
                        || status == NYdb::EStatus::BAD_SESSION
                        || status == NYdb::EStatus::SESSION_BUSY
                        || status == NYdb::EStatus::UNDETERMINED
                        || status == NYdb::EStatus::TIMEOUT
                        || status == NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED
                        || status == NYdb::EStatus::CLIENT_INTERNAL_ERROR;
                };
                if (transient(descResult.GetStatus())) {
                    // Transient failure: do not latch; release the claim so
                    // the next GetWorkload() call re-attempts the validation.
                    releaseClaim();
                } else {
                    // Only the current claimant may latch a permanent failure.
                    std::lock_guard<std::mutex> guard(ValidationMutex);
                    if (ValidationClaimGeneration.load(std::memory_order_relaxed) != myGeneration
                        || ExternalValidated.load(std::memory_order_relaxed)) {
                        return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
                    }
                    ExternalValidationFailed.store(true, std::memory_order_release);
                }
                NYdb::NIssue::TIssues issues = descResult.GetIssues();
                return NYdb::TStatus(descResult.GetStatus(), std::move(issues));
            }
            const auto& desc = descResult.GetTableDescription();

            // Single Uint64 primary key.
            const auto& keyColumns = desc.GetPrimaryKeyColumns();
            if (keyColumns.size() != 1 || keyColumns.front() != Params.KeyColumn) {
                return fail(NYdb::EStatus::SCHEME_ERROR, TStringBuilder()
                    << "Table " << path << " is not compatible: expected a single primary key column '"
                    << Params.KeyColumn << "'"
                    << (keyColumns.size() == 1 ? TString(", found '") + keyColumns.front() + "'" : TString()));
            }
            const auto& columns = desc.GetTableColumns();
            // TType exposes only the proto; TTypeParser reads it. Nullable
            // columns are described as Optional(T); unwrap before comparing.
            const auto isPrimitiveOf = [](const NYdb::TType& type, NYdb::EPrimitiveType expected) {
                NYdb::TTypeParser parser(type);
                if (parser.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
                    parser.OpenOptional();
                }
                return parser.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive
                    && parser.GetPrimitive() == expected;
            };
            const auto keyIt = std::find_if(columns.begin(), columns.end(),
                [&](const auto& c) { return c.Name == Params.KeyColumn; });
            if (keyIt == columns.end() || !isPrimitiveOf(keyIt->Type, NYdb::EPrimitiveType::Uint64)) {
                return fail(NYdb::EStatus::SCHEME_ERROR, TStringBuilder()
                    << "Table " << path << ": key column '" << Params.KeyColumn << "' must be Uint64");
            }

            // Payload column exists with the expected type.
            const auto payloadIt = std::find_if(columns.begin(), columns.end(),
                [&](const auto& c) { return c.Name == Params.PayloadColumn; });
            if (payloadIt == columns.end()) {
                return fail(NYdb::EStatus::SCHEME_ERROR, TStringBuilder()
                    << "Table " << path << " has no payload column '" << Params.PayloadColumn
                    << "' (pass --payload-column)");
            }
            const NYdb::EPrimitiveType expected = Params.PayloadType == "utf8" ? NYdb::EPrimitiveType::Utf8
                : Params.PayloadType == "uint64" ? NYdb::EPrimitiveType::Uint64
                : Params.PayloadType == "int64" ? NYdb::EPrimitiveType::Int64
                : NYdb::EPrimitiveType::String;
            if (!isPrimitiveOf(payloadIt->Type, expected)) {
                return fail(NYdb::EStatus::SCHEME_ERROR, TStringBuilder()
                    << "Table " << path << ": payload column '" << Params.PayloadColumn
                    << "' type does not match --payload-type " << Params.PayloadType
                    << " (pass the matching --payload-type)");
            }

            // Record the actual partition count for hot-key-range math.
            partitionsCounts[tableIdx] = desc.GetPartitionsCount();
        }
        // Validate --target-shards against the observed partition counts
        // (targeted mode writes to table 0).
        if (!Params.TargetShards.empty() && !partitionsCounts.empty()) {
            const ui64 partitions = Max<ui64>(partitionsCounts.front(), 1);
            for (const ui64 shard : Params.TargetShards) {
                if (shard != Max<ui64>() && shard >= partitions) {
                    return fail(NYdb::EStatus::SCHEME_ERROR, TStringBuilder()
                        << "target-shards index " << shard
                        << " is out of range: table " << Params.ExternalTables.front()
                        << " has " << partitions << " partitions");
                }
            }
        }
        {
            std::lock_guard<std::mutex> guard(ValidationMutex);
            if (ValidationClaimGeneration.load(std::memory_order_relaxed) != myGeneration
                || ExternalValidationFailed.load(std::memory_order_relaxed)
                || ExternalValidated.load(std::memory_order_relaxed)) {
                return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
            }
            ExternalPartitionsCounts = std::move(partitionsCounts);
            ExternalValidated.store(true, std::memory_order_release);
        }
        for (size_t tableIdx = 0; tableIdx < Params.ExternalTables.size(); ++tableIdx) {
            Cout << "external-table\tvalidated\t" << Params.ExternalTables[tableIdx]
                 << "\tpartitions\t" << ExternalPartitionsCounts[tableIdx] << Endl;
        }
        return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
    };
    return TQueryInfoList(1, std::move(info));
}

TString TSplitMergeWorkloadGenerator::PayloadYqlType() const {
    if (Params.PayloadType == "utf8") {
        return "Utf8";
    }
    if (Params.PayloadType == "uint64") {
        return "Uint64";
    }
    if (Params.PayloadType == "int64") {
        return "Int64";
    }
    return "String";
}

void TSplitMergeWorkloadGenerator::AddPayloadParam(NYdb::TParamsBuilder& builder, const TString& name) const {
    if (Params.PayloadType == "utf8") {
        builder.AddParam(name).Utf8(BigString).Build();
    } else if (Params.PayloadType == "uint64") {
        builder.AddParam(name).Uint64(42).Build();
    } else if (Params.PayloadType == "int64") {
        builder.AddParam(name).Int64(42).Build();
    } else {
        builder.AddParam(name).String(BigString).Build();
    }
}

ui64 TSplitMergeWorkloadGenerator::InitialPartitionsFor(size_t tableIdx) const {
    if (Params.InitialPartitions.empty()) {
        return SplitMergeWorkloadConstants::INITIAL_PARTITIONS;
    }
    if (Params.InitialPartitions.size() == 1) {
        // A single value applies to all tables.
        return Params.InitialPartitions.front();
    }
    return Params.InitialPartitions[tableIdx];
}

ui64 TSplitMergeWorkloadGenerator::MinPartitionsFor(size_t tableIdx) const {
    if (Params.MinPartitions.empty()) {
        return SplitMergeWorkloadConstants::MIN_PARTITIONS;
    }
    if (Params.MinPartitions.size() == 1) {
        return Params.MinPartitions.front();
    }
    return Params.MinPartitions[tableIdx];
}

ui64 TSplitMergeWorkloadGenerator::MaxPartitionsFor(size_t tableIdx) const {
    if (Params.MaxPartitions.empty()) {
        return SplitMergeWorkloadConstants::MAX_PARTITIONS;
    }
    if (Params.MaxPartitions.size() == 1) {
        return Params.MaxPartitions.front();
    }
    return Params.MaxPartitions[tableIdx];
}

ui64 TSplitMergeWorkloadGenerator::AutoPartitionFor(size_t tableIdx) const {
    if (Params.AutoPartition.empty()) {
        return SplitMergeWorkloadConstants::AUTO_PARTITION;
    }
    if (Params.AutoPartition.size() == 1) {
        return Params.AutoPartition.front();
    }
    return Params.AutoPartition[tableIdx];
}

std::string TSplitMergeWorkloadGenerator::GetDDLQueries() const {
    std::stringstream ss;
    for (size_t i = 0; i < Params.TablesCnt; ++i) {
        ss << "--!syntax_v1\n";
        ss << "CREATE TABLE `" << FullTablePath(i) << "` ("
           << "k Uint64, "
           << "payload String, "
           << "PRIMARY KEY (k)"
           << ") WITH ("
           << "STORE = ROW, ";
        if (AutoPartitionFor(i)) {
            ss << "AUTO_PARTITIONING_BY_LOAD = ENABLED, ";
        }
        ss << "UNIFORM_PARTITIONS = " << InitialPartitionsFor(i) << ", "
            << "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << MinPartitionsFor(i) << ", "
            << "AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << Max(MinPartitionsFor(i), MaxPartitionsFor(i)) << ", "
           << "AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.PartitionSizeMb;
        // Note: the split-by-load CPU threshold is not a YQL table setting on
        // this server; the server-side default applies. --cpu-threshold is kept
        // as a no-op placeholder for forward compatibility.
        ss << ");\n";
    }
    return ss.str();
}

TQueryInfoList TSplitMergeWorkloadGenerator::GetInitialData() {
    return TQueryInfoList();
}

TVector<std::string> TSplitMergeWorkloadGenerator::GetCleanPaths() const {
    // Relative to the database root: the CLI driver prepends config.Database.
    TVector<std::string> result;
    for (size_t i = 0; i < Params.TablesCnt; ++i) {
        result.push_back(Params.TableName + std::to_string(i));
    }
    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TSplitMergeWorkloadGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::SplitBySize), "split-by-size", "Grow partitions past the size threshold via fresh-row writes (key distribution follows --key-distribution; sequential by default)");
    result.emplace_back(static_cast<int>(EType::SplitByLoad), "split-by-load", "Hot-spot point UPSERT traffic on a narrow key slice of table 0 (by-load splits; requires init with --auto-partition 1)");
    result.emplace_back(static_cast<int>(EType::MergeBySize), "merge-by-size", "ALTER partition size up -> merge cascade; poll until drained");
    result.emplace_back(static_cast<int>(EType::MergeByLoad), "merge-by-load", "Watch the by-load merge drain after hot traffic stops; run split-by-load first to create the load, then start this mode (requires init with --auto-partition 1)");
    result.emplace_back(static_cast<int>(EType::SplitBurst), "split-burst", "All shards of table 0 pushed past the size threshold simultaneously");
    result.emplace_back(static_cast<int>(EType::MergeBurst), "merge-burst", "Merge cascade with the min-partitions floor dropped to 1, permitting a collapse down to a single partition (unlike merge-by-size, which respects --min-partitions); the actual end count depends on the data size vs the partition-size threshold");
    result.emplace_back(static_cast<int>(EType::MultiTableSplit), "multi-table-split", "Many tables split in parallel, competing for slots");
    result.emplace_back(static_cast<int>(EType::SmallTableStarvation), "small-table-starvation", "Demanding writes on table 0; pair with an init like --tables 2 --initial-partitions 1,512 --min-partitions 1,512 so the big table's split/merge report flood is driven by its own churn");
    result.emplace_back(static_cast<int>(EType::SplitVsMergeRace), "split-vs-merge-race", "Merge wave racing concurrent split demand");
    result.emplace_back(static_cast<int>(EType::Flap), "flap", "Alternate grow/merge/split phases on one table");
    result.emplace_back(static_cast<int>(EType::Status), "status", "Poll and print partition count, interval width and row distribution per table (all init/external tables), once per second");
    return result;
}

TQueryInfoList TSplitMergeWorkloadGenerator::GetWorkload(int type) {
    // External-table mode: validate the schema once before any traffic or
    // ALTERs are issued; every worker waits until validation completes.
    // A failed validation stops the run: an empty list makes the run driver
    // exit the worker loop instead of hammering the server with keepalives.
    if (ExternalMode()) {
        if (ExternalValidationFailed.load(std::memory_order_acquire)) {
            return TQueryInfoList();
        }
        if (!ExternalValidated.load(std::memory_order_acquire)) {
            return ValidateExternalTables();
        }
    }
    switch (static_cast<EType>(type)) {
        case EType::SplitBySize:
            return WriteStep();
        case EType::SplitByLoad:
            WarnIfByLoadDisabled();
            return SplitByLoadStep();
        case EType::MergeBySize:
            // Issue the merge ALTER once, then poll the cascade until drained
            // without re-issuing it (repeated ALTERs only add schemeshard
            // load). Atomic exchange: exactly one worker issues it even though
            // the run driver calls GetWorkload() from multiple threads. The
            // ALTER's table operation records the pre-ALTER partition count
            // so the done line's "initial" is the true pre-merge count.
            if (!MergeAlterIssued.exchange(true)) {
                // The cascade must not be polled until this ALTER has actually
                // executed: the driver's rate limiter may delay it, and a poll
                // in that window could report a "done" plateau for a cascade
                // that has not started. MergeAlterExpected arms that gate;
                // the ALTER's table operation sets MergeAlterDone on success.
                MergeAlterExpected.store(true, std::memory_order_release);
                return AlterPartitionSize(Params.PartitionSizeHighMb, MinPartitionsFor(0), 0, /*recordInitialPartitions=*/true);
            }
            return MergeCascadePollStep("merge-by-size");
        case EType::MergeBurst:
            // Unlike merge-by-size, drop the min-partitions floor to 1 so the
            // cascade is permitted to merge all the way down to a single
            // partition (the actual end count still depends on the data size
            // vs the partition-size threshold). Same once-only ALTER + poll
            // pattern.
            if (!MergeAlterIssued.exchange(true)) {
                MergeAlterExpected.store(true, std::memory_order_release);
                return AlterPartitionSize(Params.PartitionSizeHighMb, 1, 0, /*recordInitialPartitions=*/true);
            }
            return MergeCascadePollStep("merge-burst");
        case EType::MergeByLoad:
            WarnIfByLoadDisabled();
            return MergeCascadePollStep("merge-by-load");
        case EType::SplitBurst:
            // All shards must be pushed past the size threshold simultaneously,
            // so this mode always uses the striped generator regardless of the
            // global --key-distribution default (sequential).
            return UpsertStriped();
        case EType::MultiTableSplit: {
            // Round-robin fresh-row writes across all tables so they compete
            // for split slots in parallel.
            const size_t tableCount = ExternalTableCount();
            const size_t tableIdx = tableCount > 1
                ? TableRotator.fetch_add(1, std::memory_order_relaxed) % tableCount
                : 0;
            return UpsertSequential(tableIdx);
        }
        case EType::SmallTableStarvation:
            // Small table demand; the big table stays big and dormant because
            // init pinned its MIN_PARTITIONS (per-table --min-partitions list).
            return WriteStep();
        case EType::SplitVsMergeRace:
            // The race needs both sides: one worker issues the merge ALTER once
            // (raising the partition-size threshold so a merge wave starts),
            // while all other workers keep writing fresh rows to drive split
            // demand concurrently.
            if (!MergeAlterIssued.exchange(true)) {
                // No recordInitialPartitions here: this mode never runs
                // MergeCascadePollStep, so a recorded initial count would be
                // write-only.
                return AlterPartitionSize(Params.PartitionSizeHighMb, MinPartitionsFor(0), 0, /*recordInitialPartitions=*/false, "split-vs-merge-race");
            }
            return WriteStep();
        case EType::Flap:
            return FlapStep();
        case EType::Status:
            return StatusPoll();
        default:
            return TQueryInfoList();
    }
}

void TSplitMergeWorkloadGenerator::WarnIfByLoadDisabled() {
    // By-load splits/merges only fire with AUTO_PARTITIONING_BY_LOAD = ENABLED,
    // which the init defaults leave disabled (--auto-partition 1 is required).
    // Warn once instead of silently generating traffic that tests nothing.
    // External mode is skipped: the table's setting is only knowable from
    // its description, which the validation step does not currently inspect.
    if (ExternalMode() || AutoPartitionFor(0)) {
        return;
    }
    if (!ByLoadPrereqWarned.exchange(true, std::memory_order_relaxed)) {
        Cerr << "warning: table " << FullTablePath(0)
             << " was created without AUTO_PARTITIONING_BY_LOAD (init with --auto-partition 1); "
             << "by-load splits/merges will not fire in this run" << Endl;
    }
}

TQueryInfoList TSplitMergeWorkloadGenerator::WriteStep() {
    // Route the write modes through --key-distribution.
    if (Params.KeyDistribution == "striped") {
        return UpsertStriped();
    }
    if (Params.KeyDistribution == "targeted") {
        return UpdateHotKeys();
    }
    return UpsertSequential();
}

TQueryInfoList TSplitMergeWorkloadGenerator::UpsertSequential(size_t tableIdx) {
    // Reserve first, then emit exactly the reserved number of rows: a partial
    // reservation (budget nearly exhausted) shrinks the query instead of
    // overshooting --max-rows.
    const ui64 rowsCnt = TryReserveRows(Params.RowsCnt);
    if (rowsCnt == 0) {
        return TQueryInfoList();
    }

    NYdb::TParamsBuilder paramsBuilder;
    std::stringstream ss;
    ss << "--!syntax_v1\n";
    const ui64 firstKey = NextFirstKey.fetch_add(rowsCnt, std::memory_order_relaxed);
    for (ui64 row = 0; row < rowsCnt; ++row) {
        const TString cname = "$k" + std::to_string(row);
        const TString pname = "$p" + std::to_string(row);
        ss << "DECLARE " << cname << " AS Uint64;\n";
        ss << "DECLARE " << pname << " AS " << PayloadYqlType() << ";\n";
        paramsBuilder.AddParam(cname).Uint64(firstKey + row).Build();
        AddPayloadParam(paramsBuilder, pname);
    }
    ss << "UPSERT INTO `" << FullTablePath(tableIdx) << "` (" << Params.KeyColumn << ", " << Params.PayloadColumn << ") VALUES ";
    for (ui64 row = 0; row < rowsCnt; ++row) {
        ss << "($k" << row << ", $p" << row << ")";
        if (row + 1 < rowsCnt) {
            ss << ", ";
        }
    }

    return TQueryInfoList(1, TQueryInfo(ss.str(), paramsBuilder.Build()));
}

TQueryInfoList TSplitMergeWorkloadGenerator::UpsertStriped() {
    // Round-robin across the whole key space: each query takes a block from a
    // different region so all shards grow simultaneously. Reserve first, then
    // emit exactly the reserved number of rows (see UpsertSequential).
    const ui64 rowsCnt = TryReserveRows(Params.RowsCnt);
    if (rowsCnt == 0) {
        return TQueryInfoList();
    }
    NYdb::TParamsBuilder paramsBuilder;
    std::stringstream ss;
    ss << "--!syntax_v1\n";
    const ui64 block = NextFirstKey.fetch_add(rowsCnt, std::memory_order_relaxed);
    // Spread the block's keys across the whole key space with a large stride:
    // row i lands at block + i*stride, so each query touches every shard
    // ~RowsCnt/partitions times. The stride must NOT be divided by the
    // partition count: that would confine each query to the first 1/P of the
    // key space (shard 0 only). Keys stay fresh across queries: block advances
    // by the reserved rowsCnt per query, which is negligible against the stride.
    const ui64 stride = Max<ui64>() / (rowsCnt + 1);
    for (ui64 row = 0; row < rowsCnt; ++row) {
        const TString cname = "$k" + std::to_string(row);
        const TString pname = "$p" + std::to_string(row);
        ss << "DECLARE " << cname << " AS Uint64;\n";
        ss << "DECLARE " << pname << " AS " << PayloadYqlType() << ";\n";
        paramsBuilder.AddParam(cname).Uint64(block + row * stride).Build();
        AddPayloadParam(paramsBuilder, pname);
    }
    ss << "UPSERT INTO `" << FullTablePath(0) << "` (" << Params.KeyColumn << ", " << Params.PayloadColumn << ") VALUES ";
    for (ui64 row = 0; row < rowsCnt; ++row) {
        ss << "($k" << row << ", $p" << row << ")";
        if (row + 1 < rowsCnt) {
            ss << ", ";
        }
    }

    return TQueryInfoList(1, TQueryInfo(ss.str(), paramsBuilder.Build()));
}


TQueryInfoList TSplitMergeWorkloadGenerator::UpdateHotKeys() {
    // In-place rewrites over a fixed hot key set: CPU grows, DataSize stays
    // flat. UPSERT (not UPDATE) so the loop is idempotent and self-healing:
    // if the initial seed is lost, the first iteration re-creates the rows.
    // --max-rows is a hard stop on total rows written: reserve the row budget
    // atomically so concurrent workers cannot overrun it.
    NYdb::TParamsBuilder paramsBuilder;
    std::stringstream ss;
    ss << "--!syntax_v1\n";
    // Resolve the target shard set: empty -> shard 0; the "random" sentinel
    // (Max<ui64>) -> a uniformly random shard of the initial split. Multiple
    // listed shards are rotated round-robin across successive queries.
    TVector<ui64> shards = Params.TargetShards;
    if (shards.empty()) {
        shards.push_back(0);
    }
    const ui64 initialPartitions = HotKeySpacePartitions();
    for (auto& shard : shards) {
        if (shard == Max<ui64>()) {
            shard = RandomNumber(initialPartitions);
        }
    }
    const ui64 shardIdx = shards[TargetShardRotator.fetch_add(1, std::memory_order_relaxed) % shards.size()];
    const ui64 shardStride = Max<ui64>() / initialPartitions;
    // The hot key set is spread across --target-share percent of the target
    // shard's key range (at least one key): row i lands at
    // base + keyStride * i / hotKeyCount, so the hotKeyCount keys cover the whole
    // share range instead of clustering at its start. Compute the percentage
    // without a ui64 overflow: a direct shardStride * pct multiplication
    // overflows for any share above ~4%, so split into whole and remainder
    // parts instead. The keyStride * row product uses __int128 because both
    // operands can be near the ui64 limit.
    const ui64 pct = Params.TargetSharePct;
    const ui64 keyStride = Max<ui64>(1,
        (shardStride / 100) * pct + ((shardStride % 100) * pct) / 100);
    // Clamp the hot key count to the share range: when keyStride < RowsCnt
    // (tiny --target-share with large --rows), spreading RowsCnt keys over
    // keyStride slots would emit duplicate primary keys in one UPSERT, which
    // the server may reject. Fewer distinct keys still concentrate the load.
    // The reservation may shrink it further (budget nearly exhausted); emit
    // exactly the reserved count so --max-rows stays a hard stop.
    const ui64 hotKeyCount = TryReserveRows(Min(Params.RowsCnt, keyStride));
    if (hotKeyCount == 0) {
        return TQueryInfoList();
    }
    const ui64 base = shardIdx * shardStride;
    for (ui64 row = 0; row < hotKeyCount; ++row) {
        const TString cname = "$k" + std::to_string(row);
        const TString pname = "$p" + std::to_string(row);
        ss << "DECLARE " << cname << " AS Uint64;\n";
        ss << "DECLARE " << pname << " AS " << PayloadYqlType() << ";\n";
        const ui64 key = base + static_cast<ui64>(
            (static_cast<unsigned __int128>(keyStride) * row) / hotKeyCount);
        paramsBuilder.AddParam(cname).Uint64(key).Build();
        AddPayloadParam(paramsBuilder, pname);
    }
    ss << "UPSERT INTO `" << FullTablePath(0) << "` (" << Params.KeyColumn << ", " << Params.PayloadColumn << ") VALUES ";
    for (ui64 row = 0; row < hotKeyCount; ++row) {
        ss << "($k" << row << ", $p" << row << ")";
        if (row + 1 < hotKeyCount) {
            ss << ", ";
        }
    }

    return TQueryInfoList(1, TQueryInfo(ss.str(), paramsBuilder.Build()));
}

TQueryInfoList TSplitMergeWorkloadGenerator::AlterPartitionSize(ui64 sizeMb, ui64 minPartitions, size_t tableIdx, bool recordInitialPartitions, const TString& failureLabel) {
    auto alterTable = NYdb::NTable::TAlterTableSettings()
        .AlterPartitioningSettings(NYdb::NTable::TPartitioningSettingsBuilder()
            .SetPartitionSizeMb(sizeMb)
            .SetMinPartitionsCount(minPartitions)
            .Build());
    TQueryInfo info;
    info.QueryName = "AlterPartitionSize";
    info.TablePath = FullTablePath(tableIdx);
    if (!recordInitialPartitions) {
        if (failureLabel.empty()) {
            info.AlterTable = alterTable;
            return TQueryInfoList(1, std::move(info));
        }
        // One-shot ALTER with observable failure: wrap the plain AlterTable in
        // a table operation so a failure can print a mode-labeled line (the
        // driver only reports errors under --verbose).
        info.TableOperation = [alterTable, path = FullTablePath(tableIdx), failureLabel](
            NYdb::NTable::TTableClient& tableClient) -> NYdb::TStatus {
            auto sessionResult = tableClient.GetSession().GetValueSync();
            if (!sessionResult.IsSuccess()) {
                NYdb::NIssue::TIssues issues = sessionResult.GetIssues();
                Cout << failureLabel << "\terror\t" << path << "\t"
                     << sessionResult.GetStatus() << "\t" << issues.ToString() << Endl;
                return NYdb::TStatus(sessionResult.GetStatus(), std::move(issues));
            }
            auto alterResult = sessionResult.GetSession().AlterTable(path, alterTable).GetValueSync();
            if (!alterResult.IsSuccess()) {
                Cout << failureLabel << "\terror\t" << path << "\t"
                     << alterResult.GetStatus() << "\t" << alterResult.GetIssues().ToString() << Endl;
            }
            return alterResult;
        };
        return TQueryInfoList(1, std::move(info));
    }
    // Describe-then-alter in one table operation: the pre-ALTER partition
    // count is recorded as the merge cascade's "initial" value before any
    // merge can fire (a plain AlterTable cannot do the describe half, and
    // recording on the first poll would already miss early merges).
    info.TableOperation = [this, alterTable, path = FullTablePath(tableIdx)](
        NYdb::NTable::TTableClient& tableClient) -> NYdb::TStatus {
        auto sessionResult = tableClient.GetSession().GetValueSync();
        if (!sessionResult.IsSuccess()) {
            NYdb::NIssue::TIssues issues = sessionResult.GetIssues();
            return NYdb::TStatus(sessionResult.GetStatus(), std::move(issues));
        }
        auto descResult = sessionResult.GetSession().DescribeTable(
            path, NYdb::NTable::TDescribeTableSettings()
                .WithTableStatistics(true)).GetValueSync();
        if (!descResult.IsSuccess()) {
            NYdb::NIssue::TIssues issues = descResult.GetIssues();
            return NYdb::TStatus(descResult.GetStatus(), std::move(issues));
        }
        ui64 initialExpected = 0;
        MergeCascadeInitialPartitions.compare_exchange_strong(
            initialExpected,
            descResult.GetTableDescription().GetPartitionsCount(),
            std::memory_order_relaxed, std::memory_order_relaxed);
        auto alterResult = sessionResult.GetSession().AlterTable(path, alterTable).GetValueSync();
        // Release the cascade-polling gate only on success: a failed ALTER
        // leaves the gate closed, so the run keeps waiting instead of polling
        // a cascade that was never started. Report the failure on the mode's
        // output line so a permanently closed gate is observable (the run
        // driver only prints errors under --verbose).
        if (!alterResult.IsSuccess()) {
            Cout << "merge-alter\terror\t" << path << "\t"
                 << alterResult.GetStatus() << "\t" << alterResult.GetIssues().ToString() << Endl;
        } else {
            MergeAlterDone.store(true, std::memory_order_release);
        }
        return alterResult;
    };
    return TQueryInfoList(1, std::move(info));
}

TQueryInfoList TSplitMergeWorkloadGenerator::StatusPoll() {
    TQueryInfo info;
    info.QueryName = "StatusPoll";
    info.TableOperation = [this](NYdb::NTable::TTableClient& tableClient) -> NYdb::TStatus {
        // Status is a poll loop, not a throughput workload: at most one poll
        // per second across all worker threads. A lease CAS on the last-poll
        // timestamp picks a single winner per tick without thread-local or
        // function-local static state (which would leak across generator
        // instances); the losers return a no-op success, which the run driver
        // still counts as a keepalive Tx.
        const i64 nowUs = Now().MicroSeconds();
        i64 lastUs = StatusLastPollUs.load(std::memory_order_relaxed);
        if (lastUs != 0 && nowUs - lastUs < 1000000) {
            return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
        }
        if (!StatusLastPollUs.compare_exchange_strong(lastUs, nowUs, std::memory_order_relaxed)) {
            return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
        }
        auto sessionResult = tableClient.GetSession().GetValueSync();
        if (!sessionResult.IsSuccess()) {
            // Roll the lease back so a failing poll does not silence the
            // timeline for a second: the next GetWorkload() call re-polls.
            StatusLastPollUs.store(0, std::memory_order_relaxed);
            NYdb::NIssue::TIssues issues = sessionResult.GetIssues();
            return NYdb::TStatus(sessionResult.GetStatus(), std::move(issues));
        }
        for (size_t tableIdx = 0; tableIdx < ExternalTableCount(); ++tableIdx) {
            const TString path = FullTablePath(tableIdx);
            auto descResult = sessionResult.GetSession().DescribeTable(
                path, NYdb::NTable::TDescribeTableSettings()
                    .WithTableStatistics(true)
                    .WithPartitionStatistics(true)
                    .WithKeyShardBoundary(true)).GetValueSync();
            if (!descResult.IsSuccess()) {
                // Report the failed table and keep polling the rest: a single
                // failed DescribeTable must not truncate the whole timeline.
                Cout << "status\terror\t" << path << "\t"
                     << descResult.GetStatus() << "\t" << descResult.GetIssues().ToString() << Endl;
                continue;
            }
            const auto& desc = descResult.GetTableDescription();
            Cout << "partitions\t" << path << "\t" << desc.GetPartitionsCount();

            // Interval distribution: how the key space is carved up. For a Uint64
            // primary key the interval width is the difference of boundary keys.
            // Narrow intervals clustered around the hot range are the expected
            // signature of by-load splits; wide uniform intervals — of merges.
            const auto& ranges = desc.GetKeyRanges();
            const auto& stats = desc.GetPartitionStats();
            if (!ranges.empty()) {
                // The last partition has no upper key bound; its width is unbounded
                // and is printed as "uint64_max" instead of a huge raw number.
                const bool lastUnbounded = !ranges.back().To();
                TVector<ui64> widths;
                widths.reserve(ranges.size());
                for (const auto& range : ranges) {
                    // TValue exposes only the proto; TValueParser reads it.
                    auto boundToKey = [](const std::optional<NYdb::NTable::TKeyBound>& bound) -> ui64 {
                        if (!bound) {
                            return 0;
                        }
                        try {
                            NYdb::TValueParser parser(bound->GetValue());
                            // The bound value may be wrapped (Tuple for composite
                            // keys, Optional); unwrap down to the Uint64 primitive.
                            for (size_t depth = 0; depth < 4; ++depth) {
                                const auto kind = parser.GetKind();
                                if (kind == NYdb::TTypeParser::ETypeKind::Tuple) {
                                    parser.OpenTuple();
                                    if (!parser.TryNextElement()) {
                                        return 0;
                                    }
                                } else if (kind == NYdb::TTypeParser::ETypeKind::Optional) {
                                    parser.OpenOptional();
                                } else {
                                    break;
                                }
                            }
                            // OpenOptional() above already unwrapped the Optional:
                            // the parser now sits on the primitive, so read it
                            // directly. GetOptionalUint64() would try to open
                            // another Optional layer and throw on the primitive.
                            return parser.GetUint64();
                        } catch (...) {
                            // Non-numeric key type: no width to report.
                            return 0;
                        }
                    };
                    const ui64 from = boundToKey(range.From());
                    const ui64 to = range.To()
                        ? std::max<ui64>(boundToKey(range.To()), 1)
                        : Max<ui64>();
                    widths.push_back(to > from ? to - from : 0);
                }
                std::sort(widths.begin(), widths.end());
                Cout << "\twidths\tmin=" << widths.front()
                     << "\tmed=" << widths[widths.size() / 2]
                     << "\tmax=" << (lastUnbounded ? TString("uint64_max") : ToString(widths.back()));
                if (!stats.empty()) {
                    TVector<ui64> rows;
                    rows.reserve(stats.size());
                    for (const auto& stat : stats) {
                        rows.push_back(stat.Rows);
                    }
                    std::sort(rows.begin(), rows.end());
                    Cout << "\trows\tmin=" << rows.front()
                         << "\tmed=" << rows[rows.size() / 2]
                         << "\tmax=" << rows.back();
                }
            }
            Cout << Endl;
        }
        return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
    };
    return TQueryInfoList(1, std::move(info));
}

TQueryInfoList TSplitMergeWorkloadGenerator::SplitByLoadStep() {
    // Idempotent UPSERTs over the fixed hot key set: CPU grows, DataSize stays
    // flat. The first iteration doubles as the seed (self-healing), so no
    // separate seed step is needed — and no seed/worker race is possible.
    return UpdateHotKeys();
}

TQueryInfoList TSplitMergeWorkloadGenerator::MergeCascadePollStep(const TString& modeName) {
    // Shared "poll the merge cascade until drained" step for merge-by-size,
    // merge-burst and merge-by-load. Instead of a blind keepalive wait, the
    // poller thread polls the target table's partition count once per second
    // (the same lease pattern as StatusPoll) and prints a timeline line so the
    // merge cascade draining is observable and verifiable. Non-poller threads
    // keep the no-op keepalive query so the run driver loop stays alive.
    //
    // The wait is not a throughput workload: at most one poll per second
    // across all worker threads; the losers fall back to the keepalive query
    // below. The lease CAS on the member last-poll timestamp picks a single
    // winner per tick without thread-local or function-local static state
    // (which would leak across generator instances).
    // For the ALTER-driven modes (merge-by-size, merge-burst) do not poll
    // until the one-shot ALTER has actually executed: the driver's rate
    // limiter may delay it, and polling in that window could report a "done"
    // plateau for a cascade that has not started. merge-by-load sets no
    // expectation and polls immediately.
    if (MergeAlterExpected.load(std::memory_order_acquire)
        && !MergeAlterDone.load(std::memory_order_acquire)) {
        return PhaseWaitQuery("MergeCascadeWait");
    }
    const i64 nowUs = Now().MicroSeconds();
    i64 lastUs = MergeCascadeLastPollUs.load(std::memory_order_relaxed);
    if (lastUs != 0 && nowUs - lastUs < 1000000) {
        // Keepalive for non-poller threads (and rate-limited poller ticks):
        // the run driver loop expects a query to execute; a no-op SELECT 1
        // keeps it alive without adding server load.
        return PhaseWaitQuery("MergeCascadeWait");
    }
    if (!MergeCascadeLastPollUs.compare_exchange_strong(lastUs, nowUs, std::memory_order_relaxed)) {
        return PhaseWaitQuery("MergeCascadeWait");
    }

    TQueryInfo info;
    info.QueryName = "MergeCascadePoll";
    info.TableOperation = [this, modeName, path = FullTablePath(0)](NYdb::NTable::TTableClient& tableClient) -> NYdb::TStatus {
        const auto pollFailed = [this, &modeName, &path](const NYdb::TStatus& status) -> NYdb::TStatus {
            // Roll the lease back so a failing poll does not silence the
            // timeline for a second, and report the gap: without this a
            // persistent failure would look like a frozen cascade.
            MergeCascadeLastPollUs.store(0, std::memory_order_relaxed);
            Cout << modeName << "\terror\t" << path << "\t"
                 << status.GetStatus() << "\t" << status.GetIssues().ToString() << Endl;
            return status;
        };
        auto sessionResult = tableClient.GetSession().GetValueSync();
        if (!sessionResult.IsSuccess()) {
            return pollFailed(sessionResult);
        }
        auto descResult = sessionResult.GetSession().DescribeTable(
            path, NYdb::NTable::TDescribeTableSettings()
                .WithTableStatistics(true)).GetValueSync();
        if (!descResult.IsSuccess()) {
            return pollFailed(descResult);
        }
        const ui64 partitions = descResult.GetTableDescription().GetPartitionsCount();

        // Fallback initial-count recording for modes that issue no ALTER
        // (merge-by-load): the first poll's count stands in for the pre-cascade
        // count. For the ALTER modes this is a no-op: the ALTER's table
        // operation has already recorded the true pre-merge count.
        ui64 initialExpected = 0;
        MergeCascadeInitialPartitions.compare_exchange_strong(
            initialExpected, partitions,
            std::memory_order_relaxed, std::memory_order_relaxed);

        // Track stability: a run of consecutive polls with an unchanged count
        // means the merge cascade has drained (no further merges are firing).
        const ui64 previous = MergeCascadeLastPartitions.exchange(partitions, std::memory_order_relaxed);
        if (previous == partitions) {
            MergeCascadeStablePolls.fetch_add(1, std::memory_order_relaxed);
        } else {
            MergeCascadeStablePolls.store(0, std::memory_order_relaxed);
        }

        Cout << modeName << "\tpartitions\t" << path << "\t" << partitions << Endl;

        // Summary once the count has been stable for several consecutive polls:
        // a verifiable "cascade drained" signal. The detector re-arms: a new
        // "done" line is printed for every plateau whose count differs from the
        // previously reported one (a delayed split/merge tail after the first
        // plateau is still reported), while a plateau at the same count as the
        // last report is not printed twice. The atomic exchange guards against
        // a double print for the same value. "initial" stays the run's true
        // pre-cascade count (recorded by the ALTER's table operation, or the
        // first poll for the no-ALTER modes).
        constexpr ui64 stablePollsNeeded = 3;
        if (MergeCascadeStablePolls.load(std::memory_order_relaxed) >= stablePollsNeeded
            && MergeCascadeLastDonePartitions.exchange(partitions, std::memory_order_relaxed) != partitions) {
            Cout << modeName << "\tdone\t" << path << "\t" << partitions
                 << "\tinitial\t" << MergeCascadeInitialPartitions.load(std::memory_order_relaxed) << Endl;
        }
        return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
    };
    return TQueryInfoList(1, std::move(info));
}

TQueryInfoList TSplitMergeWorkloadGenerator::FlapStep() {
    // The whole phase machine is mutex-guarded: the run driver calls
    // GetWorkload() from multiple worker threads, and both the phase
    // transition and the one-ALTER-per-phase flag are check-and-set.
    TQueryInfoList result;
    {
        std::lock_guard<std::mutex> guard(FlapMutex);
        const auto now = Now();
        if (FlapPhaseStart == TInstant()) {
            FlapPhaseStart = now;
        }

        // Advance the phase machine on --phase-time expiry; stop after --cycles.
        if (FlapCycle >= Params.CyclesCnt) {
            // All cycles complete: an empty list makes the run driver exit
            // the worker loop cleanly (the same mechanism used for max-rows
            // exhaustion) instead of issuing keepalives until the user
            // stops the run.
            return TQueryInfoList();
        }
        if (now - FlapPhaseStart >= TDuration::Seconds(Params.PhaseTimeSec)) {
            switch (FlapPhase) {
                case EFlapPhase::Grow:
                    FlapPhase = EFlapPhase::Merge;
                    break;
                case EFlapPhase::Merge:
                    FlapPhase = EFlapPhase::Split;
                    break;
                case EFlapPhase::Split:
                    ++FlapCycle;
                    FlapPhase = EFlapPhase::Grow;
                    break;
            }
            FlapPhaseStart = now;
            FlapAlterIssued = false;
        }

        switch (FlapPhase) {
            case EFlapPhase::Grow:
                if (Params.ConcurrentPhases) {
                    // Concurrent variant: issue the merge ALTER once per run
                    // (first Grow phase only), then keep writing so the merge
                    // cascade races the ongoing split demand. Later cycles
                    // must not re-issue it: their Merge/Split phases set their
                    // own partition sizes, and a stray high-size ALTER in a
                    // later Grow phase would fight the Split phase's setting.
                    // FlapAlterIssued alone guarantees once-per-run: the
                    // previous RowsWritten == 0 condition raced with
                    // concurrent writers (another worker could reserve rows
                    // before this one evaluated the guard, silently skipping
                    // the ALTER and degrading the mode to a plain grow phase).
                    if (FlapCycle == 0 && !FlapAlterIssued) {
                        FlapAlterIssued = true;
                        return AlterPartitionSize(Params.PartitionSizeHighMb, MinPartitionsFor(0), 0, /*recordInitialPartitions=*/false, "flap");
                    }
                }
                result = UpsertSequential();
                break;
            case EFlapPhase::Merge:
                if (!FlapAlterIssued) {
                    FlapAlterIssued = true;
                    // No recordInitialPartitions: flap never runs
                    // MergeCascadePollStep, so a recorded initial count
                    // would be write-only.
                    return AlterPartitionSize(Params.PartitionSizeHighMb, MinPartitionsFor(0), 0, /*recordInitialPartitions=*/false, "flap");
                }
                break;
            case EFlapPhase::Split:
                if (!FlapAlterIssued) {
                    FlapAlterIssued = true;
                    return AlterPartitionSize(Params.PartitionSizeMb, MinPartitionsFor(0), 0, /*recordInitialPartitions=*/false, "flap");
                }
                break;
        }
    }
    // Wait out the remainder of the ALTER phase without re-issuing it.
    return result.empty() ? PhaseWaitQuery("FlapPhaseWait") : result;
}

TQueryInfoList TSplitMergeWorkloadGenerator::PhaseWaitQuery(const TString& name) {
    // No-op keepalive. Note: the run driver counts these as successful Txs,
    // so wait-heavy modes (merge/status waits) show inflated Txs/sec.
    TQueryInfo info;
    info.QueryName = name;
    info.Query = "--!syntax_v1\nSELECT 1;";
    return TQueryInfoList(1, std::move(info));
}

} // namespace NYdbWorkload
