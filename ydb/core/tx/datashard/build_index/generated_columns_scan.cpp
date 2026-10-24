#include "generated_columns_scan.h"
#include "build_scan_upload.h"

#include <ydb/core/engine/generated_columns/row_program.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NDataShard {
namespace {

bool MatchesType(const TUserTable::TUserColumn& column, const Ydb::Type& type) {
    const auto& physical = type.has_optional_type() ? type.optional_type().item() : type;
    NScheme::TTypeInfoMod parsed;
    TString error;
    return NScheme::TypeInfoFromProto(physical, parsed, error) && parsed.TypeInfo == column.Type &&
        parsed.TypeMod == column.TypeMod &&
        (physical.has_pg_type() || type.has_optional_type() == !column.NotNull);
}

struct TEvEvaluate: TEventLocal<TEvEvaluate, EventSpaceBegin(TEvents::ES_PRIVATE) + 801> {
    struct TRow {
        TSerializedCellVec Key;
        TSerializedCellVec Inputs;
    };
    TVector<TRow> Rows;
    TSerializedCellVec Through;
};

struct TEvEvaluated: TEventLocal<TEvEvaluated, EventSpaceBegin(TEvents::ES_PRIVATE) + 802> {
    NTxProxy::TUploadRows Rows;
    TSerializedCellVec Through;
    THolder<TEvEvaluate> Continuation;
    TString Error;
    bool Prepared = false;
};

class TGeneratedColumnsWorker final: public TActorBootstrapped<TGeneratedColumnsWorker> {
    const TActorId Owner;
    const NKikimrIndexBuilder::TGeneratedColumnBuild Descriptor;
    const ui64 MemoryLimit;
    const ui64 MaxBatchBytes;
    THolder<NGeneratedColumns::IRowProgram> Program;

public:
    TGeneratedColumnsWorker(TActorId owner, const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor, ui64 memoryLimit, ui64 maxBatchBytes)
        : Owner(owner), Descriptor(descriptor), MemoryLimit(memoryLimit), MaxBatchBytes(maxBatchBytes)
    {}

    void Bootstrap() {
        auto result = MakeHolder<TEvEvaluated>();
        result->Prepared = true;
        try {
            if (!AppData()->FunctionRegistry) {
                result->Error = "No function registry is installed on the DataShard node";
            } else {
                Program = NGeneratedColumns::CreateRowProgram(Descriptor, *AppData()->FunctionRegistry, MemoryLimit, result->Error);
                if (!Program && result->Error.empty()) {
                    result->Error = "Cannot prepare generated column row program";
                }
            }
        } catch (const std::exception& error) {
            result->Error = error.what();
        }
        Send(Owner, result.Release());
        Become(&TGeneratedColumnsWorker::StateWork);
    }

    void Handle(TEvEvaluate::TPtr& event) {
        auto result = MakeHolder<TEvEvaluated>();
        try {
            Y_ENSURE(Program, "Generated column evaluator is not prepared");
            result->Rows.reserve(event->Get()->Rows.size());
            ui64 outputBytes = 0;
            for (size_t rowIndex = 0; rowIndex < event->Get()->Rows.size(); ++rowIndex) {
                auto& row = event->Get()->Rows[rowIndex];
                TSerializedCellVec output;
                if (!Program->Evaluate(row.Inputs.GetCells(), output, result->Error)) {
                    break;
                }
                if (output.GetCells().size() != 1) {
                    result->Error = "Generated column evaluator returned an invalid cell count";
                    break;
                }
                const ui64 rowBytes = row.Key.GetBuffer().size() + output.GetBuffer().size();
                Y_ENSURE(rowBytes <= MaxBatchBytes, "Generated evaluation result row exceeds MaxBatchBytes");
                if (outputBytes + rowBytes > MaxBatchBytes) {
                    // The overflowing candidate has not committed. Keep its
                    // owned inputs and evaluate it again after this prefix acks.
                    result->Continuation = MakeHolder<TEvEvaluate>();
                    result->Continuation->Through = std::move(event->Get()->Through);
                    for (size_t i = rowIndex; i < event->Get()->Rows.size(); ++i) {
                        result->Continuation->Rows.push_back(std::move(event->Get()->Rows[i]));
                    }
                    break;
                }
                outputBytes += rowBytes;
                result->Rows.emplace_back(std::move(row.Key), output.GetBuffer());
            }
            result->Through = result->Continuation
                ? result->Rows.back().first : std::move(event->Get()->Through);
        } catch (const std::exception& error) {
            result->Error = error.what();
        }
        Send(Owner, result.Release());
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvEvaluate, Handle);
        cFunc(TEvents::TSystem::PoisonPill, PassAway);
    )
};

class TGeneratedColumnsScan final: public TBuildScanUpload<NKikimrServices::TActivity::BUILD_COLUMNS_SCAN_ACTOR> {
    using TBase = TBuildScanUpload<NKikimrServices::TActivity::BUILD_COLUMNS_SCAN_ACTOR>;
    const NKikimrIndexBuilder::TGeneratedColumnBuild Descriptor;
    const TGeneratedColumnsProjection Projection;
    const TString ColumnName;
    const bool NotNull;
    TActorId Worker;
    THolder<TEvEvaluate> Pending = MakeHolder<TEvEvaluate>();
    THolder<TEvEvaluate> Continuation;
    ui64 PendingBytes = 0;
    TSerializedCellVec ScannedThrough;
    TSerializedCellVec UploadingThrough;
    bool Busy = true;

public:
    TGeneratedColumnsScan(ui64 id, const TString& database, const TString& target,
        const TScanRecord::TSeqNo& seqNo, ui64 shard, const TActorId& progress,
        const TSerializedTableRange& range, const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor,
        const NKikimrTxDataShard::TGeneratedColumnFillContext& context,
        const TUserTable& table, const TIndexBuildScanSettings& settings)
        : TBase(id, database, target, seqNo, shard, progress, range, table, settings, true)
        , Descriptor(descriptor)
        , Projection(BuildGeneratedColumnsProjection(table, descriptor, context.GetTargetColumnId()))
        , ColumnName(table.Columns.at(context.GetTargetColumnId()).Name)
        , NotNull(table.Columns.at(context.GetTargetColumnId()).NotNull)
    {
        ScanTags = Projection.Tags;
        GeneratedFillContext = context;
        UploadMode = NTxProxy::EUploadRowsMode::FillGeneratedColumn;
        UploadColumnsTypes = std::make_shared<NTxProxy::TUploadTypes>();
        for (ui32 columnId : table.KeyColumnIds) {
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(table.Columns.at(columnId).Type, type);
            UploadColumnsTypes->emplace_back(table.Columns.at(columnId).Name, std::move(type));
        }
        Ydb::Type type;
        NScheme::ProtoFromTypeInfo(table.Columns.at(context.GetTargetColumnId()).Type, type);
        UploadColumnsTypes->emplace_back(ColumnName, std::move(type));
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) override {
        TBase::Prepare(driver, std::move(scheme));
        this->Become(&TGeneratedColumnsScan::StateWork);
        // Arbitrary native UDFs execute only on this worker's separate mailbox.
        Worker = this->Register(new TGeneratedColumnsWorker(this->SelfId(), Descriptor,
            ScanSettings.GetMaxProgramMemoryBytes(), ScanSettings.GetMaxBatchBytes()), TMailboxType::HTSwap, AppData()->BatchPoolId);
        return {EScan::Sleep, {}};
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) override {
        Y_ENSURE(!Busy, "Generated scan resumed with evaluation or upload in flight");
        bool dispatched = false;
        if (row.GetCellOp(Projection.TargetPosition) == NTable::ECellOp::Empty) {
            auto inputs = Projection.GetInputs(key, row);
            const ui64 bytes = TSerializedCellVec::SerializedSize(key) + TSerializedCellVec::SerializedSize(inputs);
            if (bytes > ScanSettings.GetMaxBatchBytes()) {
                UploadStatus.StatusCode = Ydb::StatusIds::BAD_REQUEST;
                UploadStatus.Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Generated column " << ColumnName << ": input row exceeds MaxBatchBytes"));
                return EScan::Final;
            }
            if (!Pending->Rows.empty() && PendingBytes + bytes > ScanSettings.GetMaxBatchBytes()) {
                Evaluate();
                dispatched = true;
            }
            auto& owned = Pending->Rows.emplace_back();
            owned.Key = TSerializedCellVec(key);
            owned.Inputs = TSerializedCellVec(inputs);
            PendingBytes += bytes;
        }
        ScannedThrough = TSerializedCellVec(key);
        if (dispatched) {
            // The current row is retained in the next bounded input buffer. The
            // in-flight batch's checkpoint precedes it.
            return EScan::Sleep;
        }
        if (Pending->Rows.size() >= Max<ui32>(1, ScanSettings.GetMaxBatchRows()) ||
            PendingBytes >= Max<ui64>(1, ScanSettings.GetMaxBatchBytes()))
        {
            Evaluate();
            return EScan::Sleep;
        }
        return EScan::Feed;
    }

    EScan Seek(TLead& lead, ui64 sequence) override {
        if (!sequence) {
            return TBase::Seek(lead, sequence);
        }
        if (Busy) {
            return EScan::Sleep;
        }
        if (!Pending->Rows.empty()) {
            Evaluate();
            return EScan::Sleep;
        }
        AcknowledgeSkippedPrefix();
        return TBase::Seek(lead, sequence);
    }

    EScan PageFault() override {
        if (Busy) {
            return EScan::Sleep;
        }
        if (!Pending->Rows.empty()) {
            Evaluate();
            return EScan::Sleep;
        }
        AcknowledgeSkippedPrefix();
        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        if (Worker) {
            this->Send(Worker, new TEvents::TEvPoisonPill());
            Worker = {};
        }
        return TBase::Finish(status);
    }

private:
    void Evaluate() {
        Y_ENSURE(!Busy && !Pending->Rows.empty());
        Busy = true;
        Pending->Through = ScannedThrough;
        this->Send(Worker, Pending.Release());
        Pending = MakeHolder<TEvEvaluate>();
        PendingBytes = 0;
    }

    void AcknowledgeSkippedPrefix() {
        if (ScannedThrough.GetBuffer().empty() || ScannedThrough.GetBuffer() == LastUploadedKey.GetBuffer()) {
            return;
        }
        // There is no pending evaluation or upload here. Every row through this
        // boundary was already materialized in the snapshot.
        LastUploadedKey = ScannedThrough;
        auto progress = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        FillScanResponseCommonFields(*progress, BuildIndexId, DataShardId, SeqNo);
        progress->Record.SetLastKeyAck(LastUploadedKey.GetBuffer());
        progress->Record.SetStatus(NKikimrIndexBuilder::IN_PROGRESS);
        progress->Record.SetUploadStatus(Ydb::StatusIds::SUCCESS);
        this->Send(ProgressActorId, progress.Release());
    }

    void Handle(TEvEvaluated::TPtr& event) {
        auto& result = *event->Get();
        if (!result.Error.empty()) {
            UploadStatus.StatusCode = Ydb::StatusIds::BAD_REQUEST;
            UploadStatus.Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Generated column " << ColumnName << ": " << result.Error));
            Driver->Touch(EScan::Final);
            return;
        }
        if (result.Prepared) {
            Busy = false;
            Driver->Touch(EScan::Feed);
            return;
        }
        UploadingThrough = std::move(result.Through);
        Continuation = std::move(result.Continuation);
        for (auto& [key, serializedValue] : result.Rows) {
            TSerializedCellVec value(serializedValue);
            if (NotNull && value.GetCells()[0].IsNull()) {
                UploadStatus.StatusCode = Ydb::StatusIds::BAD_REQUEST;
                UploadStatus.Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Generated column " << ColumnName << ": NULL result for NOT NULL column"));
                Driver->Touch(EScan::Final);
                return;
            }
            ReadBuf.AddRow(key.GetCells(), value.GetCells(), std::move(serializedValue), key.GetCells());
        }
        Y_ENSURE(!ReadBuf.IsEmpty(), "Successful generated batch has no rows");
        ReadBuf.FlushTo(WriteBuf);
        Upload();
    }

    TSerializedCellVec ExtractAcknowledgedKey() override {
        return std::exchange(UploadingThrough, {});
    }

    bool OnUploadSuccess() override {
        if (Continuation) {
            // Do not wake the scan while the residual prefix is pending. The
            // next input buffer may contain a later row and remains owned.
            this->Send(Worker, Continuation.Release());
            return false;
        }
        Busy = false;
        return true;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvEvaluated, Handle);
            default:
                TBase::StateWork(ev);
        }
    }
};

} // namespace

TVector<TCell> TGeneratedColumnsProjection::GetInputs(TConstArrayRef<TCell> key, const NTable::TRowState& row) const {
    TVector<TCell> result;
    result.reserve(Inputs.size());
    for (const auto& input : Inputs) {
        result.push_back(input.Key ? key[input.Position] : row.Get(input.Position));
    }
    return result;
}

TGeneratedColumnsProjection BuildGeneratedColumnsProjection(const TUserTable& table,
    const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor, ui32 targetColumnId)
{
    TGeneratedColumnsProjection result;
    THashMap<ui32, ui32> keys;
    for (ui32 i = 0; i < table.KeyColumnIds.size(); ++i) {
        keys.emplace(table.KeyColumnIds[i], i);
    }
    THashSet<ui32> seen;
    for (const auto& dependency : descriptor.GetDependencies()) {
        const auto* column = table.Columns.FindPtr(dependency.GetColumnId());
        Y_ENSURE(column && column->Name == dependency.GetName() && MatchesType(*column, dependency.GetType()),
            "Changed generated column dependency binding: " << dependency.GetName());
        Y_ENSURE(dependency.GetColumnId() != targetColumnId && !column->IsStoredGenerated() && !column->IsBuildInProgress && seen.insert(dependency.GetColumnId()).second,
            "Invalid generated column dependency: " << dependency.GetName());
        if (const auto* position = keys.FindPtr(dependency.GetColumnId())) {
            result.Inputs.push_back({true, *position});
        } else {
            result.Inputs.push_back({false, ui32(result.Tags.size())});
            result.Tags.push_back(dependency.GetColumnId());
        }
    }
    Y_ENSURE(table.Columns.contains(targetColumnId), "Missing generated target column");
    result.TargetPosition = result.Tags.size();
    result.Tags.push_back(targetColumnId);
    return result;
}

bool ValidateGeneratedColumnsBuild(const NKikimrTxDataShard::TEvBuildIndexCreateRequest& request,
    const TUserTable& table, NKikimrTxDataShard::TGeneratedColumnFillContext& context, TString& error)
{
    try {
        Y_ENSURE(request.GetColumnBuildSettings().columnSize() == 1, "A generated build must contain exactly one column");
        const auto& setting = request.GetColumnBuildSettings().column(0);
        Y_ENSURE(setting.has_generated_from_expression() && !setting.has_default_from_literal() && setting.default_from_sequence().empty(),
            "Conflicting generated column initializers");
        const auto& descriptor = setting.generated_from_expression();
        Y_ENSURE(request.GetOwnerId() == descriptor.GetSourceOwnerId() && request.GetPathId() == descriptor.GetSourcePathId() &&
            descriptor.GetSourceSchemaVersion() && descriptor.GetSourceSchemaVersion() < table.GetTableSchemaVersion(),
            "Generated program source table binding mismatch");
        Y_ENSURE(request.GetId() && request.HasSeqNoGeneration() && request.HasSeqNoRound() &&
            request.GetSnapshotStep() && request.GetSnapshotTxId(), "Incomplete generated build attempt");
        ui32 targetId = 0;
        for (const auto& [id, column] : table.Columns) {
            if (column.Name == setting.GetColumnName()) {
                Y_ENSURE(column.IsStoredGenerated() && column.IsBuildInProgress && !column.IsKey,
                    "Generated target is not a hidden STORED column");
                Y_ENSURE(column.DefaultFromExpression.GetExprText() == descriptor.GetExprText() && MatchesType(column, descriptor.GetOutputType()),
                    "Generated output type or expression changed");
                Y_ENSURE(column.DefaultFromExpression.DependencyColumnNamesSize() == descriptor.DependenciesSize(),
                    "Generated dependency count changed");
                for (size_t i = 0; i < descriptor.DependenciesSize(); ++i) {
                    Y_ENSURE(column.DefaultFromExpression.GetDependencyColumnNames(i) == descriptor.GetDependencies(i).GetName(),
                        "Generated dependency order changed");
                }
                targetId = id;
            }
        }
        Y_ENSURE(targetId && !descriptor.GetProgram().empty(), "Missing generated target or compiled program");
        BuildGeneratedColumnsProjection(table, descriptor, targetId);
        context.SetBuildId(request.GetId());
        context.SetSeqNoGeneration(request.GetSeqNoGeneration());
        context.SetSeqNoRound(request.GetSeqNoRound());
        context.SetOwnerId(request.GetOwnerId());
        context.SetPathId(request.GetPathId());
        context.SetSchemaVersion(table.GetTableSchemaVersion());
        context.SetTargetColumnId(targetId);
        context.SetSnapshotStep(request.GetSnapshotStep());
        context.SetSnapshotTxId(request.GetSnapshotTxId());
        context.SetProgramFingerprint(MD5::Calc(descriptor.SerializeAsString()));
        return true;
    } catch (const std::exception& exception) {
        error = exception.what();
        return false;
    }
}

TAutoPtr<NTable::IScan> CreateGeneratedColumnsScan(ui64 buildIndexId,
    const TString& databaseName, const TString& target, const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId, const TActorId& progressActorId, const TSerializedTableRange& range,
    const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor,
    const NKikimrTxDataShard::TGeneratedColumnFillContext& fillContext,
    const TUserTable& tableInfo, const TIndexBuildScanSettings& scanSettings)
{
    return new TGeneratedColumnsScan(buildIndexId, databaseName, target, seqNo, dataShardId,
        progressActorId, range, descriptor, fillContext, tableInfo, scanSettings);
}

} // namespace NKikimr::NDataShard
