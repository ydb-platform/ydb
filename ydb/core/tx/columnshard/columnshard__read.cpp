#include "columnshard_impl.h"
#include "columnshard_txs.h"
#include "columnshard_schema.h"
#include "columnshard__index_scan.h"
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>
#include <ydb/core/formats/ssa_program_optimizer.h>

namespace NKikimr::NColumnShard {

namespace {

template <typename T, typename U>
TVector<T> ProtoToVector(const U& cont) {
    return TVector<T>(cont.begin(), cont.end());
}

}

using namespace NTabletFlatExecutor;

class TTxRead : public TTxReadBase {
public:
    TTxRead(TColumnShard* self, TEvColumnShard::TEvRead::TPtr& ev)
        : TTxReadBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_READ; }

private:
    TEvColumnShard::TEvRead::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvReadResult> Result;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
};


std::shared_ptr<NOlap::TReadMetadata>
TTxReadBase::PrepareReadMetadata(const TActorContext& ctx, const TReadDescription& read,
                                 const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                 const std::unique_ptr<NOlap::IColumnEngine>& index,
                                 TString& error) const {
    Y_UNUSED(ctx);

    if (!insertTable || !index) {
        return {};
    }

    if (read.PlanStep < Self->GetMinReadStep()) {
        error = Sprintf("Snapshot %" PRIu64 ":%" PRIu64 " too old", read.PlanStep, read.TxId);
        return {};
    }

    const NOlap::TIndexInfo& indexInfo = index->GetIndexInfo();
    auto spOut = std::make_shared<NOlap::TReadMetadata>(indexInfo);
    auto& out = *spOut;

    out.PlanStep = read.PlanStep;
    out.TxId = read.TxId;

    // schemas

    out.BlobSchema = indexInfo.ArrowSchema();
    if (read.ColumnIds.size()) {
        out.ResultSchema = indexInfo.ArrowSchema(read.ColumnIds);
    } else if (read.ColumnNames.size()) {
        out.ResultSchema = indexInfo.ArrowSchema(read.ColumnNames);
    } else {
        error = "Empty column list requested";
        return {};
    }

    if (!out.BlobSchema) {
        error = "Could not get BlobSchema.";
        return {};
    }

    if (!out.ResultSchema) {
        error = "Could not get ResultSchema.";
        return {};
    }

    // insert table

    out.CommittedBlobs = insertTable->Read(read.PathId, read.PlanStep, read.TxId);

    // index

    /// @note We could have column name changes between schema versions:
    /// Add '1:foo', Drop '1:foo', Add '2:foo'. Drop should hide '1:foo' from reads.
    /// It's expected that we have only one version on 'foo' in blob and could split them by schema {planStep:txId}.
    /// So '1:foo' would be omitted in blob records for the column in new snapshots. And '2:foo' - in old ones.
    /// It's not possible for blobs with several columns. There should be a special logic for them.
    TVector<TString> columns = read.ColumnNames;
    if (!read.ColumnIds.empty()) {
        columns = indexInfo.GetColumnNames(read.ColumnIds);
    }
    Y_VERIFY(!columns.empty(), "Empty column list");

    { // Add more columns: snapshot, replace, predicate
        // Key columns (replace, sort)
        THashSet<TString> requiredColumns = indexInfo.GetRequiredColumns();

        // Snapshot columns
        requiredColumns.insert(NOlap::TIndexInfo::SPEC_COL_PLAN_STEP);
        requiredColumns.insert(NOlap::TIndexInfo::SPEC_COL_TX_ID);

        // Predicate columns
        if (read.LessPredicate) {
            for (auto& col : read.LessPredicate->ColumnNames()) {
                requiredColumns.insert(col);
            }
        }
        if (read.GreaterPredicate) {
            for (auto& col : read.GreaterPredicate->ColumnNames()) {
                requiredColumns.insert(col);
            }
        }

        for (auto& col : columns) {
            requiredColumns.erase(col);
        }

        for (auto& reqCol : requiredColumns) {
            columns.push_back(reqCol);
        }
    }

    out.LoadSchema = indexInfo.AddColumns(out.ResultSchema, columns);
    if (!out.LoadSchema) {
        return {};
    }

    if (read.LessPredicate) {
        if (!read.LessPredicate->Good() ||
            !read.LessPredicate->IsTo()) {
            return {};
        }
        out.LessPredicate = read.LessPredicate;
    }
    if (read.GreaterPredicate) {
        if (!read.GreaterPredicate->Good() ||
            !read.GreaterPredicate->IsFrom()) {
            return {};
        }
        out.GreaterPredicate = read.GreaterPredicate;
    }

    THashSet<ui32> columnIds;
    for (auto& field : out.LoadSchema->fields()) {
        TString column(field->name().data(), field->name().size());
        columnIds.insert(indexInfo.GetColumnId(column));
    }

    out.Program = std::move(read.Program);
    for (auto& [id, name] : read.ProgramSourceColumns) {
        columnIds.insert(id);
    }

    if (read.ReadNothing) {
        out.SelectInfo = std::make_shared<NOlap::TSelectInfo>();
    } else {
        out.SelectInfo = index->Select(read.PathId, {read.PlanStep, read.TxId}, columnIds,
                                       out.GreaterPredicate, out.LessPredicate);
    }
    return spOut;
}

bool TTxReadBase::ParseProgram(const TActorContext& ctx, NKikimrSchemeOp::EOlapProgramType programType,
    TString serializedProgram, TReadDescription& read, const IColumnResolver& columnResolver)
{
    if (serializedProgram.empty()) {
        return true;
    }

    NKikimrSSA::TProgram program;
    NKikimrSSA::TOlapProgram olapProgram;

    switch (programType) {
        case NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS:
            if (!olapProgram.ParseFromString(serializedProgram)) {
                ErrorDescription = TStringBuilder() << "Can't parse TOlapProgram at " << Self->TabletID();
                return false;
            }

            if (!program.ParseFromString(olapProgram.GetProgram())) {
                ErrorDescription = TStringBuilder() << "Can't parse TProgram at " << Self->TabletID();
                return false;
            }

            break;
        default:
            ErrorDescription = TStringBuilder() << "Unsupported olap program version: " << (ui32)programType;
            return false;
    }

    if (ctx.LoggerSettings() &&
        ctx.LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::TX_COLUMNSHARD))
    {
        TString out;
        ::google::protobuf::TextFormat::PrintToString(program, &out);
        LOG_S_DEBUG("Process program: " << Endl << out);
    }

    if (olapProgram.HasParameters()) {
        Y_VERIFY(olapProgram.HasParametersSchema(), "Parameters are present, but there is no schema.");

        auto schema = NArrow::DeserializeSchema(olapProgram.GetParametersSchema());
        read.ProgramParameters = NArrow::DeserializeBatch(olapProgram.GetParameters(), schema);
    }


    auto ssaProgramSteps = read.AddProgram(columnResolver, program);
    if (!ssaProgramSteps) {
        ErrorDescription = TStringBuilder() << "Wrong olap program";
        return false;
    }
    if (!ssaProgramSteps->Program.empty() && Self->PrimaryIndex) {
        ssaProgramSteps->Program = NKikimr::NSsaOptimizer::OptimizeProgram(ssaProgramSteps->Program, Self->PrimaryIndex->GetIndexInfo());
    }
        
    read.Program = ssaProgramSteps->Program;
    read.ProgramSourceColumns = ssaProgramSteps->ProgramSourceColumns;
    return true;
}

bool TTxRead::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_VERIFY(Ev);
    Y_VERIFY(Self->PrimaryIndex);
    Y_UNUSED(txc);
    LOG_S_DEBUG("TTxRead.Execute at tablet " << Self->TabletID());

    txc.DB.NoMoreReadsForTx();

    const NOlap::TIndexInfo& indexInfo = Self->PrimaryIndex->GetIndexInfo();
    auto& record = Proto(Ev->Get());

    ui64 metaShard = record.GetTxInitiator();

    TReadDescription read;
    read.PlanStep = record.GetPlanStep();
    read.TxId = record.GetTxId();
    read.PathId = record.GetTableId();
    read.ReadNothing = Self->PathsToDrop.count(read.PathId);
    read.ColumnIds = ProtoToVector<ui32>(record.GetColumnIds());
    read.ColumnNames = ProtoToVector<TString>(record.GetColumnNames());
    if (read.ColumnIds.empty() && read.ColumnNames.empty()) {
        auto allColumnNames = indexInfo.ArrowSchema()->field_names();
        read.ColumnNames.assign(allColumnNames.begin(), allColumnNames.end());
    }

    if (record.HasGreaterPredicate()) {
        auto& proto = record.GetGreaterPredicate();
        auto schema = indexInfo.ArrowSchema(ProtoToVector<TString>(proto.GetColumnNames()));
        read.GreaterPredicate = std::make_shared<NOlap::TPredicate>(
            NArrow::EOperation::Greater, proto.GetRow(), schema, proto.GetInclusive());
    }
    if (record.HasLessPredicate()) {
        auto& proto = record.GetLessPredicate();
        auto schema = indexInfo.ArrowSchema(ProtoToVector<TString>(proto.GetColumnNames()));
        read.LessPredicate = std::make_shared<NOlap::TPredicate>(
            NArrow::EOperation::Less, proto.GetRow(), schema, proto.GetInclusive());
    }

    bool parseResult = ParseProgram(ctx, record.GetOlapProgramType(), record.GetOlapProgram(), read,
        TIndexColumnResolver(Self->PrimaryIndex->GetIndexInfo()));

    std::shared_ptr<NOlap::TReadMetadata> metadata;
    if (parseResult) {
        metadata = PrepareReadMetadata(ctx, read, Self->InsertTable, Self->PrimaryIndex, ErrorDescription);
    }

    ui32 status = NKikimrTxColumnShard::EResultStatus::ERROR;

    if (metadata) {
        Self->MapExternBlobs(ctx, *metadata);
        ReadMetadata = metadata;
        status = NKikimrTxColumnShard::EResultStatus::SUCCESS;
    }

    Result = std::make_unique<TEvColumnShard::TEvReadResult>(
        Self->TabletID(), metaShard, read.PlanStep, read.TxId, read.PathId, 0, true, status);

    if (status == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        Self->IncCounter(COUNTER_READ_SUCCESS);
    } else {
        Self->IncCounter(COUNTER_READ_FAIL);
    }
    return true;
}

void TTxRead::Complete(const TActorContext& ctx) {
    Y_VERIFY(Ev);
    Y_VERIFY(Result);

    bool noData = !ReadMetadata || ReadMetadata->Empty();
    bool success = (Proto(Result.get()).GetStatus() == NKikimrTxColumnShard::EResultStatus::SUCCESS);

    if (!success) {
        LOG_S_DEBUG("TTxRead.Complete. Error " << ErrorDescription << " while reading at tablet " << Self->TabletID());
        ctx.Send(Ev->Get()->GetSource(), Result.release());
    } else if (noData) {
        LOG_S_DEBUG("TTxRead.Complete. Empty result at tablet " << Self->TabletID());
        ctx.Send(Ev->Get()->GetSource(), Result.release());
    } else {
        LOG_S_DEBUG("TTxRead.Complete at tablet " << Self->TabletID() << " Metadata: " << *ReadMetadata);

        const ui64 requestCookie = Self->InFlightReadsTracker.AddInFlightRequest(
            std::static_pointer_cast<const NOlap::TReadMetadataBase>(ReadMetadata), *Self->BlobManager);
        auto statsDelta = Self->InFlightReadsTracker.GetSelectStatsDelta();

        Self->IncCounter(COUNTER_READ_INDEX_GRANULES, statsDelta.Granules);
        Self->IncCounter(COUNTER_READ_INDEX_PORTIONS, statsDelta.Portions);
        Self->IncCounter(COUNTER_READ_INDEX_BLOBS, statsDelta.Blobs);
        Self->IncCounter(COUNTER_READ_INDEX_ROWS, statsDelta.Rows);
        Self->IncCounter(COUNTER_READ_INDEX_BYTES, statsDelta.Bytes);

        TInstant deadline = TInstant::Max(); // TODO
        ctx.Register(CreateReadActor(Self->TabletID(), Ev->Get()->GetSource(),
            std::move(Result), ReadMetadata, deadline, Self->SelfId(), requestCookie));
    }
}


void TColumnShard::Handle(TEvColumnShard::TEvRead::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    TRowVersion readVersion(msg->Record.GetPlanStep(), msg->Record.GetTxId());
    TRowVersion maxReadVersion = GetMaxReadVersion();
    LOG_S_DEBUG("Read at tablet " << TabletID() << " version=" << readVersion << " readable=" << maxReadVersion);

    if (maxReadVersion < readVersion) {
        WaitingReads.emplace(readVersion, std::move(ev));
        WaitPlanStep(readVersion.Step);
        return;
    }

    Execute(new TTxRead(this, ev), ctx);
}

}
