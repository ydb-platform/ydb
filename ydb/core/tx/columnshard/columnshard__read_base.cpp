#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard__read_base.h>
#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/formats/arrow/ssa_program_optimizer.h>

namespace NKikimr::NColumnShard {

std::shared_ptr<NOlap::TReadMetadata>
TTxReadBase::PrepareReadMetadata(const TActorContext& ctx, const NOlap::TReadDescription& read,
                                 const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                 const std::unique_ptr<NOlap::IColumnEngine>& index,
                                 const TBatchCache& batchCache,
                                 TString& error, const bool isReverse) const {
    Y_UNUSED(ctx);

    if (!insertTable || !index) {
        return {};
    }

    if (read.PlanStep < Self->GetMinReadStep()) {
        error = Sprintf("Snapshot %" PRIu64 ":%" PRIu64 " too old", read.PlanStep, read.TxId);
        return {};
    }

    const NOlap::TIndexInfo& indexInfo = index->GetIndexInfo();
    auto spOut = std::make_shared<NOlap::TReadMetadata>(indexInfo, isReverse ? NOlap::TReadMetadata::ESorting::DESC : NOlap::TReadMetadata::ESorting::ASC);
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
    for (auto& cmt : out.CommittedBlobs) {
        if (auto batch = batchCache.Get(cmt.BlobId)) {
            out.CommittedBatches.emplace(cmt.BlobId, batch);
        }
    }

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

        for (auto&& i : read.PKRangesFilter.GetColumnNames()) {
            requiredColumns.emplace(i);
        }
        out.SetPKRangesFilter(read.PKRangesFilter);

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

    THashSet<ui32> columnIds;
    for (auto& field : out.LoadSchema->fields()) {
        TString column(field->name().data(), field->name().size());
        columnIds.insert(indexInfo.GetColumnId(column));
    }

    out.Program = std::move(read.Program);
    if (out.Program) {
        for (auto& [id, name] : out.Program->SourceColumns) {
            columnIds.insert(id);
        }
    }

    if (read.ReadNothing) {
        out.SelectInfo = std::make_shared<NOlap::TSelectInfo>();
    } else {
        out.SelectInfo = index->Select(read.PathId, {read.PlanStep, read.TxId}, columnIds, out.GetPKRangesFilter());
    }
    return spOut;
}

bool TTxReadBase::ParseProgram(const TActorContext& ctx, NKikimrSchemeOp::EOlapProgramType programType,
    TString serializedProgram, NOlap::TReadDescription& read, const NOlap::IColumnResolver& columnResolver)
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

    auto ssaProgram = read.AddProgram(columnResolver, program);
    if (!ssaProgram) {
        ErrorDescription = TStringBuilder() << "Wrong olap program";
        return false;
    }
    if (!ssaProgram->Steps.empty() && Self->TablesManager.HasPrimaryIndex()) {
        NSsa::OptimizeProgram(*ssaProgram);
    }

    read.Program = ssaProgram;
    return true;
}

}
