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
        return nullptr;
    }

    if (read.GetSnapshot().GetPlanStep() < Self->GetMinReadStep()) {
        error = TStringBuilder() << "Snapshot too old: " << read.GetSnapshot();
        return nullptr;
    }

    NOlap::TDataStorageAccessor dataAccessor(insertTable, index, batchCache);
    auto readMetadata = std::make_shared<NOlap::TReadMetadata>(index->GetVersionedIndex(), read.GetSnapshot(), isReverse ? NOlap::TReadMetadata::ESorting::DESC : NOlap::TReadMetadata::ESorting::ASC);
    
    if (!readMetadata->Init(read, dataAccessor, error)) {
        return nullptr;
    }
    return readMetadata;
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
