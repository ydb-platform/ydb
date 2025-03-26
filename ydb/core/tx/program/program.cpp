#include "builder.h"
#include "program.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/program/execution.h>

namespace NKikimr::NOlap {

const THashSet<ui32>& TProgramContainer::GetSourceColumns() const {
    if (!Program) {
        return Default<THashSet<ui32>>();
    }
    return Program->GetSourceColumns();
}

bool TProgramContainer::HasProgram() const {
    return !!Program;
}

const THashSet<ui32>& TProgramContainer::GetEarlyFilterColumns() const {
    if (!Program) {
        return Default<THashSet<ui32>>();
    }
    return Program->GetFilterColumns();
}

TConclusionStatus TProgramContainer::Init(const NArrow::NSSA::IColumnResolver& columnResolver, const NKikimrSSA::TProgram& programProto) noexcept {
    ProgramProto = programProto;
    if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        TString out;
        ::google::protobuf::TextFormat::PrintToString(programProto, &out);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "parse_program")("program", out);
    }

    if (programProto.HasKernels()) {
        try {
            if (!KernelsRegistry.Parse(programProto.GetKernels())) {
                return TConclusionStatus::Fail("Can't parse kernels");
            }
        } catch (...) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "program_parsed_error")("result", CurrentExceptionMessage());
            return TConclusionStatus::Fail(TStringBuilder() << "Can't initialize program, exception thrown: " << CurrentExceptionMessage());
        }
    }

    auto parseStatus = ParseProgram(columnResolver, programProto);
    if (parseStatus.IsFail()) {
        return parseStatus;
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "program_parsed")("result", DebugString());
    return TConclusionStatus::Success();
}

TConclusionStatus TProgramContainer::Init(
    const NArrow::NSSA::IColumnResolver& columnResolver, const NKikimrSSA::TOlapProgram& olapProgramProto) noexcept {
    NKikimrSSA::TProgram programProto;
    if (!programProto.ParseFromString(olapProgramProto.GetProgram())) {
        return TConclusionStatus::Fail("Can't parse TProgram protobuf");
    }

    if (olapProgramProto.HasParameters()) {
        Y_ABORT_UNLESS(olapProgramProto.HasParametersSchema(), "Parameters are present, but there is no schema.");

        auto schema = NArrow::DeserializeSchema(olapProgramProto.GetParametersSchema());
        ProgramParameters = NArrow::DeserializeBatch(olapProgramProto.GetParameters(), schema);
    }

    ProgramProto = programProto;

    auto initStatus = Init(columnResolver, ProgramProto);
    if (initStatus.IsFail()) {
        return initStatus;
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TProgramContainer::Init(
    const NArrow::NSSA::IColumnResolver& columnResolver, NKikimrSchemeOp::EOlapProgramType programType, TString serializedProgram) noexcept {
    Y_ABORT_UNLESS(serializedProgram);
    Y_ABORT_UNLESS(!OverrideProcessingColumnsVector);

    NKikimrSSA::TOlapProgram olapProgramProto;

    switch (programType) {
        case NKikimrSchemeOp::EOlapProgramType::OLAP_PROGRAM_SSA_PROGRAM_WITH_PARAMETERS:
            if (!olapProgramProto.ParseFromString(serializedProgram)) {
                return TConclusionStatus::Fail("Can't parse TOlapProgram protobuf");
            }

            break;
        default:
            return TConclusionStatus::Fail(TStringBuilder() << "Unsupported olap program version: " << (ui32)programType);
    }

    return Init(columnResolver, olapProgramProto);
}

TConclusionStatus TProgramContainer::ParseProgram(const NArrow::NSSA::IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program) {
    using TId = NKikimrSSA::TProgram::TCommand;

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("parse_proto_program", program.DebugString());
//    Cerr << program.DebugString() << Endl;
    NArrow::NSSA::TProgramBuilder programBuilder(columnResolver, KernelsRegistry);
    for (auto& cmd : program.GetCommand()) {
        switch (cmd.GetLineCase()) {
            case TId::kAssign: {
                auto status = programBuilder.ReadAssign(cmd.GetAssign(), ProgramParameters);
                if (status.IsFail()) {
                    return status;
                }
                break;
            }
            case TId::kFilter: {
                auto status = programBuilder.ReadFilter(cmd.GetFilter());
                if (status.IsFail()) {
                    return status;
                }
                break;
            }
            case TId::kProjection: {
                auto status = programBuilder.ReadProjection(cmd.GetProjection());
                if (status.IsFail()) {
                    return status;
                }
                break;
            }
            case TId::kGroupBy: {
                auto status = programBuilder.ReadGroupBy(cmd.GetGroupBy());
                if (status.IsFail()) {
                    return status;
                }
                break;
            }
            case TId::LINE_NOT_SET:
                return TConclusionStatus::Fail("incorrect SSA line case");
        }
    }
    auto programStatus = programBuilder.Finish();
    if (programStatus.IsFail()) {
        return programStatus;
    }
    Program = programStatus.DetachResult();
    return TConclusionStatus::Success();
}

const THashSet<ui32>& TProgramContainer::GetProcessingColumns() const {
    if (!Program) {
        if (OverrideProcessingColumnsSet) {
            return *OverrideProcessingColumnsSet;
        }
        return Default<THashSet<ui32>>();
    }
    return Program->GetSourceColumns();
}

TConclusionStatus TProgramContainer::ApplyProgram(
    const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& collection, const std::shared_ptr<NArrow::NSSA::IDataSource>& source) const {
    if (Program) {
        return Program->Apply(source, collection);
    } else if (OverrideProcessingColumnsVector) {
        collection->RemainOnly(*OverrideProcessingColumnsVector, true);
    }
    return TConclusionStatus::Success();
}

TConclusion<std::shared_ptr<arrow::RecordBatch>> TProgramContainer::ApplyProgram(
    const std::shared_ptr<arrow::RecordBatch>& batch, const NArrow::NSSA::IColumnResolver& resolver) const {
    auto resources = std::make_shared<NArrow::NAccessor::TAccessorsCollection>(batch, resolver);
    auto status = ApplyProgram(resources, std::make_shared<NArrow::NSSA::TFakeDataSource>());
    if (status.IsFail()) {
        return status;
    }
    return resources->ToBatch();
}

}   // namespace NKikimr::NOlap
