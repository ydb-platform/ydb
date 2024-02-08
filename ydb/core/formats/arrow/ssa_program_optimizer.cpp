#include "ssa_program_optimizer.h"

namespace NKikimr::NSsa {

namespace {

void ReplaceCountAll(TProgram& program) {
    Y_ABORT_UNLESS(!program.SourceColumns.empty());

    for (auto& step : program.Steps) {
        Y_ABORT_UNLESS(step);

        for (auto& groupBy : step->MutableGroupBy()) {
            if (groupBy.GetOperation() == EAggregate::Count && groupBy.GetArguments().empty()) {
                if (step->GetGroupByKeys().size()) {
                    groupBy.MutableArguments().push_back(step->GetGroupByKeys()[0]);
                } else {
                    auto& anySourceColumn = program.SourceColumns.begin()->second;
                    groupBy.MutableArguments().push_back(anySourceColumn);
                }
            }
        }
    }
}

} // anonymous namespace

void OptimizeProgram(TProgram& program) {
    ReplaceCountAll(program);
}

}
