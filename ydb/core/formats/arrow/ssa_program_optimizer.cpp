#include "ssa_program_optimizer.h"

namespace NKikimr::NSsa {

namespace {

void ReplaceCountAll(TProgram& program) {
    Y_VERIFY(!program.SourceColumns.empty());

    for (auto& step : program.Steps) {
        Y_VERIFY(step);

        for (auto& groupBy : step->GroupBy) {
            if (groupBy.GetOperation() == EAggregate::Count && groupBy.GetArguments().empty()) {
                if (!step->GroupByKeys.empty()) {
                    groupBy.MutableArguments().push_back(step->GroupByKeys[0]);
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
