#include "ssa_program_optimizer.h"

namespace NKikimr::NSsaOptimizer {

namespace {

std::vector<std::shared_ptr<NArrow::TProgramStep>> ReplaceCountAll(const std::vector<std::shared_ptr<NArrow::TProgramStep>>& program, const NTable::TScheme::TTableSchema& tableSchema) {
    std::vector<std::shared_ptr<NArrow::TProgramStep>> newProgram;
    newProgram.reserve(program.size());

    for (auto& step : program) {
        newProgram.push_back(std::make_shared<NArrow::TProgramStep>(*step));
        for (size_t i = 0; i < step->GroupBy.size(); ++i) {
            auto groupBy = step->GroupBy[i];
            auto& newGroupBy = newProgram.back()->GroupBy;
            if (groupBy.GetOperation() == NArrow::EAggregate::Count 
                && groupBy.GetArguments().size() == 1
                && groupBy.GetArguments()[0] == "") {
                // COUNT(*)
                auto replaceIt = std::next(newGroupBy.begin(), i);
                replaceIt = newGroupBy.erase(replaceIt);
                auto pkColName = tableSchema.Columns.find(tableSchema.KeyColumns[0])->second.Name;
                newGroupBy.emplace(replaceIt, groupBy.GetName(), groupBy.GetOperation(), std::string(pkColName));
            }
        }
    }
    return newProgram;
}

} // anonymous namespace

std::vector<std::shared_ptr<NArrow::TProgramStep>> OptimizeProgram(const std::vector<std::shared_ptr<NArrow::TProgramStep>>& program, const NTable::TScheme::TTableSchema& tableSchema) {
    return ReplaceCountAll(program, tableSchema);
}

} // namespace NKikimr::NSsaOptimizer
