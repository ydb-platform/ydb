#include "constructor.h"
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/policy.h>
#include <ydb/core/tx/program/program.h>

namespace NKikimr::NOlap::NReader {

NKikimr::TConclusionStatus IScannerConstructor::ParseProgram(const TVersionedIndex* vIndex,
    const NKikimrSchemeOp::EOlapProgramType programType, const TString& serializedProgram, TReadDescription& read, const IColumnResolver& columnResolver) const {
    AFL_VERIFY(!read.ColumnIds.size() || !read.ColumnNames.size());
    std::vector<TString> names;
    std::set<TString> namesChecker;
    for (auto&& i : read.ColumnIds) {
        names.emplace_back(columnResolver.GetColumnName(i));
        AFL_VERIFY(namesChecker.emplace(names.back()).second);
    }
    if (serializedProgram.empty()) {
        for (auto&& i : read.ColumnNames) {
            names.emplace_back(i);
            AFL_VERIFY(namesChecker.emplace(names.back()).second);
        }
        TProgramContainer container;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "overriden_columns")("columns", JoinSeq(",", names));
        container.OverrideProcessingColumns(std::vector<TString>(names.begin(), names.end()));
        read.SetProgram(std::move(container));
        return TConclusionStatus::Success();
    } else {
        TProgramContainer ssaProgram;
        TString error;
        if (!ssaProgram.Init(columnResolver, programType, serializedProgram, error)) {
            return TConclusionStatus::Fail(TStringBuilder() << "Can't parse SsaProgram: " << error);
        }

        if (names.size()) {
            std::set<TString> programColumns;
            for (auto&& i : ssaProgram.GetSourceColumns()) {
                if (!i.second.IsGenerated()) {
                    programColumns.emplace(i.second.GetColumnName());
                }
            }
            //its possible dont use columns from filter where pk field compare with null and remove from PKFilter and program, but stay in kqp columns request
            if (vIndex) {
                for (auto&& i : vIndex->GetSchema(read.GetSnapshot())->GetIndexInfo().GetReplaceKey()->field_names()) {
                    const TString cId(i.data(), i.size());
                    namesChecker.erase(cId);
                    programColumns.erase(cId);
                }
            }

            const auto getDiffColumnsMessage = [&]() {
                return TStringBuilder() << "ssa program has different columns with kqp request: kqp_columns=" << JoinSeq(",", namesChecker) << " vs program_columns=" << JoinSeq(",", programColumns);
            };

            if (namesChecker.size() != programColumns.size()) {
                return TConclusionStatus::Fail(getDiffColumnsMessage());
            }
            for (auto&& i : namesChecker) {
                if (!programColumns.contains(i)) {
                    return TConclusionStatus::Fail(getDiffColumnsMessage());
                }
            }
        }

        read.SetProgram(std::move(ssaProgram));

        return TConclusionStatus::Success();
    }
}

NKikimr::TConclusion<std::shared_ptr<TReadMetadataBase>> IScannerConstructor::BuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const {
    TConclusion<std::shared_ptr<TReadMetadataBase>> result = DoBuildReadMetadata(self, read);
    if (result.IsFail()) {
        return result;
    } else if (!*result) {
        return result.DetachResult();
    } else {
        (*result)->Limit = ItemsLimit;
        return result.DetachResult();
    }
}

}