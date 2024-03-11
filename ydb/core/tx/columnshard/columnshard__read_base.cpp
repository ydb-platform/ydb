#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard__read_base.h>
#include <ydb/core/tx/columnshard/columnshard__index_scan.h>

namespace NKikimr::NColumnShard {


std::shared_ptr<NOlap::TReadMetadata>
TTxReadBase::PrepareReadMetadata(const NOlap::TReadDescription& read,
                                 const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                 const std::unique_ptr<NOlap::IColumnEngine>& index,
                                 TString& error, const bool isReverse) const {
    if (!insertTable || !index) {
        return nullptr;
    }

    if (read.GetSnapshot().GetPlanStep() < Self->GetMinReadStep()) {
        error = TStringBuilder() << "Snapshot too old: " << read.GetSnapshot();
        return nullptr;
    }

    NOlap::TDataStorageAccessor dataAccessor(insertTable, index);
    auto readMetadata = std::make_shared<NOlap::TReadMetadata>(index->GetVersionedIndex(), read.GetSnapshot(),
                            isReverse ? NOlap::TReadMetadata::ESorting::DESC : NOlap::TReadMetadata::ESorting::ASC, read.GetProgram());

    if (!readMetadata->Init(read, dataAccessor, error)) {
        return nullptr;
    }
    return readMetadata;
}

bool TTxReadBase::ParseProgram(NKikimrSchemeOp::EOlapProgramType programType,
    TString serializedProgram, NOlap::TReadDescription& read, const NOlap::IColumnResolver& columnResolver) {

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
        NOlap::TProgramContainer container;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "overriden_columns")("columns", JoinSeq(",", names));
        container.OverrideProcessingColumns(std::vector<TString>(names.begin(), names.end()));
        read.SetProgram(std::move(container));
        return true;
    } else {
        NOlap::TProgramContainer ssaProgram;
        TString error;
        if (!ssaProgram.Init(columnResolver, programType, serializedProgram, error)) {
            ErrorDescription = TStringBuilder() << "Can't parse SsaProgram at " << Self->TabletID() << " / " << error;
            return false;
        }

        if (names.size()) {
            std::set<TString> programColumns;
            for (auto&& i : ssaProgram.GetSourceColumns()) {
                if (!i.second.IsGenerated()) {
                    programColumns.emplace(i.second.GetColumnName());
                }
            }
            //its possible dont use columns from filter where pk field compare with null and remove from PKFilter and program, but stay in kqp columns request
            if (Self->TablesManager.HasPrimaryIndex()) {
                for (auto&& i : Self->TablesManager.GetIndexInfo(read.GetSnapshot()).GetReplaceKey()->field_names()) {
                    const TString cId(i.data(), i.size());
                    namesChecker.erase(cId);
                    programColumns.erase(cId);
                }
            }

            const auto getDiffColumnsMessage = [&]() {
                return TStringBuilder() << "ssa program has different columns with kqp request: kqp_columns=" << JoinSeq(",", namesChecker) << " vs program_columns=" << JoinSeq(",", programColumns);
            };

            if (namesChecker.size() != programColumns.size()) {
                ErrorDescription = getDiffColumnsMessage();
                return false;
            }
            for (auto&& i : namesChecker) {
                if (!programColumns.contains(i)) {
                    ErrorDescription = getDiffColumnsMessage();
                    return false;
                }
            }
        }

        read.SetProgram(std::move(ssaProgram));

        return true;
    }
}

}
