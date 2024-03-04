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
    if (serializedProgram.empty()) {
        return true;
    }
    NOlap::TProgramContainer ssaProgram;
    TString error;
    if (!ssaProgram.Init(columnResolver, programType, serializedProgram, error)) {
        ErrorDescription = TStringBuilder() << "Can't parse SsaProgram at " << Self->TabletID() << " / " << error;
        return false;
    }
    read.SetProgram(std::move(ssaProgram));
    return true;
}

}
