#pragma once
#include "columns_set.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>

namespace NKikimr::NOlap::NPlainReader {

class IDataSource;

class TSpecialReadContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TReadContext>, CommonContext);

    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, SpecColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, ResultColumns);
    
    TReadMetadata::TConstPtr ReadMetadata;
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();
    std::shared_ptr<TColumnsSet> PKFFColumns;
    std::shared_ptr<TColumnsSet> EFPKColumns;
    std::shared_ptr<TColumnsSet> FFMinusEFColumns;
    std::shared_ptr<TColumnsSet> FFMinusEFPKColumns;
    bool TrivialEFFlag = false;
public:
    ui64 GetMemoryForSources(const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const bool isExclusive);

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    std::shared_ptr<NIndexedReader::TMergePartialStream> BuildMerger() const;

    TString DebugString() const {
        return TStringBuilder() <<
            "ef=" << EFColumns->DebugString() << ";" <<
            "pk=" << PKColumns->DebugString() << ";" <<
            "ff=" << FFColumns->DebugString() << ";" <<
            "result_schema=" << ResultColumns->DebugString()
            ;
    }

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
        : CommonContext(commonContext)
    {
        ReadMetadata = dynamic_pointer_cast<const TReadMetadata>(CommonContext->GetReadMetadata());
        Y_ABORT_UNLESS(ReadMetadata);
        Y_ABORT_UNLESS(ReadMetadata->SelectInfo);

        SpecColumns = std::make_shared<TColumnsSet>(TIndexInfo::GetSpecialColumnIdsSet(), ReadMetadata->GetIndexInfo());
        EFColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetEarlyFilterColumnIds(), ReadMetadata->GetIndexInfo());
        *EFColumns = *EFColumns + *SpecColumns;
        if (ReadMetadata->GetProgram().HasProgram()) {
            FFColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetProcessingColumnIds(), ReadMetadata->GetIndexInfo());
            AFL_VERIFY(!FFColumns->Contains(*SpecColumns))("info", FFColumns->DebugString());
            *FFColumns = *FFColumns + *EFColumns;
        } else {
            FFColumns = std::make_shared<TColumnsSet>(*EFColumns);
        }
        ResultColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetResultColumnIds(), ReadMetadata->GetIndexInfo());
//        AFL_VERIFY(FFColumns->Contains(*ResultColumns))("info", FFColumns->DebugString())("res", ResultColumns->DebugString());
        *FFColumns = *FFColumns + *ResultColumns;

        PKColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetPKColumnIds(), ReadMetadata->GetIndexInfo());

        TrivialEFFlag = EFColumns->ColumnsOnly(ReadMetadata->GetIndexInfo().ArrowSchemaSnapshot()->field_names());

        PKFFColumns = std::make_shared<TColumnsSet>(*PKColumns + *FFColumns);
        EFPKColumns = std::make_shared<TColumnsSet>(*EFColumns + *PKColumns);
        FFMinusEFColumns = std::make_shared<TColumnsSet>(*FFColumns - *EFColumns);
        FFMinusEFPKColumns = std::make_shared<TColumnsSet>(*FFColumns - *EFColumns - *PKColumns);

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("columns_context_info", DebugString());
    }

    TFetchingPlan GetColumnsFetchingPlan(const bool exclusiveSource) const {
        if (CommonContext->GetIsInternalRead()) {
            return TFetchingPlan(PKFFColumns, EmptyColumns, exclusiveSource);
        }

        if (exclusiveSource) {
            if (TrivialEFFlag) {
                return TFetchingPlan(FFColumns, EmptyColumns, true);
            } else {
                return TFetchingPlan(EFColumns, FFMinusEFColumns, true);
            }
        } else {
            if (TrivialEFFlag) {
                return TFetchingPlan(PKFFColumns, EmptyColumns, false);
            } else {
                return TFetchingPlan(EFPKColumns, FFMinusEFPKColumns, false);
            }
        }
    }
};

}
