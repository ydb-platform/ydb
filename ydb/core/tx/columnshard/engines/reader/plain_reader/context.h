#pragma once
#include "columns_set.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>

namespace NKikimr::NOlap::NPlainReader {

class IDataSource;

class TSpecialReadContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<TReadContext>, CommonContext);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Field>>, ResultFields);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, ResultSchema);
    YDB_READONLY_DEF(std::vector<TString>, ResultFieldNames);

    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
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
            "ff=" << FFColumns->DebugString() << ";"
            ;
    }

    TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
        : CommonContext(commonContext)
    {
        ReadMetadata = dynamic_pointer_cast<const TReadMetadata>(CommonContext->GetReadMetadata());
        Y_ABORT_UNLESS(ReadMetadata);
        Y_ABORT_UNLESS(ReadMetadata->SelectInfo);

        EFColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetEarlyFilterColumnIds(), ReadMetadata->GetIndexInfo());
        PKColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetPKColumnIds(), ReadMetadata->GetIndexInfo());
        FFColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetAllColumns(), ReadMetadata->GetIndexInfo());
        TrivialEFFlag = EFColumns->ColumnsOnly(ReadMetadata->GetIndexInfo().ArrowSchemaSnapshot()->field_names());


        PKFFColumns = std::make_shared<TColumnsSet>(*PKColumns + *FFColumns);
        EFPKColumns = std::make_shared<TColumnsSet>(*EFColumns + *PKColumns);
        FFMinusEFColumns = std::make_shared<TColumnsSet>(*FFColumns - *EFColumns);
        FFMinusEFPKColumns = std::make_shared<TColumnsSet>(*FFColumns - *EFColumns - *PKColumns);

        Y_ABORT_UNLESS(FFColumns->Contains(EFColumns));

        auto resultSchema = ReadMetadata->GetLoadSchema(ReadMetadata->GetSnapshot());
        for (auto&& f : ReadMetadata->GetAllColumns()) {
            ResultFields.emplace_back(resultSchema->GetFieldByColumnIdVerified(f));
            ResultFieldNames.emplace_back(ResultFields.back()->name());
        }
        ResultSchema = std::make_shared<arrow::Schema>(ResultFields);
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
