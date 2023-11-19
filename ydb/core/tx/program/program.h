#pragma once

#include "registry.h" 
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/ssa.pb.h>
#include <ydb/core/formats/arrow/program.h>
#include <ydb/core/formats/arrow/custom_registry.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>


namespace NKikimr::NOlap {
class IColumnResolver {
public:
    virtual ~IColumnResolver() = default;
    virtual TString GetColumnName(ui32 id, bool required = true) const = 0;
    virtual const NTable::TScheme::TTableSchema& GetSchema() const = 0;
};

class TProgramContainer {
private:
    std::shared_ptr<NSsa::TProgram> Program;
    std::shared_ptr<arrow::RecordBatch> ProgramParameters; // TODO
    TKernelsRegistry KernelsRegistry;
public:
    bool Init(const IColumnResolver& columnResolver, NKikimrSchemeOp::EOlapProgramType programType, TString serializedProgram, TString& error);

    std::shared_ptr<NArrow::TColumnFilter> ApplyEarlyFilter(std::shared_ptr<arrow::Table>& batch, const bool useFilter) const {
        if (Program) {
            return Program->ApplyEarlyFilter(batch, useFilter);
        } else {
            return nullptr;
        }
    }

    inline arrow::Status ApplyProgram(std::shared_ptr<arrow::RecordBatch>& batch) const {
        if (Program) {
            return Program->ApplyTo(batch, NArrow::GetCustomExecContext());
        }
        return arrow::Status::OK();
    }

    const THashMap<ui32, TString>& GetSourceColumns() const;
    bool HasProgram() const;

    std::set<std::string> GetEarlyFilterColumns() const;
    std::set<std::string> GetProcessingColumns() const;

    bool HasEarlyFilterOnly() const;
private:
    bool ParseProgram(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program, TString& error);
};

}
