#pragma once

#include "registry.h" 
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/formats/arrow/protos/ssa.pb.h>
#include <ydb/core/formats/arrow/program.h>
#include <ydb/core/formats/arrow/custom_registry.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/checker.h>
#include <ydb/core/tx/columnshard/common/portion.h>

namespace NKikimr::NSchemeShard {
class TOlapSchema;
}

namespace NKikimr::NOlap {
class IColumnResolver {
public:
    virtual ~IColumnResolver() = default;
    virtual TString GetColumnName(ui32 id, bool required = true) const = 0;
    virtual std::optional<ui32> GetColumnIdOptional(const TString& name) const = 0;
    virtual const NTable::TScheme::TTableSchema& GetSchema() const = 0;
    virtual NSsa::TColumnInfo GetDefaultColumn() const = 0;
};

class TSchemaResolverColumnsOnly: public IColumnResolver {
private:
    std::shared_ptr<NSchemeShard::TOlapSchema> Schema;
public:
    TSchemaResolverColumnsOnly(const std::shared_ptr<NSchemeShard::TOlapSchema>& schema)
        : Schema(schema) {
        AFL_VERIFY(Schema);
    }

    virtual TString GetColumnName(ui32 id, bool required = true) const override;
    virtual std::optional<ui32> GetColumnIdOptional(const TString& name) const override;
    virtual const NTable::TScheme::TTableSchema& GetSchema() const override {
        AFL_VERIFY(false);
        return Default<NTable::TScheme::TTableSchema>();
    }
    virtual NSsa::TColumnInfo GetDefaultColumn() const override {
        return NSsa::TColumnInfo::Original((ui32)NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP_INDEX, NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP);
    }
};

class TProgramContainer {
private:
    NKikimrSSA::TProgram ProgramProto;
    std::shared_ptr<NSsa::TProgram> Program;
    std::shared_ptr<arrow::RecordBatch> ProgramParameters; // TODO
    TKernelsRegistry KernelsRegistry;
    std::optional<std::set<std::string>> OverrideProcessingColumnsSet;
    std::optional<std::vector<TString>> OverrideProcessingColumnsVector;
    YDB_READONLY_DEF(NIndexes::TIndexCheckerContainer, IndexChecker);
public:
    TString ProtoDebugString() const {
        return ProgramProto.DebugString();
    }

    TString DebugString() const {
        return Program ? Program->DebugString() : "NO_PROGRAM";
    }

    bool HasOverridenProcessingColumnIds() const {
        return !!OverrideProcessingColumnsVector;
    }

    bool HasProcessingColumnIds() const {
        return !!Program || !!OverrideProcessingColumnsVector;
    }
    void OverrideProcessingColumns(const std::vector<TString>& data) {
        if (data.empty()) {
            return;
        }
        Y_ABORT_UNLESS(!Program);
        OverrideProcessingColumnsVector = data;
        OverrideProcessingColumnsSet = std::set<std::string>(data.begin(), data.end());
    }

    bool Init(const IColumnResolver& columnResolver, NKikimrSchemeOp::EOlapProgramType programType, TString serializedProgram, TString& error);
    bool Init(const IColumnResolver& columnResolver, const NKikimrSSA::TOlapProgram& olapProgramProto, TString& error);
    bool Init(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& programProto, TString& error);

    const std::vector<std::shared_ptr<NSsa::TProgramStep>>& GetSteps() const {
        if (!Program) {
            return Default<std::vector<std::shared_ptr<NSsa::TProgramStep>>>();
        } else {
            return Program->Steps;
        }
    }

    template <class TDataContainer>
    inline arrow::Status ApplyProgram(std::shared_ptr<TDataContainer>& batch) const {
        if (Program) {
            return Program->ApplyTo(batch, NArrow::GetCustomExecContext());
        } else if (OverrideProcessingColumnsVector) {
            batch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(batch, *OverrideProcessingColumnsVector);
        }
        return arrow::Status::OK();
    }

    const THashMap<ui32, NSsa::TColumnInfo>& GetSourceColumns() const;
    bool HasProgram() const;

    std::set<std::string> GetEarlyFilterColumns() const;
    std::set<std::string> GetProcessingColumns() const;
private:
    bool ParseProgram(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program, TString& error);
};

}
