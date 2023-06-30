#pragma once

#include "yql_job_base.h"

#include <ydb/library/yql/providers/yt/codec/yt_codec_job.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>

#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/io/job_reader.h>
#include <yt/cpp/mapreduce/io/job_writer.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/generic/hash_set.h>

#include <utility>

namespace NKikimr {
    namespace NMiniKQL {
        class IFunctionRegistry;
    }
}

namespace NYql {

class TYqlUserJob: public TYqlJobBase {
public:
    TYqlUserJob()
        : TYqlJobBase()
    {
    }
    virtual ~TYqlUserJob() = default;

    void SetUseSkiff(bool useSkiff, TMkqlIOSpecs::TSystemFields sysFields) {
        UseSkiff = useSkiff;
        SkiffSysFields = sysFields;
    }

    void SetYamrInput(bool yamrInput) {
        YamrInput = yamrInput;
    }

    void SetLambdaCode(const TString& code) {
        LambdaCode = code;
    }

    void SetInputSpec(const TString& spec) {
        InputSpec = spec;
    }

    void SetInputGroups(const TVector<ui32>& inputGroups) {
        InputGroups = inputGroups;
    }

    void SetOutSpec(const TString& spec) {
        OutSpec = spec;
    }

    void SetAuxColumns(const THashSet<TString>& auxColumns) {
        AuxColumns = auxColumns;
    }

    void SetInputType(const TString& type) {
        InputType = type;
    }

    void SetRowOffsets(const TVector<ui64>& rowOffsets) {
        RowOffsets = rowOffsets;
    }

    std::pair<NYT::TFormat, NYT::TFormat> GetIOFormats(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry) const;

    void Save(IOutputStream& s) const override;
    void Load(IInputStream& s) override;

protected:
    void DoImpl(const TFile& inHandle, const TVector<TFile>& outHandles) final;

protected:
    // Serializable part (don't forget to add new members to Save/Load)
    bool UseSkiff = false;
    TMkqlIOSpecs::TSystemFields SkiffSysFields;
    bool YamrInput = false;
    TString LambdaCode;
    TString InputSpec;
    TString OutSpec;
    TVector<ui32> InputGroups;
    THashSet<TString> AuxColumns;
    TString InputType;
    TVector<ui64> RowOffsets;
    // End of serializable part

    NKikimr::NMiniKQL::TExploringNodeVisitor Explorer;
    THolder<NKikimr::NMiniKQL::IComputationGraph> CompGraph;
    THolder<NKikimr::NMiniKQL::TBindTerminator> BindTerminator;

    THolder<TMkqlIOSpecs> MkqlIOSpecs;
};

} // NYql
