#pragma once

#include <ydb/library/workload/benchmark_base/workload.h>

namespace NYdbWorkload {

namespace NQuery {

class TQueryWorkloadParams final : public TWorkloadBaseParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;
    TString GetWorkloadName() const override;
    TString GetDescription(ECommandType commandType, int workloadType) const override;

    YDB_READONLY_DEF(TFsPath, SuitePath);
    YDB_READONLY_DEF(TVector<TString>, CustomQueries);
};

class TQueryGenerator final: public TWorkloadQueryGeneratorBase<TQueryWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TQueryWorkloadParams>;
    using TBase::TBase;
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;
    std::string GetDDLQueries() const override;
    TVector<std::string> GetCleanPaths() const override;

protected:
    TQueryInfoList GetInitialData() override;

private:
    TQueryInfo MakeQuery(const TString& queryText, const TString& queryName) const;
    TQueryInfoList GetWorkloadFromDir(const TFsPath& dir, const TString namePrefix) const;
    std::string GetDDLQueriesFromDir(const TFsPath& dir) const;
};

} // namespace NLog

} // namespace NYdbWorkload
