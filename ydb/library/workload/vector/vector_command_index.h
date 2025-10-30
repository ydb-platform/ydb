#pragma once

#include "vector_workload_params.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

namespace NYdbWorkload {

class TWorkloadCommandIndexBase : public NYdb::NConsoleClient::TYdbCommand {
private:
    THolder<NYdb::TDriver> Driver;
    THolder<NYdb::NQuery::TQueryClient> QueryClient;

protected:
    NYdbWorkload::TVectorWorkloadParams& Params;
    bool DryRun = false;

    void HandleQuery(const TString& query);

public:
    TWorkloadCommandIndexBase(NYdbWorkload::TVectorWorkloadParams& params, const TString& name, const TString& description = TString());

    virtual void Config(TConfig& config) override;
    virtual void DoConfig(TConfig& config) = 0;

    virtual int Run(TConfig& config) override;
    virtual int DoRun() = 0;
};

class TWorkloadCommandBuildIndex final : public TWorkloadCommandIndexBase {
public:
    TWorkloadCommandBuildIndex(NYdbWorkload::TVectorWorkloadParams& params);

    virtual void DoConfig(TConfig& config) override;
    virtual int DoRun() override;
};

class TWorkloadCommandDropIndex final : public TWorkloadCommandIndexBase {
public:
    TWorkloadCommandDropIndex(NYdbWorkload::TVectorWorkloadParams& params);

    virtual void DoConfig(TConfig& config) override;
    virtual int DoRun() override;
};

} // namespace NYdbWorkload
