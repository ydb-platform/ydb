#pragma once

#include "fulltext_workload_params.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

namespace NYdbWorkload {

class TFulltextWorkloadCommandIndexBase : public NYdb::NConsoleClient::TYdbCommand {
private:
    THolder<NYdb::TDriver> Driver;
    THolder<NYdb::NQuery::TQueryClient> QueryClient;

protected:
    NYdbWorkload::TFulltextWorkloadParams& Params;
    bool DryRun = false;

    void HandleQuery(const TString& query);

public:
    TFulltextWorkloadCommandIndexBase(NYdbWorkload::TFulltextWorkloadParams& params, const TString& name, const TString& description = TString());

    virtual void Config(TConfig& config) override;
    virtual void DoConfig(TConfig& config) = 0;

    virtual int Run(TConfig& config) override;
    virtual int DoRun() = 0;
};

class TFulltextWorkloadCommandBuildIndex final : public TFulltextWorkloadCommandIndexBase {
public:
    TFulltextWorkloadCommandBuildIndex(NYdbWorkload::TFulltextWorkloadParams& params);

    virtual void DoConfig(TConfig& config) override;
    virtual int DoRun() override;

private:
    TString ExtractIndexParams() const;
};

class TFulltextWorkloadCommandDropIndex final : public TFulltextWorkloadCommandIndexBase {
public:
    TFulltextWorkloadCommandDropIndex(NYdbWorkload::TFulltextWorkloadParams& params);

    virtual void DoConfig(TConfig& config) override;
    virtual int DoRun() override;
};

} // namespace NYdbWorkload
