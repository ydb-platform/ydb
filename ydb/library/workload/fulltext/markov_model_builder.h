#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TMarkovModelBuilder final: public TClientCommand {
    public:
        TMarkovModelBuilder();

        void Config(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TString InputPath;
        TString OutputPath = "markov_dict.tsv.gz";
        int Order = 1;
    };

} // namespace NYdb::NConsoleClient
