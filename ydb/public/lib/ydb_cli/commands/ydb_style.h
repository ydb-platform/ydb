#pragma once

#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parameters.h>

#include <util/stream/fwd.h>
#include <util/folder/path.h>
#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

    class TCommandStyle
        : public TYdbCommand,
          public TCommandWithOutput,
          public TCommandWithParameters {
    public:
        TCommandStyle();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        int RunStd();
        void FormatDEntry(const TFsPath& path, bool isSkippingNoSql = false);
        void FormatFile(const TFsPath& path);
        void FormatDirectory(const TFsPath& path);
        bool Format(IInputStream& input, TString& formatted, TString& error);

        TVector<TFsPath> Paths;
    };

} // namespace NYdb::NConsoleClient
