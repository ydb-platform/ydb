#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/ptr.h>

namespace NYdb::NConsoleClient {

    using TCompletions = replxx::Replxx::completions_t;
    using THints = replxx::Replxx::hints_t;

    class IYQLCompleter {
    public:
        using TPtr = THolder<IYQLCompleter>;

        virtual TCompletions ApplyHeavy(TStringBuf text, const std::string& prefix, int& contextLen) = 0;
        virtual THints ApplyLight(TStringBuf text, const std::string& prefix, int& contextLen) = 0;
        virtual ~IYQLCompleter() = default;
    };

    IYQLCompleter::TPtr MakeYQLCompleter(
        TColorSchema color, TDriver driver, TString database, bool isVerbose);

} // namespace NYdb::NConsoleClient
