#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

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

    struct TYQLCompleterConfig {
        TColorSchema Color;
        TLazyDriver::TPtr LazyDriver;
        TString Database;
        bool IsVerbose;
    };

    IYQLCompleter::TPtr MakeYQLCompleter(const TYQLCompleterConfig& config);

    struct TCompositeCompleterConfig {
        // Commands matched on the leading '/' (case-sensitive prefix).
        std::vector<TString> SlashCommands;
        // TCL-style command lines (BEGIN, COMMIT, ROLLBACK, ...) used as
        // case-insensitive completion candidates whenever the current input
        // looks like a transaction control command.
        std::vector<TString> TclCommands;
        // YQL completer used for everything else; nullopt disables YQL completion.
        std::optional<TYQLCompleterConfig> YqlCompleterConfig;
    };

    IYQLCompleter::TPtr MakeYQLCompositeCompleter(const TCompositeCompleterConfig& config);

} // namespace NYdb::NConsoleClient
