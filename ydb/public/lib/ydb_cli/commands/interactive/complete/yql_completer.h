#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/ptr.h>

namespace NYdb::NConsoleClient {

    using TCompletions = replxx::Replxx::completions_t;

    class IYQLCompleter {
    public:
        using TPtr = THolder<IYQLCompleter>;

        virtual TCompletions Apply(const std::string& prefix, int& contextLen) = 0;
        virtual ~IYQLCompleter() = default;
    };

    IYQLCompleter::TPtr MakeYQLCompleter(TColorSchema color);

} // namespace NYdb::NConsoleClient
