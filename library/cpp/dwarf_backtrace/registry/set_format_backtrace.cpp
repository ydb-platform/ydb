#include <library/cpp/dwarf_backtrace/backtrace.h>
#include <util/stream/format.h>
#include <util/system/backtrace.h>

namespace {
    void PrintDwarfBacktrace(IOutputStream* out, void* const* backtrace, size_t size) {
        auto error = NDwarf::ResolveBacktrace({backtrace, size}, [out](const NDwarf::TLineInfo& info) {
            *out << info.FileName << ":" << info.Line << ":" << info.Col
                 << " in " << info.FunctionName << " (" << Hex(info.Address, HF_ADDX) << ')' << Endl;
            return NDwarf::EResolving::Continue;
        });
        if (error) {
            *out << "***Cannot get backtrace: " << error->Message << " (" << error->Code << ")***" << Endl;
        }
    }

    [[maybe_unused]] auto _ = SetFormatBackTraceFn(&PrintDwarfBacktrace);
}
