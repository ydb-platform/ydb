#include "backtrace.h"
#include "symbolizer.h"

#include "fake_llvm_symbolizer/fake_llvm_symbolizer.h"

#include <util/string/split.h>
#include <util/stream/str.h>

namespace NYql {

namespace NBacktrace {

struct TStackFrame {
    TString ModulePath;
    ui64 Address;
    ui64 Offset;
};

TString Symbolize(const TString& input, const THashMap<TString, TString>& mapping) {
    TString output;
    TStringOutput out(output);

    i64 stackSize = -1;
    TVector<TStackFrame> frames;
    for (TStringBuf line: StringSplitter(input).SplitByString("\n").SkipEmpty()) {
        if (line.StartsWith("StackFrames:")) {
            TVector<TString> parts;
            Split(TString(line), " ", parts);
            if (parts.size() > 1) {
                TryFromString<i64>(parts[1], stackSize);
                frames.reserve(stackSize);
            }
        } else if (line.StartsWith("StackFrame:")) {
            TVector<TString> parts;
            Split(TString(line), " ", parts);
            TString modulePath;
            ui64 address;
            ui64 offset;
            if (parts.size() > 3) {
                modulePath = parts[1];
                TryFromString<ui64>(parts[2], address);
                TryFromString<ui64>(parts[3], offset);
                auto it = mapping.find(modulePath);
                if (it != mapping.end()) {
                    modulePath = it->second;
                }
                frames.emplace_back(TStackFrame{modulePath, address, offset});
            }
        } else {
            out << line << "\n";
        }
    }

    if (stackSize == 0) {
        out << "Empty stack trace\n";
    }

    for (const auto& frame : frames) {
#ifdef _linux_
        llvm::object::SectionedAddress secAddr;
        secAddr.Address = frame.Address - frame.Offset;
        out << NYql::NBacktrace::SymbolizeAndDumpToString(frame.ModulePath, secAddr, frame.Offset) << Endl;
#else
        out << "StackFrame: " << frame.ModulePath << " " << frame.Address << " " << frame.Offset << Endl;
#endif
    }
    return output;
}

} /* namespace NBacktrace */

} /* namespace NYql */
