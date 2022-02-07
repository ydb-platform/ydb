#include "backtrace.h"

#ifdef _linux_
#include <llvm/DebugInfo/Symbolize/Symbolize.h>
#include <llvm/DebugInfo/Symbolize/DIPrinter.h>
#include <llvm/Support/raw_ostream.h>
#endif

#include <util/string/split.h>
#include <util/stream/str.h>

namespace NYql {

namespace NBacktrace {

struct TStackFrame {
    TString ModulePath;
    ui64 Address;
    ui64 Offset;
};

#ifdef _linux_
class TRawOStreamProxy: public llvm::raw_ostream {
public:
    TRawOStreamProxy(IOutputStream& out)
        : llvm::raw_ostream(true) // unbuffered
        , Slave_(out)
    {
    }
    void write_impl(const char* ptr, size_t size) override {
        Slave_.Write(ptr, size);
    }
    uint64_t current_pos() const override {
        return 0;
    }
    size_t preferred_buffer_size() const override {
        return 0;
    }
private:
    IOutputStream& Slave_;
};
#endif

TString Symbolize(const TString& input, const THashMap<TString, TString>& mapping) {
    TString output;
    TStringOutput out(output);

#ifdef _linux_
    TRawOStreamProxy outStream(out);
    llvm::symbolize::LLVMSymbolizer::Options opts;
    llvm::symbolize::LLVMSymbolizer symbolyzer(opts);
    llvm::symbolize::DIPrinter printer(outStream, true, true, false);
#else
    auto& outStream = out;
#endif

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
            outStream << line << "\n";
        }
    }

    if (stackSize == 0) {
        outStream << "Empty stack trace\n";
    }

    for (const auto& frame : frames) {
#ifdef _linux_
        llvm::object::SectionedAddress secAddr;
        secAddr.Address = frame.Address - frame.Offset;
        auto resOrErr = symbolyzer.symbolizeCode(frame.ModulePath, secAddr);
        if (resOrErr) {
            auto value = resOrErr.get();
            if (value.FileName == "<invalid>" && frame.Offset > 0) {
                value.FileName = frame.ModulePath;
            }

            printer << value;
        } else {
            logAllUnhandledErrors(resOrErr.takeError(), outStream,
                "LLVMSymbolizer: error reading file: ");
        }
#else
        outStream << "StackFrame: " << frame.ModulePath << " " << frame.Address << " " << frame.Offset << Endl;
#endif
    }
    return output;
}

} /* namespace NBacktrace */

} /* namespace NYql */
