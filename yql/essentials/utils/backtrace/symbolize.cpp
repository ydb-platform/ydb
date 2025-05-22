#include "backtrace.h"

#include "symbolizer.h"

#include <util/string/split.h>
#include <util/stream/str.h>

namespace NYql {

    namespace NBacktrace {
        TString Symbolize(const TString& input, const THashMap<TString, TString>& mapping) {
#if defined(__linux__) && defined(__x86_64__)
            TString output;
            TStringOutput out(output);

            i64 stackSize = -1;
            TVector<TStackFrame> frames;
            TVector<TString> usedFilenames;
            for (TStringBuf line: StringSplitter(input).SplitByString("\n")) {
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
                        usedFilenames.emplace_back(std::move(modulePath));
                        frames.emplace_back(TStackFrame{usedFilenames.back().c_str(), address - offset});
                    }
                } else {
                    out << line << "\n";
                }
            }

            if (stackSize == 0) {
                out << "Empty stack trace\n";
            }
            Symbolize(frames.data(), frames.size(), &out);
            return output;
#else
            Y_UNUSED(mapping);
            return input;
#endif
        }

    } /* namespace NBacktrace */

} /* namespace NYql */
