#include "backtrace.h"

#include <ydb/library/yql/utils/backtrace/libbacktrace/symbolizer.h>

#include <util/string/split.h>
#include <util/stream/str.h>

namespace NYql {

    namespace NBacktrace {

        TString Symbolize(const TString& input, const THashMap<TString, TString>& mapping) {
            TString output;
            TStringOutput out(output);

            i64 stackSize = -1;
            TVector<TStackFrame> frames;
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
                    if (parts.size() > 2) {
                        modulePath = parts[1];
                        TryFromString<ui64>(parts[2], address);
                        if (modulePath == "/proc/self/exe") {
                            modulePath = "EXE";
                        }
                        auto it = mapping.find(modulePath);
                        if (it != mapping.end()) {
                            modulePath = it->second;
                        }
                        frames.emplace_back(TStackFrame{modulePath, address});
                    }
                } else {
                    out << line << "\n";
                }
            }

            if (stackSize == 0) {
                out << "Empty stack trace\n";
            }
            for (auto &e: Symbolize(frames)) {
                out << e << "\n";
            }
            return output;
        }

    } /* namespace NBacktrace */

} /* namespace NYql */
