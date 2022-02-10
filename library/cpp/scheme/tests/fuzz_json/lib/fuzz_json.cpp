#include "fuzz_json.h"
#include "util/generic/fwd.h"

#include <library/cpp/scheme/scheme.h>
#include <util/stream/null.h>

namespace {
    static constexpr size_t MAX_DEPTH = 4;
    static constexpr size_t MAX_PATH_LEN = 256;
    static constexpr size_t MAX_ITERATIONS = 4;

    void SplitOnDepth(const TStringBuf src, const size_t depth, const size_t maxPathLen,
        TStringBuf& left, TStringBuf& right)
    {
        size_t pos = 0;
        size_t prevPos = 0;
        for(size_t i = 0; i < depth; ++i) {
            if (pos > maxPathLen) {
                break;
            }
            prevPos = pos;
            pos = src.find_first_of(TStringBuf("/]"), pos + 1);
            if (pos == TStringBuf::npos) {
                break;
            }
        }
        if (pos == TStringBuf::npos && prevPos > 0) {
            pos = prevPos;
        }
        if (src.length() > maxPathLen)  {
            if (pos == TStringBuf::npos || pos > maxPathLen) {
                pos = maxPathLen;
            }
        }
        if (pos == TStringBuf::npos || pos == 0) {
            left = src;
            right = TStringBuf();
        } else {
            src.SplitAt(pos + 1, left, right);
        }
    }

    TString tmp;
    //Limit max array size in the path to 256
    TStringBuf ProcessPath(TStringBuf path) {
        size_t pos = 0;
        while(pos != TStringBuf::npos) {
            pos = path.find(']', pos + 1);
            if (pos == TStringBuf::npos) {
                continue;
            }
            size_t open = path.rfind('[', pos);
            if (open == TStringBuf::npos) {
                continue;
            }
            bool allDigit = true;
            for(size_t i = open + 1; i < pos; ++i) {
                if (path[i] < '0' || path[i] > '9') {
                    allDigit = false;
                    break;
                }
            }
            if (!allDigit) {
                continue;
            }
            if (pos - open > 4)  {
                TString str = TString::Join(path.Head(open + 1), "256", path.Tail(pos));
                tmp = std::move(str);
                path = tmp;
                pos = (open + 1) + 3;
                continue;
            }
        }
        return path;
    }
}

namespace NSc::NUt {


    void FuzzJson(TStringBuf wire) {
        if (wire.size() < 2) {
            return;
        }


        ProcessPath("[123][1234][12][2134][12312312][1][12]");
        ui8 len1 = wire[0];
        ui8 len2 = wire[1];
        wire.Skip(2);
        auto json1 = wire.NextTokAt(len1);
        auto json2 = wire.NextTokAt(len2);
        NSc::TValue val1 = NSc::TValue::FromJson(json1);
        NSc::TValue val2 = NSc::TValue::FromJson(json2);
        NSc::TValue val3;
        val3.MergeUpdate(val1);

        size_t i = 0;
        while (!wire.empty()) {
            TStringBuf path;
            SplitOnDepth(wire, MAX_DEPTH, MAX_PATH_LEN, path, wire);
            path = ProcessPath(path);
            if (auto* target = val3.TrySelectOrAdd(path)) {
                target->MergeUpdate(val2);
            }
            ++i;
            // Release memory since there are up to MAX_DICT_SIZE * MAX_DEPTH elements
            if (i > MAX_ITERATIONS) {
                Cnull << val3.ToJson();
                val3 = NSc::TValue();
            }
        }
        Cnull << val3.ToJson();
    }
}
