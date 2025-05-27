#include "parameter_stream.h"

#include <ydb/public/lib/ydb_cli/common/common.h>

#include <util/generic/string.h>
#include <util/stream/input.h>

namespace NYdb {
namespace NConsoleClient {

TSimpleParamStream::TSimpleParamStream(IInputStream* input)
    : Input(input) {
}

size_t TSimpleParamStream::ReadLine(TString& res) {
    return Input->ReadLine(res);
}

TString TSimpleParamStream::ReadAll() {
    return Input->ReadAll();
}

TCsvParamStream::TCsvParamStream(IInputStream* input)
    : Input(std::move(input))
    , CountingInput(Input)
    , Splitter(CountingInput) {
}

size_t TCsvParamStream::ReadLine(TString& res) {
    res = Splitter.ConsumeLine();
    size_t len = CountingInput.Counter() - CurrentCount;
    CurrentCount = CountingInput.Counter();
    return len;
}

TString TCsvParamStream::ReadAll() {
    TString res, temp;
    if (!ReadLine(res)) {
        return TString();
    }
    if (ReadLine(temp)) {
        throw TMisuseException() << "Too many rows in data, exactly one CSV/TSV row with values is allowed in \"no-framing\" format.";
    }
    return res;
}

}
}