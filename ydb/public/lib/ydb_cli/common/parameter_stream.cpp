#include "parameter_stream.h"

#include <ydb/public/lib/ydb_cli/common/common.h>

#include <util/generic/string.h>
#include <util/stream/input.h>

namespace NYdb {
namespace NConsoleClient {

size_t TSimpleParamStream::ReadLine(TString& res) {
    return Cin.ReadLine(res);
}

TString TSimpleParamStream::ReadAll() {
    return Cin.ReadAll();
}

TCsvParamStream::TCsvParamStream() : Input(&Cin), Splitter(Input) {
}

size_t TCsvParamStream::ReadLine(TString& res) {
    res = Splitter.ConsumeLine();
    size_t len = Input.Counter() - CurrentCount;
    CurrentCount = Input.Counter();
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