#pragma once

#include <google/protobuf/message.h>

namespace NYdbGrpc {

template<typename TInProtoPrinter>
TString MakeDbgMessageString(const NProtoBuf::Message& message, bool ok) {
    TString resp;
    if (ok) {
        TInProtoPrinter printer;
        printer.SetSingleLineMode(true);
        printer.PrintToString(message, &resp);
    } else {
        resp = "<not ok>";
    }
    return resp;
}

} // namespace NYdbGrpc
