#include "stream_helper.h"

#include <google/protobuf/messagext.h>

namespace NKikimr {

TProtobufPrinterOutputWrapper::operator TString() const {
    TString string;
    Printer.PrintToString(Msg, &string);
    return string;
}

void TProtobufPrinterOutputWrapper::Print(IOutputStream& o) const {
    NProtoBuf::io::TCopyingOutputStreamAdaptor adaptor(&o);
    if (!Printer.Print(Msg, &adaptor)) {
        o << "Error printing message";
    }
}

} // namespace NKikimr

template<>
void Out<NKikimr::TProtobufPrinterOutputWrapper>(IOutputStream& o, typename TTypeTraits<NKikimr::TProtobufPrinterOutputWrapper>::TFuncParam x) {
    return x.Print(o);
}
