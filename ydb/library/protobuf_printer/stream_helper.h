#pragma once
#include <util/generic/noncopyable.h>
#include <util/stream/output.h>

#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

namespace NKikimr {

class TProtobufPrinterOutputWrapper : public TMoveOnly {
public:
    TProtobufPrinterOutputWrapper(const google::protobuf::Message& msg, const google::protobuf::TextFormat::Printer& printer)
        : Msg(msg)
        , Printer(printer)
    {
    }

    operator TString() const;

    void Print(IOutputStream& o) const;

private:
    const google::protobuf::Message& Msg;
    const google::protobuf::TextFormat::Printer& Printer;
};

} // namespace NKikimr
