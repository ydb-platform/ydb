#include "event_field_printer.h"

#include <library/cpp/protobuf/json/proto2json.h>

namespace {

    const NProtobufJson::TProto2JsonConfig PROTO_2_JSON_CONFIG = NProtobufJson::TProto2JsonConfig()
        .SetMissingRepeatedKeyMode(NProtobufJson::TProto2JsonConfig::MissingKeyDefault)
        .AddStringTransform(MakeIntrusive<NProtobufJson::TBase64EncodeBytesTransform>());

} // namespace

TEventProtobufMessageFieldPrinter::TEventProtobufMessageFieldPrinter(EProtobufMessageFieldPrintMode mode)
    : Mode(mode)
{}

template <>
void TEventProtobufMessageFieldPrinter::PrintProtobufMessageFieldToOutput<google::protobuf::Message, false>(const google::protobuf::Message& field, TEventFieldOutput& output) {
    switch (Mode) {
        case EProtobufMessageFieldPrintMode::DEFAULT:
        case EProtobufMessageFieldPrintMode::JSON: {
            // Do not use field.PrintJSON() here: IGNIETFERRO-2002
            NProtobufJson::Proto2Json(field, output, PROTO_2_JSON_CONFIG);
            break;
        }
    }
}
