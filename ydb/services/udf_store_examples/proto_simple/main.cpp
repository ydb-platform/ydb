#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <ydb/services/udf_store_examples/proto_simple/example.pb.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" {
__attribute__((visibility("default")))
void proto_roundtrip(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* arg)
{
    if (arg->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }

    NProtoSimpleExample::TMessage message;
    message.set_value(42);

    TProtoStringType serialized;
    if (!message.SerializeToString(&serialized)) {
        result->Type = EValueType::Null;
        return;
    }

    NProtoSimpleExample::TMessage parsed;
    if (!parsed.ParseFromString(serialized)) {
        result->Type = EValueType::Null;
        return;
    }

    result->Type = EValueType::Int64;
    result->Data.Int64 = parsed.has_value() ? parsed.value() : 0;
}
}
