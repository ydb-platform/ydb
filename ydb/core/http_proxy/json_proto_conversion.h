#pragma once

#include <ydb/library/http_proxy/error/error.h>
#include <ydb/library/naming_conventions/naming_conventions.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/json_output_create.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/proto2json_printer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <contrib/libs/protobuf/src/google/protobuf/message.h>
#include <contrib/libs/protobuf/src/google/protobuf/reflection.h>
#include <grpcpp/impl/codegen/config_protobuf.h>
#include <nlohmann/json.hpp>

namespace NKikimr::NHttpProxy {

void ProtoToJson(const NProtoBuf::Message& resp, NJson::TJsonValue& value, bool skipBase64Encode);
void JsonToProto(const NJson::TJsonValue& jsonValue, NProtoBuf::Message* message, ui32 depth = 0);
void NlohmannJsonToProto(const nlohmann::json& jsonValue, NProtoBuf::Message* message, ui32 depth = 0);

} // namespace NKikimr::NHttpProxy
