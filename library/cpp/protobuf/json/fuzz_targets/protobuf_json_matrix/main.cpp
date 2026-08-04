#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/yassert.h>

namespace {

constexpr size_t MaxString = 48;
constexpr ui32 MaxNodes = 6;

TString SmallAscii(FuzzedDataProvider& provider) {
    TString value = provider.ConsumeRandomLengthString(
        provider.ConsumeIntegralInRange<size_t>(0, MaxString));
    for (char& c : value) {
        const ui8 x = static_cast<ui8>(c);
        if (x < 0x20 || x > 0x7e || c == '\\') {
            c = char('a' + (x % 26));
        }
    }
    return value;
}

void FillLocation(Ydb::Discovery::NodeLocation* location, FuzzedDataProvider& provider) {
    if (provider.ConsumeBool()) {
        location->set_data_center_num(provider.ConsumeIntegral<ui32>());
    }
    if (provider.ConsumeBool()) {
        location->set_room_num(provider.ConsumeIntegral<ui32>());
    }
    if (provider.ConsumeBool()) {
        location->set_rack_num(provider.ConsumeIntegral<ui32>());
    }
    if (provider.ConsumeBool()) {
        location->set_body_num(provider.ConsumeIntegral<ui32>());
    }
    if (provider.ConsumeBool()) {
        location->set_bridge_pile_name(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        location->set_data_center(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        location->set_module(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        location->set_rack(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        location->set_unit(SmallAscii(provider));
    }
}

void FillNode(Ydb::Discovery::NodeInfo* node, FuzzedDataProvider& provider) {
    if (provider.ConsumeBool()) {
        node->set_node_id(provider.ConsumeIntegral<ui32>());
    }
    if (provider.ConsumeBool()) {
        node->set_host(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        node->set_port(provider.ConsumeIntegral<ui32>());
    }
    if (provider.ConsumeBool()) {
        node->set_resolve_host(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        node->set_address(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        FillLocation(node->mutable_location(), provider);
    }
    if (provider.ConsumeBool()) {
        node->set_expire(provider.ConsumeIntegral<ui64>());
    }
}

Ydb::Discovery::NodeRegistrationResult MakeMessage(FuzzedDataProvider& provider) {
    Ydb::Discovery::NodeRegistrationResult message;
    if (provider.ConsumeBool()) {
        message.set_node_id(provider.ConsumeIntegral<ui32>());
    }
    if (provider.ConsumeBool()) {
        message.set_domain_path(SmallAscii(provider));
    }
    if (provider.ConsumeBool()) {
        message.set_expire(provider.ConsumeIntegral<ui64>());
    }
    const ui32 nodeCount = provider.ConsumeIntegralInRange<ui32>(0, MaxNodes);
    for (ui32 i = 0; i < nodeCount; ++i) {
        FillNode(message.add_nodes(), provider);
    }
    if (provider.ConsumeBool()) {
        message.set_scope_tablet_id(provider.ConsumeIntegral<ui64>());
    }
    if (provider.ConsumeBool()) {
        message.set_scope_path_id(provider.ConsumeIntegral<ui64>());
    }
    if (provider.ConsumeBool()) {
        message.set_node_name(SmallAscii(provider));
    }
    return message;
}

template <typename TEnum>
TEnum PickEnum(FuzzedDataProvider& provider, ui32 maxValue) {
    return static_cast<TEnum>(provider.ConsumeIntegralInRange<ui32>(0, maxValue));
}

NProtobufJson::TProto2JsonConfig MakePrintConfig(FuzzedDataProvider& provider) {
    NProtobufJson::TProto2JsonConfig config;
    config.SetFormatOutput(provider.ConsumeBool());
    config.SetSortMapKeys(true);
    config.SetMapAsObject(provider.ConsumeBool());
    config.SetStringifyNumbers(PickEnum<NProtobufJson::TProto2JsonConfig::EStringifyNumbersMode>(
        provider, NProtobufJson::TProto2JsonConfig::StringifyInt64Always));
    config.SetStringifyNumbersRepeated(PickEnum<NProtobufJson::TProto2JsonConfig::EStringifyNumbersMode>(
        provider, NProtobufJson::TProto2JsonConfig::StringifyInt64Always));
    config.SetMissingSingleKeyMode(PickEnum<NProtobufJson::TProto2JsonConfig::MissingKeyMode>(
        provider, NProtobufJson::TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired));
    config.SetMissingRepeatedKeyMode(PickEnum<NProtobufJson::TProto2JsonConfig::MissingKeyMode>(
        provider, NProtobufJson::TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired));
    config.SetAddMissingFields(provider.ConsumeBool());

    if (provider.ConsumeBool()) {
        config.SetUseJsonName(true);
    } else {
        config.SetFieldNameMode(PickEnum<NProtobufJson::TProto2JsonConfig::FldNameMode>(
            provider, NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense));
    }
    return config;
}

NProtobufJson::TJson2ProtoConfig MakeParseConfig(
    const NProtobufJson::TProto2JsonConfig& printConfig,
    FuzzedDataProvider& provider) {
    NProtobufJson::TJson2ProtoConfig config;
    if (printConfig.UseJsonName) {
        config.SetUseJsonName(true);
    } else {
        config.SetFieldNameMode(static_cast<NProtobufJson::TJson2ProtoConfig::FldNameMode>(printConfig.FieldNameMode));
    }
    config.SetMapAsObject(printConfig.MapAsObject);
    config.SetCastFromString(true);
    config.SetCastRobust(provider.ConsumeBool());
    config.SetAllowUnknownFields(provider.ConsumeBool());
    config.SetReplaceRepeatedFields(true);
    config.CheckRequiredFields = provider.ConsumeBool();
    return config;
}

bool PrintParsePrint(
    const Ydb::Discovery::NodeRegistrationResult& input,
    const NProtobufJson::TProto2JsonConfig& printConfig,
    const NProtobufJson::TJson2ProtoConfig& parseConfig,
    TString* stableJson) {
    TString json;
    NProtobufJson::Proto2Json(input, json, printConfig);

    Ydb::Discovery::NodeRegistrationResult parsed;
    try {
        NProtobufJson::Json2Proto(json, parsed, parseConfig);
    } catch (const yexception&) {
        return false;
    } catch (const std::exception&) {
        return false;
    }

    TString json2;
    NProtobufJson::Proto2Json(parsed, json2, printConfig);

    Ydb::Discovery::NodeRegistrationResult reparsed;
    NProtobufJson::Json2Proto(json2, reparsed, parseConfig);
    TString json3;
    NProtobufJson::Proto2Json(reparsed, json3, printConfig);
    Y_ABORT_UNLESS(json2 == json3);
    if (stableJson) {
        *stableJson = std::move(json2);
    }
    return true;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    const auto message = MakeMessage(provider);
    const auto printConfig = MakePrintConfig(provider);
    const auto parseConfig = MakeParseConfig(printConfig, provider);
    TString stableJson;
    PrintParsePrint(message, printConfig, parseConfig, &stableJson);

    if (!stableJson.empty() && provider.remaining_bytes() > 0) {
        const size_t offset = provider.ConsumeIntegralInRange<size_t>(0, stableJson.size() - 1);
        stableJson[offset] = char(stableJson[offset] ^ provider.ConsumeIntegralInRange<ui8>(1, 255));
        Ydb::Discovery::NodeRegistrationResult parsed;
        try {
            NProtobufJson::Json2Proto(stableJson, parsed, parseConfig);
            PrintParsePrint(parsed, printConfig, parseConfig, nullptr);
        } catch (const yexception&) {
        } catch (const std::exception&) {
        }
    }

    return 0;
}
