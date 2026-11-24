#include "helpers.h"

using namespace NFq::NWasmServices::NTestHelpers;

Y_UNIT_TEST_SUITE(TFqWasmManifestTest) {
    Y_UNIT_TEST(ServiceManifestAcceptsOutputAtResultByteLimit) {
        // The result header is 16 bytes, and a String adds a 4-byte length.
        // 16 + 4 + 32748 == 32768, so this output fits exactly.
        const TString manifest = R"({
            "module_type": "module", "module_kind": "wasm", "module_name": "Boundary",
            "module_extension": "wasm", "service_abi_version": 2,
            "service_methods": [{
                "name": "Read", "batch": false, "max_batch_rows": 1,
                "input": [{"name": "id", "type": "Uint64"}],
                "output": [{"name": "value", "type": "String", "max_bytes": 32748}],
                "max_output_row_bytes": 32752
            }]
        })";
        const auto parsed = ParseServiceManifest(manifest);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Methods.at("Read").MaxOutputRowBytes, 32752);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Methods.at("Read").Output.at(0).MaxBytes, 32748);
        NJson::TJsonValue root;
        UNIT_ASSERT(NJson::ReadJsonTree(manifest, &root, true));
        auto& method = root["service_methods"][0];
        method["output"][0]["max_bytes"] = 32749;
        UNIT_ASSERT_EXCEPTION(ParseServiceManifest(NJson::WriteJson(root, false)), yexception);
        method["output"][0]["max_bytes"] = 32748;
        method["input"] = method["output"];
        method["input"][0]["max_bytes"] = 32732;
        UNIT_ASSERT_VALUES_EQUAL(ParseServiceManifest(NJson::WriteJson(root, false)).Methods.at("Read").MaxInputRowBytes, 32736);
        method["input"][0]["max_bytes"] = 32733;
        UNIT_ASSERT_EXCEPTION(ParseServiceManifest(NJson::WriteJson(root, false)), yexception);
    }

    Y_UNIT_TEST(ServiceManifestDescribesDifferentModules) {
        const auto profile = ParseServiceManifest(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        const auto echo = ParseServiceManifest(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"));
        UNIT_ASSERT_VALUES_EQUAL(profile.Name, "WASM_PROFILE");
        UNIT_ASSERT_VALUES_EQUAL(profile.Methods.at("Profile").Input[0].Name, "id");
        UNIT_ASSERT_VALUES_EQUAL(echo.Name, "Echo");
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Echo").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodEcho);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Length").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodLength);
        UNIT_ASSERT_VALUES_EQUAL(profile.Methods.at("Profile").Id, NYdb::NWasm::NServices::NGenerated::NModuleWASM_PROFILE::MethodProfile);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Echo").Output.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Length").Output.size(), 1);
    }

    Y_UNIT_TEST(ServiceGeneratedDispatchIgnoresManifestOrder) {
        NJson::TJsonValue root;
        UNIT_ASSERT(NJson::ReadJsonTree(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"), &root, true));
        for (const auto& method : root["service_methods"].GetArraySafe()) {
            UNIT_ASSERT(!method.Has("id"));
        }
        auto& methods = root["service_methods"].GetArraySafe();
        std::swap(methods[0], methods[1]);
        auto additional = methods[0];
        additional["name"] = "Additional";
        methods.push_back(std::move(additional));
        const auto changed = ParseServiceManifest(NJson::WriteJson(root, false));
        UNIT_ASSERT_VALUES_EQUAL(changed.Methods.at("Echo").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodEcho);
        UNIT_ASSERT_VALUES_EQUAL(changed.Methods.at("Length").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodLength);
    }

    Y_UNIT_TEST(ServiceRowContractRejectsOldDispatchVersion) {
        using namespace NYdb::NWasm::NServices;
        TServiceRequest request;
        request.Count = 1;
        request.Version = 1;
        const auto bytes = Encode(request, {});
        TServiceRequest decoded;
        std::string_view rows;
        UNIT_ASSERT(!ReadServiceRequest({ bytes.data(), bytes.size() }, decoded, rows));
    }

    Y_UNIT_TEST(ServiceManifestRejectsInvalidContracts) {
        const TVector<std::function<void(NJson::TJsonValue&)>> mutations{
            [](auto& root) {
                root["service_abi_version"] = 1;
            },
            [](auto& root) {
                root["service_methods"][1]["id"] = 7;
            },
            [](auto& root) {
                root["service_methods"][1]["name"] = "Echo";
            },
            [](auto& root) {
                root["service_methods"][0]["input"][0]["type"] = "Optional<String>";
            },
            [](auto& root) {
                root["service_methods"][0]["input"][0]["max_bytes"] = 0;
            },
            [](auto& root) {
                root["service_methods"][0]["max_output_row_bytes"] = 1;
            },
            [](auto& root) {
                root["service_methods"][0]["max_batch_rows"] = 65;
            },
            [](auto& root) {
                root["service_methods"][0]["max_batch_rows"] = 0;
            },
            [](auto& root) {
                root["service_methods"][0]["batch"] = false;
            },
            [](auto& root) {
                root["service_methods"][0]["output"][1]["name"] = "value";
            },
            [](auto& root) {
                root["service_methods"][0]["name"] = "M15119";
                root["service_methods"][1]["name"] = "M203802";
            },
        };
        for (const auto& mutate : mutations) {
            NJson::TJsonValue root;
            UNIT_ASSERT(NJson::ReadJsonTree(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"), &root, true));
            mutate(root);
            UNIT_ASSERT_EXCEPTION(ParseServiceManifest(NJson::WriteJson(root, false)), yexception);
        }
    }

    Y_UNIT_TEST(ServiceRowWireBounds) {
        using namespace NYdb::NWasm::NServices;
        char buffer[128];
        TRowWriter writer(buffer, sizeof(buffer));
        UNIT_ASSERT(writer.Put(ui64(Max<ui64>())));
        UNIT_ASSERT(writer.Put(i64(-7)));
        UNIT_ASSERT(writer.String(std::string_view("a\0b", 3)));
        UNIT_ASSERT(writer.String({}));
        TRowReader reader({ buffer, writer.Size() });
        ui64 unsignedValue;
        i64 signedValue;
        std::string_view text;
        UNIT_ASSERT(reader.Get(unsignedValue) && unsignedValue == Max<ui64>());
        UNIT_ASSERT(reader.Get(signedValue) && signedValue == -7);
        UNIT_ASSERT(reader.String(text, 3) && text == std::string_view("a\0b", 3));
        UNIT_ASSERT(reader.String(text, 3) && text.empty());
        UNIT_ASSERT(reader.Remaining().empty());
        UNIT_ASSERT(!reader.Get(unsignedValue));
        TRowReader truncated({ buffer, 1 });
        UNIT_ASSERT(!truncated.Get(unsignedValue));
        TRowWriter shortWriter(buffer, 3);
        UNIT_ASSERT(!shortWriter.String("x"));
        TServiceRequest header;
        header.Count = 1;
        auto bytes = Encode(header, std::string_view(buffer, 8));
        std::string_view rows;
        UNIT_ASSERT(ReadServiceRequest(bytes, header, rows));
        header.Count = MaxServiceBatchRows + 1;
        UNIT_ASSERT(!ReadServiceRequest(Encode(header), header, rows));
    }
}
