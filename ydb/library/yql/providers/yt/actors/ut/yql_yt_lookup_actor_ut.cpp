
#include <ydb/library/yql/providers/yt/actors/yql_yt_lookup_actor.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>

#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/system/tempfile.h>
#include <library/cpp/testing/unittest/registar.h>
#include <initializer_list>

using namespace NYql;
using namespace NActors;

Y_UNIT_TEST_SUITE(YtLookupActor) {

NUdf::TUnboxedValue CreateStructValue(NKikimr::NMiniKQL::THolderFactory& holderFactory, std::initializer_list<TStringBuf> members) {
    NUdf::TUnboxedValue* items;
    NUdf::TUnboxedValue result = holderFactory.CreateDirectArrayHolder(members.size(), items);
    for (size_t i = 0; i != members.size(); ++i) {
        items[i] = NKikimr::NMiniKQL::MakeString(*(members.begin() + i));
    }
    return result;
}

bool CheckStructValue(const NUdf::TUnboxedValue& v, std::initializer_list<TStringBuf> members) {
    for (size_t i = 0; i != members.size(); ++i) {
        NUdf::TUnboxedValue m = v.GetElement(i);
        if (m.AsStringRef() != *(members.begin() + i)) {
            return false;
        }
    }
    return true;
}

//Simple actor to call IDqAsyncLookupSource::AsyncLookup from an actor system's thread
class TCallLookupActor: public TActorBootstrapped<TCallLookupActor> {
public:
    TCallLookupActor(
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        NYql::NDq::IDqAsyncLookupSource* lookupSource,
        NKikimr::NMiniKQL::TUnboxedValueVector&& keysToLookUp)
        : Alloc(alloc)
        , LookupSource(lookupSource)
        , KeysToLookUp(std::move(keysToLookUp))
    {
    }

    void Bootstrap() {
        LookupSource->AsyncLookup(std::move(KeysToLookUp));
        auto guard = Guard(*Alloc);
        KeysToLookUp.clear();
        KeysToLookUp.shrink_to_fit();
    }

private:
    static constexpr char ActorName[] = "TEST";

private:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NYql::NDq::IDqAsyncLookupSource* LookupSource;
    NKikimr::NMiniKQL::TUnboxedValueVector KeysToLookUp;
};

Y_UNIT_TEST(Lookup) {
    auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> functionRegistry = CreateFunctionRegistry(NKikimr::NMiniKQL::IBuiltinFunctionRegistry::TPtr());
    NKikimr::NMiniKQL::TMemoryUsageInfo memUsage("TestMemUsage");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc->Ref(), memUsage);
    NKikimr::NMiniKQL::TTypeEnvironment typeEnv(*alloc);
    NKikimr::NMiniKQL::TTypeBuilder typeBuilder(typeEnv);

    auto loggerConfig = NYql::NProto::TLoggingConfig();
    loggerConfig.set_allcomponentslevel(::NYql::NProto::TLoggingConfig_ELevel::TLoggingConfig_ELevel_TRACE);
    NYql::NLog::InitLogger(loggerConfig, false);

    TTestActorRuntimeBase runtime;
    runtime.Initialize();
    auto edge = runtime.AllocateEdgeActor();

    NYql::NYt::NSource::TLookupSource source;
    source.SetCluster("Plato");
    source.SetTable("Lookup");
    source.SetRowSpec(R"(
{"_yql_row_spec"={
    "Type"=["StructType";[
        ["hostname";["DataType";"String"]];
        ["network";["DataType";"String"]];
        ["fqdn";["DataType";"String"]];
        ["ip4";["DataType";"String"]];
        ["ip6";["DataType";"String"]]
    ]];
}}
    )");

    NKikimr::NMiniKQL::TStructTypeBuilder keyTypeBuilder{typeEnv};
    keyTypeBuilder.Add("hostname", typeBuilder.NewDataType(NUdf::EDataSlot::String, false));
    keyTypeBuilder.Add("network", typeBuilder.NewDataType(NUdf::EDataSlot::String, false));
    NKikimr::NMiniKQL::TStructTypeBuilder payloadTypeBuilder{typeEnv};
    payloadTypeBuilder.Add("fqdn", typeBuilder.NewDataType(NUdf::EDataSlot::String, true));
    payloadTypeBuilder.Add("ip4", typeBuilder.NewDataType(NUdf::EDataSlot::String, true));

    TTempFileHandle lookupTable("lookup.txt");
    TString lookupTableData = R"(
{"hostname"="host1";"network"="vpc1";"fqdn"="host1.vpc1.net";"ip4"="192.168.1.1"; "ip6"="[xxxx:xxxx:xxxx:1111]"};
{"hostname"="host2";"network"="vpc1";"fqdn"="host2.vpc1.net";"ip4"="192.168.1.2"; "ip6"="[xxxx:xxxx:xxxx:2222]"};
{"hostname"="host1";"network"="vpc2";"fqdn"="host2.vpc2.net";"ip4"="192.168.2.1"; "ip6"="[xxxx:xxxx:xxxx:3333]"};
{"hostname"="very very long hostname to for test 1";"network"="vpc1";"fqdn"="very very long fqdn for test 1";"ip4"="192.168.100.1"; "ip6"="[xxxx:xxxx:XXXX:1111]"};
{"hostname"="very very long hostname to for test 2";"network"="vpc2";"fqdn"="very very long fqdn for test 2";"ip4"="192.168.100.2"; "ip6"="[xxxx:xxxx:XXXX:2222]"};
    )";
    lookupTable.Write(lookupTableData.data(), lookupTableData.size());
    const THashMap<TString, TString> mapping = {
            {"yt.Plato.Lookup", lookupTable.Name()}
    };
    auto ytServices = NFile::TYtFileServices::Make(
        nullptr, 
        mapping
    );
    auto guard = Guard(*alloc.get());
    auto [lookupSource, actor] = NYql::NDq::CreateYtLookupActor(
        ytServices,
        edge,
        alloc,
        *functionRegistry,
        std::move(source),
        keyTypeBuilder.Build(),
        payloadTypeBuilder.Build(),
        typeEnv,
        holderFactory,
        1'000'000);
    runtime.Register(actor);

    NKikimr::NMiniKQL::TUnboxedValueVector keys {\
        CreateStructValue(holderFactory, {"host1", "vpc1"}),
        CreateStructValue(holderFactory, {"host2", "vpc1"}),
        CreateStructValue(holderFactory, {"host2", "vpc2"}), //NOT_FOUND expected
        CreateStructValue(holderFactory, {"very very long hostname to for test 2", "vpc2"}),
    };

    guard.Release(); //let actors use alloc

    auto callLookupActor = new TCallLookupActor(alloc, lookupSource, std::move(keys));
    runtime.Register(callLookupActor);

    auto ev = runtime.GrabEdgeEventRethrow<NYql::NDq::IDqAsyncLookupSource::TEvLookupResult>(edge);
    auto guard2 = Guard(*alloc.get());
    NKikimr::NMiniKQL::TKeyPayloadPairVector lookupResult = std::move(ev->Get()->Data);
    UNIT_ASSERT_EQUAL(4, lookupResult.size());
    {
        auto& [k, v] = lookupResult[0];
        UNIT_ASSERT(CheckStructValue(k, {"host1", "vpc1"}));
        UNIT_ASSERT(CheckStructValue(v, {"host1.vpc1.net", "192.168.1.1"}));
    }
    {
        auto& [k, v] = lookupResult[1];
        UNIT_ASSERT(CheckStructValue(k, {"host2", "vpc1"}));
        UNIT_ASSERT(CheckStructValue(v, {"host2.vpc1.net", "192.168.1.2"}));
    }
    {
        auto& [k, v] = lookupResult[2];
        UNIT_ASSERT(CheckStructValue(k, {"host2", "vpc2"}));
        UNIT_ASSERT(!v);
    }
    {
        auto& [k, v] = lookupResult[3];
        UNIT_ASSERT(CheckStructValue(k, {"very very long hostname to for test 2", "vpc2"}));
        UNIT_ASSERT(CheckStructValue(v, {"very very long fqdn for test 2", "192.168.100.2"}));
    }
    
}

} //Y_UNIT_TEST_SUITE(GenericProviderLookupActor)