#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/dq/runtime/ut/ut_helper.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NYql;
using namespace NYql::NDq;

namespace {

// #define DEBUG_LOGS

void Log(TStringBuf msg) {
#ifdef DEBUG_LOGS
    Cerr << msg << Endl;
#else
    Y_UNUSED(msg);
#endif
}

struct TTestContext {
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder Vb;
    NDqProto::EDataTransportVersion TransportVersion;
    TDqDataSerializer Ds;
    TStructType* OutputType = nullptr;

    TTestContext(NDqProto::EDataTransportVersion transportVersion = NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, bool bigRows = false)
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("Mem")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , Vb(HolderFactory)
        , TransportVersion(transportVersion)
        , Ds(TypeEnv, HolderFactory, TransportVersion)
    {
        if (bigRows) {
            TStructMember members[3] = {
                {"x", TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv)},
                {"y", TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv)},
                {"z", TDataType::Create(NUdf::TDataType<char*>::Id, TypeEnv)}
            };
            OutputType = TStructType::Create(3, members, TypeEnv);
        } else {
            TStructMember members[2] = {
                {"x", TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv)},
                {"y", TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv)}
            };
            OutputType = TStructType::Create(2, members, TypeEnv);
        }
    }

    NUdf::TUnboxedValue CreateRow(ui32 value) {
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(OutputType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
        items[1] = NUdf::TUnboxedValuePod((ui64) (value * value));
        if (OutputType->GetMembersCount() == 3) {
            items[2] = NMiniKQL::MakeString("***");
        }
        return row;
    }

    NUdf::TUnboxedValue CreateBigRow(ui32 value, ui32 size) {
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(OutputType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
        items[1] = NUdf::TUnboxedValuePod((ui64) (value * value));
        if (OutputType->GetMembersCount() == 3) {
            items[2] = NMiniKQL::MakeString(std::string(size, '*'));
        }
        return row;
    }
};

void TestSingleRead(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 1000;
    settings.MaxChunkBytes = 200;
    settings.CollectProfileStats = true;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, ctx.OutputType, ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 10; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        ch->Push(std::move(row));
    }

    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->Chunks);
    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->RowsOut);

    NDqProto::TData data;
    UNIT_ASSERT(ch->Pop(data));

    UNIT_ASSERT_VALUES_EQUAL(10, data.GetRows());
    UNIT_ASSERT_VALUES_EQUAL(1, ch->GetStats()->Chunks);
    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetStats()->RowsOut);

    TUnboxedValueVector buffer;
    ctx.Ds.Deserialize(data, ctx.OutputType, buffer);

    UNIT_ASSERT_VALUES_EQUAL(10, buffer.size());
    for (i32 i = 0; i < 10; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(i, buffer[i].GetElement(0).Get<i32>());
        UNIT_ASSERT_VALUES_EQUAL(i * i, buffer[i].GetElement(1).Get<ui64>());
    }

    data.Clear();
    UNIT_ASSERT(!ch->Pop(data));
}

void TestPartialRead(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 1000;
    settings.MaxChunkBytes = 17;
    settings.CollectProfileStats = true;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, ctx.OutputType, ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 9; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        ch->Push(std::move(row));
    }

    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->Chunks);
    UNIT_ASSERT_VALUES_EQUAL(9, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->RowsOut);

    int req = 0;
    ui32 expected[] = {2, 2, 2, 2, 1};

    ui32 readChunks = 0;
    ui32 readRows = 0;
    while (readRows < 9) {
        NDqProto::TData data;
        UNIT_ASSERT(ch->Pop(data));

        ui32 v = expected[req];
        ++req;

        UNIT_ASSERT_VALUES_EQUAL(v, data.GetRows());
        UNIT_ASSERT_VALUES_EQUAL(++readChunks, ch->GetStats()->Chunks);
        UNIT_ASSERT_VALUES_EQUAL(9, ch->GetStats()->RowsIn);
        UNIT_ASSERT_VALUES_EQUAL(readRows + data.GetRows(), ch->GetStats()->RowsOut);

        TUnboxedValueVector buffer;
        ctx.Ds.Deserialize(data, ctx.OutputType, buffer);

        UNIT_ASSERT_VALUES_EQUAL(data.GetRows(), buffer.size());
        for (ui32 i = 0; i < data.GetRows(); ++i) {
            ui32 j = readRows + i;
            UNIT_ASSERT_VALUES_EQUAL(j, buffer[i].GetElement(0).Get<i32>());
            UNIT_ASSERT_VALUES_EQUAL(j * j, buffer[i].GetElement(1).Get<ui64>());
        }

        readRows += data.GetRows();
    }

    NDqProto::TData data;
    UNIT_ASSERT(!ch->Pop(data));
}

void TestOverflow(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 30;
    settings.MaxChunkBytes = 10;
    settings.CollectProfileStats = true;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, ctx.OutputType, ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 8; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        ch->Push(std::move(row));
    }

    UNIT_ASSERT_VALUES_EQUAL(8, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->RowsOut);

    UNIT_ASSERT(ch->IsFull());
    try {
        auto row = ctx.CreateRow(100'500);
        ch->Push(std::move(row));
        UNIT_FAIL("");
    } catch (yexception& e) {
        UNIT_ASSERT(TString(e.what()).Contains("requirement !IsFull() failed"));
    }
}

void TestPopAll(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 1000;
    settings.MaxChunkBytes = 10;
    settings.CollectProfileStats = true;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, ctx.OutputType, ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 50; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        ch->Push(std::move(row));
    }

    UNIT_ASSERT_VALUES_EQUAL(50, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->RowsOut);

    NDqProto::TData data;
    TUnboxedValueVector buffer;

    UNIT_ASSERT(ch->PopAll(data));

    UNIT_ASSERT_VALUES_EQUAL(50, data.GetRows());

    ctx.Ds.Deserialize(data, ctx.OutputType, buffer);
    UNIT_ASSERT_VALUES_EQUAL(50, buffer.size());

    for (i32 i = 0; i < 50; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(i, buffer[i].GetElement(0).Get<i32>());
        UNIT_ASSERT_VALUES_EQUAL(i * i, buffer[i].GetElement(1).Get<ui64>());
    }

    data.Clear();
    UNIT_ASSERT(!ch->Pop(data));
}

void TestBigRow(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = std::numeric_limits<ui32>::max();
    settings.MaxChunkBytes = 2_MB;
    settings.CollectProfileStats = true;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, ctx.OutputType, ctx.HolderFactory, settings, Log);

    {
        auto row = ctx.CreateRow(1);
        UNIT_ASSERT(!ch->IsFull());
        ch->Push(std::move(row));
    }
    {
        for (ui32 i = 2; i < 10; ++i) {
            auto row = ctx.CreateBigRow(i, 10_MB);
            UNIT_ASSERT(!ch->IsFull());
            ch->Push(std::move(row));
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->Chunks);
    UNIT_ASSERT_VALUES_EQUAL(9, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->RowsOut);

    {
        NDqProto::TData data;
        UNIT_ASSERT(ch->Pop(data));

        UNIT_ASSERT_VALUES_EQUAL(2, data.GetRows());
        UNIT_ASSERT_VALUES_EQUAL(1, ch->GetStats()->Chunks);
        UNIT_ASSERT_VALUES_EQUAL(9, ch->GetStats()->RowsIn);
        UNIT_ASSERT_VALUES_EQUAL(2, ch->GetStats()->RowsOut);

        TUnboxedValueVector buffer;
        ctx.Ds.Deserialize(data, ctx.OutputType, buffer);

        UNIT_ASSERT_VALUES_EQUAL(2, buffer.size());
        for (ui32 i = 1; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, buffer[i - 1].GetElement(0).Get<i32>());
            UNIT_ASSERT_VALUES_EQUAL(i * i, buffer[i - 1].GetElement(1).Get<ui64>());
        }
    }

    for (ui32 i = 3; i < 10; ++i) {
        NDqProto::TData data;
        UNIT_ASSERT(ch->Pop(data));

        UNIT_ASSERT_VALUES_EQUAL(1, data.GetRows());
        UNIT_ASSERT_VALUES_EQUAL(i - 1, ch->GetStats()->Chunks);
        UNIT_ASSERT_VALUES_EQUAL(9, ch->GetStats()->RowsIn);
        UNIT_ASSERT_VALUES_EQUAL(i, ch->GetStats()->RowsOut);

        TUnboxedValueVector buffer;
        ctx.Ds.Deserialize(data, ctx.OutputType, buffer);

        UNIT_ASSERT_VALUES_EQUAL(1, buffer.size());
        UNIT_ASSERT_VALUES_EQUAL(i, buffer[0].GetElement(0).Get<i32>());
        UNIT_ASSERT_VALUES_EQUAL(i * i, buffer[0].GetElement(1).Get<ui64>());
    }

    NDqProto::TData data;
    UNIT_ASSERT(!ch->Pop(data));
}

void TestSpillWithMockStorage(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 100;
    settings.MaxChunkBytes = 20;
    settings.CollectProfileStats = true;
    settings.TransportVersion = ctx.TransportVersion;

    auto storage = MakeIntrusive<TMockChannelStorage>(100'500ul);
    settings.ChannelStorage = storage;

    auto ch = CreateDqOutputChannel(1, ctx.OutputType, ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 35; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        ch->Push(std::move(row));
    }

    UNIT_ASSERT_VALUES_EQUAL(35, ch->GetValuesCount());

    UNIT_ASSERT_VALUES_EQUAL(35, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->RowsOut);
    UNIT_ASSERT_VALUES_EQUAL(18, ch->GetStats()->SpilledRows);
    UNIT_ASSERT_VALUES_EQUAL(5, ch->GetStats()->SpilledBlobs);
    UNIT_ASSERT(ch->GetStats()->SpilledBytes > 5 * 8);

    ui32 loadedRows = 0;

    NDqProto::TData data;
    while (ch->Pop(data)) {
        TUnboxedValueVector buffer;
        ctx.Ds.Deserialize(data, ctx.OutputType, buffer);

        UNIT_ASSERT_VALUES_EQUAL(data.GetRows(), buffer.size());
        for (ui32 i = 0; i < data.GetRows(); ++i) {
            auto j = loadedRows + i;
            UNIT_ASSERT_VALUES_EQUAL(j, buffer[i].GetElement(0).Get<i32>());
            UNIT_ASSERT_VALUES_EQUAL(j * j, buffer[i].GetElement(1).Get<ui64>());
        }

        loadedRows += data.GetRows();
    }
    UNIT_ASSERT_VALUES_EQUAL(35, loadedRows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetValuesCount());

    // in memory only
    {
        loadedRows = 0;

        for (i32 i = 100; i < 105; ++i) {
            auto row = ctx.CreateRow(i);
            UNIT_ASSERT(!ch->IsFull());
            ch->Push(std::move(row));
        }

        UNIT_ASSERT_VALUES_EQUAL(5, ch->GetValuesCount());

        NDqProto::TData data;
        while (ch->Pop(data)) {
            TUnboxedValueVector buffer;
            ctx.Ds.Deserialize(data, ctx.OutputType, buffer);

            UNIT_ASSERT_VALUES_EQUAL(data.GetRows(), buffer.size());
            for (ui32 i = 0; i < data.GetRows(); ++i) {
                auto j = 100 + loadedRows + i;
                UNIT_ASSERT_VALUES_EQUAL(j, buffer[i].GetElement(0).Get<i32>());
                UNIT_ASSERT_VALUES_EQUAL(j * j, buffer[i].GetElement(1).Get<ui64>());
            }

            loadedRows += data.GetRows();
        }
        UNIT_ASSERT_VALUES_EQUAL(5, loadedRows);
        UNIT_ASSERT_VALUES_EQUAL(0, ch->GetValuesCount());
    }
}

void TestOverflowWithMockStorage(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 500;
    settings.MaxChunkBytes = 10;
    settings.CollectProfileStats = true;
    settings.TransportVersion = ctx.TransportVersion;

    auto storage = MakeIntrusive<TMockChannelStorage>(500ul);
    settings.ChannelStorage = storage;

    auto ch = CreateDqOutputChannel(1, ctx.OutputType, ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 42; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        ch->Push(std::move(row));
    }

    UNIT_ASSERT_VALUES_EQUAL(42, ch->GetStats()->RowsIn);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetStats()->RowsOut);

    // UNIT_ASSERT(ch->IsFull()); it can be false-negative with storage enabled
    try {
        ch->Push(ctx.CreateBigRow(0, 100'500));
        UNIT_FAIL("");
    } catch (yexception &e) {
        UNIT_ASSERT(TString(e.what()).Contains("Space limit exceeded"));
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(DqOutputChannelTests) {

Y_UNIT_TEST(SingleRead) {
    TTestContext ctx;
    TestSingleRead(ctx);
}

Y_UNIT_TEST(PartialRead) {
    TTestContext ctx;
    TestPartialRead(ctx);
}

Y_UNIT_TEST(Overflow) {
    TTestContext ctx;
    TestOverflow(ctx);
}

Y_UNIT_TEST(PopAll) {
    TTestContext ctx;
    TestPopAll(ctx);
}

Y_UNIT_TEST(BigRow) {
    TTestContext ctx(NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBigRow(ctx);
}

}

Y_UNIT_TEST_SUITE(DqOutputChannelWithStorageTests) {

Y_UNIT_TEST(Spill) {
    TTestContext ctx;
    TestSpillWithMockStorage(ctx);
}

Y_UNIT_TEST(Overflow) {
    TTestContext ctx(NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestOverflowWithMockStorage(ctx);
}

}
