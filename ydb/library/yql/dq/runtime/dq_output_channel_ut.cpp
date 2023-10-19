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

enum EChannelWidth {
    NARROW_CHANNEL,
    WIDE_CHANNEL,
};

struct TTestContext {
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder Vb;
    NDqProto::EDataTransportVersion TransportVersion;
    bool IsWide;
    TDqDataSerializer Ds;
    TStructType* OutputType = nullptr;
    TMultiType* WideOutputType = nullptr;

    TTestContext(EChannelWidth width = NARROW_CHANNEL, NDqProto::EDataTransportVersion transportVersion = NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, bool bigRows = false)
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("Mem")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , Vb(HolderFactory)
        , TransportVersion(transportVersion)
        , IsWide(width == WIDE_CHANNEL)
        , Ds(TypeEnv, HolderFactory, TransportVersion)
    {
        //TMultiType::Create(ui32 elementsCount, TType *const *elements, const TTypeEnvironment &env)
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

        TVector<TType*> components;
        for (ui32 i = 0; i < OutputType->GetMembersCount(); ++i) {
            components.push_back(OutputType->GetMemberType(i));
        }
        WideOutputType = TMultiType::Create(components.size(), components.data(), TypeEnv);
    }

    TUnboxedValueBatch CreateRow(ui32 value) {
        if (IsWide) {
            TUnboxedValueBatch result(WideOutputType);
            result.PushRow([&](ui32 idx) {
                if (idx == 0) {
                    return NUdf::TUnboxedValuePod(value);
                } else if (idx == 1) {
                    return NUdf::TUnboxedValuePod((ui64)(value * value));
                }
                return NMiniKQL::MakeString("***");
            });
            return result;
        }
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(OutputType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
        items[1] = NUdf::TUnboxedValuePod((ui64) (value * value));
        if (OutputType->GetMembersCount() == 3) {
            items[2] = NMiniKQL::MakeString("***");
        }
        TUnboxedValueBatch result(OutputType);
        result.emplace_back(std::move(row));
        return result;
    }

    TUnboxedValueBatch CreateBigRow(ui32 value, ui32 size) {
        if (IsWide) {
            TUnboxedValueBatch result(WideOutputType);
            result.PushRow([&](ui32 idx) {
                if (idx == 0) {
                    return NUdf::TUnboxedValuePod(value);
                } else if (idx == 1) {
                    return NUdf::TUnboxedValuePod((ui64)(value * value));
                }
                return NMiniKQL::MakeString(std::string(size, '*'));
            });
            return result;
        }
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(OutputType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
        items[1] = NUdf::TUnboxedValuePod((ui64) (value * value));
        if (OutputType->GetMembersCount() == 3) {
            items[2] = NMiniKQL::MakeString(std::string(size, '*'));
        }
        TUnboxedValueBatch result(OutputType);
        result.emplace_back(std::move(row));
        return result;
    }

    TType* GetOutputType() const {
        if (IsWide) {
            return WideOutputType;
        }
        return OutputType;
    }

    ui32 Width() const {
        if (IsWide) {
            return WideOutputType->GetElementsCount();
        }
        return 1u;
    }
};

void ValidateBatch(const TTestContext& ctx, const TUnboxedValueBatch& batch, ui32 startIndex, size_t expectedBatchSize) {
    UNIT_ASSERT_VALUES_EQUAL(expectedBatchSize, batch.RowCount());
    ui32 i = 0;
    if (ctx.IsWide) {
        batch.ForEachRowWide([&](const NUdf::TUnboxedValue* values, ui32 width) {
            ui32 j = i + startIndex;
            UNIT_ASSERT_VALUES_EQUAL(width, ctx.Width());
            UNIT_ASSERT_VALUES_EQUAL(j, values[0].Get<i32>());
            UNIT_ASSERT_VALUES_EQUAL(j * j, values[1].Get<ui64>());
            ++i;
        });
    } else {
        batch.ForEachRow([&](const NUdf::TUnboxedValue& value) {
            ui32 j = i + startIndex;
            UNIT_ASSERT_VALUES_EQUAL(j, value.GetElement(0).Get<i32>());
            UNIT_ASSERT_VALUES_EQUAL(j * j, value.GetElement(1).Get<ui64>());
            ++i;
        });
    }
    UNIT_ASSERT_VALUES_EQUAL(expectedBatchSize, i);
}

void PushRow(const TTestContext& ctx, TUnboxedValueBatch&& row, const IDqOutputChannel::TPtr& ch) {
    auto* values = row.Head();
    if (ctx.IsWide) {
        ch->WidePush(values, *row.Width());
    } else {
        ch->Push(std::move(*values));
    }
}

void TestSingleRead(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 1000;
    settings.MaxChunkBytes = 200;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 10; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetPushStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    TDqSerializedBatch data;
    UNIT_ASSERT(ch->Pop(data));

    UNIT_ASSERT_VALUES_EQUAL(10, data.RowCount());

    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetPushStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(1, ch->GetPopStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetPopStats().Rows);

    TUnboxedValueBatch buffer(ctx.GetOutputType());
    ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);

    ValidateBatch(ctx, buffer, 0, 10);
    data.Clear();
    UNIT_ASSERT(!ch->Pop(data));
}

void TestPartialRead(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 1000;
    settings.MaxChunkBytes = 17;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 9; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(9, ch->GetPushStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(9, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    int req = 0;
    ui32 expected[] = {2, 2, 2, 2, 1};

    ui32 readChunks = 0;
    ui32 readRows = 0;
    while (readRows < 9) {
        TDqSerializedBatch data;
        UNIT_ASSERT(ch->Pop(data));
        const auto rowCount = data.RowCount();

        ui32 v = expected[req];
        ++req;

        UNIT_ASSERT_VALUES_EQUAL(v, rowCount);
        UNIT_ASSERT_VALUES_EQUAL(++readChunks, ch->GetPopStats().Chunks);
        UNIT_ASSERT_VALUES_EQUAL(9, ch->GetPushStats().Rows);
        UNIT_ASSERT_VALUES_EQUAL(readRows + rowCount, ch->GetPopStats().Rows);

        TUnboxedValueBatch buffer(ctx.GetOutputType());
        ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);
        ValidateBatch(ctx, buffer, readRows, rowCount);
        readRows += rowCount;
    }

    TDqSerializedBatch data;
    UNIT_ASSERT(!ch->Pop(data));
}

void TestOverflow(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 30;
    settings.MaxChunkBytes = 10;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 8; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(8, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    UNIT_ASSERT(ch->IsFull());
    try {
        auto row = ctx.CreateRow(100'500);
        PushRow(ctx, std::move(row), ch);
        UNIT_FAIL("");
    } catch (yexception& e) {
        UNIT_ASSERT(TString(e.what()).Contains("requirement !IsFull() failed"));
    }
}

void TestPopAll(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 1000;
    settings.MaxChunkBytes = 10;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 50; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(50, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    TDqSerializedBatch data;
    TUnboxedValueBatch buffer(ctx.GetOutputType());

    UNIT_ASSERT(ch->PopAll(data));

    UNIT_ASSERT_VALUES_EQUAL(50, data.RowCount());

    ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);
    ValidateBatch(ctx, buffer, 0, 50);
    data.Clear();
    UNIT_ASSERT(!ch->Pop(data));
}

void TestBigRow(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = std::numeric_limits<ui32>::max();
    settings.MaxChunkBytes = 2_MB;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    {
        auto row = ctx.CreateRow(1);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }
    {
        for (ui32 i = 2; i < 10; ++i) {
            auto row = ctx.CreateBigRow(i, 10_MB);
            UNIT_ASSERT(!ch->IsFull());
            PushRow(ctx, std::move(row), ch);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(9, ch->GetPushStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(9, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Chunks);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    {
        TDqSerializedBatch data;
        UNIT_ASSERT(ch->Pop(data));

        UNIT_ASSERT_VALUES_EQUAL(2, data.RowCount());
        UNIT_ASSERT_VALUES_EQUAL(1, ch->GetPopStats().Chunks);
        UNIT_ASSERT_VALUES_EQUAL(9, ch->GetPushStats().Rows);
        UNIT_ASSERT_VALUES_EQUAL(2, ch->GetPopStats().Rows);

        TUnboxedValueBatch buffer(ctx.GetOutputType());
        ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);

        UNIT_ASSERT_VALUES_EQUAL(2, buffer.RowCount());
        ui32 i = 1;

        if (ctx.IsWide) {
            buffer.ForEachRowWide([&](const NUdf::TUnboxedValue* values, ui32 width) {
                UNIT_ASSERT_VALUES_EQUAL(width, ctx.Width());
                UNIT_ASSERT_VALUES_EQUAL(i, values[0].Get<i32>());
                UNIT_ASSERT_VALUES_EQUAL(i * i, values[1].Get<ui64>());
                ++i;
            });
        } else {
            buffer.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                UNIT_ASSERT_VALUES_EQUAL(i, value.GetElement(0).Get<i32>());
                UNIT_ASSERT_VALUES_EQUAL(i * i, value.GetElement(1).Get<ui64>());
                ++i;
            });
        }
        UNIT_ASSERT_VALUES_EQUAL(3, i);
    }

    for (ui32 i = 3; i < 10; ++i) {
        TDqSerializedBatch data;
        UNIT_ASSERT(ch->Pop(data));

        UNIT_ASSERT_VALUES_EQUAL(1, data.RowCount());
        UNIT_ASSERT_VALUES_EQUAL(i - 1, ch->GetPopStats().Chunks);
        UNIT_ASSERT_VALUES_EQUAL(9, ch->GetPushStats().Rows);
        UNIT_ASSERT_VALUES_EQUAL(i, ch->GetPopStats().Rows);

        TUnboxedValueBatch buffer(ctx.GetOutputType());
        ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);

        UNIT_ASSERT_VALUES_EQUAL(1, buffer.RowCount());

        auto head = buffer.Head();
        if (ctx.IsWide) {
            UNIT_ASSERT_VALUES_EQUAL(i, head[0].Get<i32>());
            UNIT_ASSERT_VALUES_EQUAL(i * i, head[1].Get<ui64>());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(i, head->GetElement(0).Get<i32>());
            UNIT_ASSERT_VALUES_EQUAL(i * i, head->GetElement(1).Get<ui64>());
        }
    }

    TDqSerializedBatch data;
    UNIT_ASSERT(!ch->Pop(data));
}

void TestSpillWithMockStorage(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 100;
    settings.MaxChunkBytes = 20;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto storage = MakeIntrusive<TMockChannelStorage>(100'500ul);
    settings.ChannelStorage = storage;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 35; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(35, ch->GetValuesCount());

    UNIT_ASSERT_VALUES_EQUAL(35, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(18, ch->GetPopStats().SpilledRows);
    UNIT_ASSERT_VALUES_EQUAL(5, ch->GetPopStats().SpilledBlobs);
    UNIT_ASSERT(ch->GetPopStats().SpilledBytes > 5 * 8);

    ui32 loadedRows = 0;

    TDqSerializedBatch data;
    while (ch->Pop(data)) {
        const auto rowCount = data.RowCount();
        TUnboxedValueBatch buffer(ctx.GetOutputType());
        ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);
        ValidateBatch(ctx, buffer, loadedRows, rowCount);
        loadedRows += rowCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(35, loadedRows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetValuesCount());

    // in memory only
    {
        loadedRows = 0;

        for (i32 i = 100; i < 105; ++i) {
            auto row = ctx.CreateRow(i);
            UNIT_ASSERT(!ch->IsFull());
            PushRow(ctx, std::move(row), ch);
        }

        UNIT_ASSERT_VALUES_EQUAL(5, ch->GetValuesCount());

        TDqSerializedBatch data;
        while (ch->Pop(data)) {
            const auto rowCount = data.RowCount();
            TUnboxedValueBatch buffer(ctx.GetOutputType());
            ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);
            ValidateBatch(ctx, buffer, loadedRows + 100, rowCount);
            loadedRows += rowCount;
        }
        UNIT_ASSERT_VALUES_EQUAL(5, loadedRows);
        UNIT_ASSERT_VALUES_EQUAL(0, ch->GetValuesCount());
    }
}

void TestOverflowWithMockStorage(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 500;
    settings.MaxChunkBytes = 10;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto storage = MakeIntrusive<TMockChannelStorage>(500ul);
    settings.ChannelStorage = storage;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 42; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(42, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    // UNIT_ASSERT(ch->IsFull()); it can be false-negative with storage enabled
    try {
        PushRow(ctx, ctx.CreateBigRow(0, 100'500), ch);
        UNIT_FAIL("");
    } catch (yexception &e) {
        UNIT_ASSERT(TString(e.what()).Contains("Space limit exceeded"));
    }
}

void TestChunkSizeLimit(TTestContext& ctx) {
    TDqOutputChannelSettings settings;
    settings.MaxStoredBytes = 500;
    settings.MaxChunkBytes = 100;
    settings.ChunkSizeLimit = 100000;
    settings.Level = TCollectStatsLevel::Profile;
    settings.TransportVersion = ctx.TransportVersion;

    auto ch = CreateDqOutputChannel(1, 1000, ctx.GetOutputType(), ctx.HolderFactory, settings, Log);

    for (i32 i = 0; i < 10; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT(!ch->IsFull());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(10, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    try {
        PushRow(ctx, ctx.CreateBigRow(0, 100'500), ch);
        UNIT_FAIL("");
    } catch (const TDqOutputChannelChunkSizeLimitExceeded& e) {
        UNIT_ASSERT(TString(e.what()).Contains("Row data size is too big"));
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
    TTestContext ctx(NARROW_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBigRow(ctx);
}

Y_UNIT_TEST(ChunkSizeLimit) {
    TTestContext ctx(NARROW_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestChunkSizeLimit(ctx);
}

}

Y_UNIT_TEST_SUITE(DqOutputWideChannelTests) {

Y_UNIT_TEST(SingleRead) {
    TTestContext ctx(WIDE_CHANNEL);
    TestSingleRead(ctx);
}

Y_UNIT_TEST(PartialRead) {
    TTestContext ctx(WIDE_CHANNEL);
    TestPartialRead(ctx);
}

Y_UNIT_TEST(Overflow) {
    TTestContext ctx(WIDE_CHANNEL);
    TestOverflow(ctx);
}

Y_UNIT_TEST(PopAll) {
    TTestContext ctx(WIDE_CHANNEL);
    TestPopAll(ctx);
}

Y_UNIT_TEST(BigRow) {
    TTestContext ctx(WIDE_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBigRow(ctx);
}

Y_UNIT_TEST(ChunkSizeLimit) {
    TTestContext ctx(WIDE_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestChunkSizeLimit(ctx);
}

}

Y_UNIT_TEST_SUITE(DqOutputChannelWithStorageTests) {

Y_UNIT_TEST(Spill) {
    TTestContext ctx;
    TestSpillWithMockStorage(ctx);
}

Y_UNIT_TEST(Overflow) {
    TTestContext ctx(NARROW_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestOverflowWithMockStorage(ctx);
}

}

Y_UNIT_TEST_SUITE(DqOutputWideChannelWithStorageTests) {

Y_UNIT_TEST(Spill) {
    TTestContext ctx(WIDE_CHANNEL);
    TestSpillWithMockStorage(ctx);
}

Y_UNIT_TEST(Overflow) {
    TTestContext ctx(WIDE_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestOverflowWithMockStorage(ctx);
}

}
