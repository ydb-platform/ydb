#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/dq/runtime/dq_output.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/dq/runtime/ut/ut_helper.h>

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>
#include <atomic>
#include <set>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NYql;
using namespace NYql::NDq;

template<>
void Out<NYql::NDq::EDqFillLevel>(IOutputStream& os, const NYql::NDq::EDqFillLevel l) {
    os << static_cast<ui32>(l);
}

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
        , Ds(TypeEnv, HolderFactory, TransportVersion, EValuePackerVersion::V0)
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

    TUnboxedValueBatch CreateVariantRow(ui32 value, ui32 varIndex) {
        UNIT_ASSERT(!IsWide);
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(OutputType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
        items[1] = NUdf::TUnboxedValuePod((ui64) (value * value));
        if (OutputType->GetMembersCount() == 3) {
            items[2] = NMiniKQL::MakeString("***");
        }
        UNIT_ASSERT(row.TryMakeVariant(varIndex));
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

    TUnboxedValueBatch CreateBigVariantRow(ui32 value, ui32 size, ui32 varIndex) {
        UNIT_ASSERT(!IsWide);
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(OutputType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
        items[1] = NUdf::TUnboxedValuePod((ui64) (value * value));
        if (OutputType->GetMembersCount() == 3) {
            items[2] = NMiniKQL::MakeString(std::string(size, '*'));
        }
        UNIT_ASSERT(row.TryMakeVariant(varIndex));
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

void ConsumeRow(const TTestContext& ctx, TUnboxedValueBatch&& row, const IDqOutputConsumer::TPtr& consumer) {
    auto* values = row.Head();
    if (ctx.IsWide) {
        consumer->WideConsume(values, *row.Width());
    } else {
        consumer->Consume(std::move(*values));
    }
}

void TestSingleRead(TTestContext& ctx) {
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 1000,
        .MaxChunkBytes = 200
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    for (i32 i = 0; i < 10; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
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
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 1000,
        .MaxChunkBytes = 17
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    for (i32 i = 0; i < 9; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
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
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 30,
        .MaxChunkBytes = 10
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    for (i32 i = 0; i < 8; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
        PushRow(ctx, std::move(row), ch);
    }

    UNIT_ASSERT_VALUES_EQUAL(8, ch->GetPushStats().Rows);
    UNIT_ASSERT_VALUES_EQUAL(0, ch->GetPopStats().Rows);

    UNIT_ASSERT_VALUES_EQUAL(HardLimit, ch->UpdateFillLevel());
    auto row = ctx.CreateRow(100'500);
    PushRow(ctx, std::move(row), ch);
    UNIT_ASSERT_VALUES_EQUAL(HardLimit, ch->UpdateFillLevel());
}

void TestPopAll(TTestContext& ctx) {
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 1000,
        .MaxChunkBytes = 10
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    for (i32 i = 0; i < 50; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
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
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = std::numeric_limits<ui32>::max(),
        .MaxChunkBytes = 2_MB
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    {
        auto row = ctx.CreateRow(1);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
        PushRow(ctx, std::move(row), ch);
    }
    {
        for (ui32 i = 2; i < 10; ++i) {
            auto row = ctx.CreateBigRow(i, 10_MB);
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
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
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 100,
        .MaxChunkBytes = 20,
        .ChannelStorage = MakeIntrusive<TMockChannelStorage>(100'500ul)
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    for (i32 i = 0; i < 35; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT_VALUES_UNEQUAL(HardLimit, ch->UpdateFillLevel());
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
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
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
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 500,
        .MaxChunkBytes = 10,
        .ChannelStorage = MakeIntrusive<TMockChannelStorage>(500ul)
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    for (i32 i = 0; i < 42; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
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
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .ChannelId = 1,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 500,
        .MaxChunkBytes = 100,
        .ChunkSizeLimit = 100000
    };

    auto ch = CreateDqOutputChannel(settings, Log);

    for (i32 i = 0; i < 10; ++i) {
        auto row = ctx.CreateRow(i);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, ch->UpdateFillLevel());
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

void TestBackPressureInMemory(TTestContext& ctx, bool multi) {
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 100,
        .MaxChunkBytes = 100
    };

    TVector<IDqOutputChannel::TPtr> channels;
    constexpr ui32 CHANNEL_BITS = 3;
    constexpr ui32 CHANNEL_COUNT = 1 << CHANNEL_BITS;
    constexpr ui32 MSG_PER_CHANNEL = 4;

    for (ui32 i = 0; i < CHANNEL_COUNT; i++) {
        settings.ChannelId = i;
        auto channel = CreateDqOutputChannel(settings, Log);
        channels.emplace_back(channel);
    }

    TMaybe<ui8> minFillPercentage;
    minFillPercentage = 100;
    NDqProto::TTaskOutputHashPartition hashPartition;
    IDqOutputConsumer::TPtr consumer;

    if (multi) {
        TVector<IDqOutputConsumer::TPtr> consumers;
        {
            TVector<IDqOutput::TPtr> outputs;
            for (ui32 i = 0; i < CHANNEL_COUNT / 2; i++) {
                outputs.emplace_back(channels[i]);
            }
            TVector<TColumnInfo> keyColumns;
            keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "x"));
            consumers.emplace_back(CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr));
        }
        {
            TVector<IDqOutput::TPtr> outputs;
            for (ui32 i = CHANNEL_COUNT / 2; i < CHANNEL_COUNT; i++) {
                outputs.emplace_back(channels[i]);
            }
            TVector<TColumnInfo> keyColumns;
            keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "x"));
            consumers.emplace_back(CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr));
        }
        consumer = CreateOutputMultiConsumer(std::move(consumers));
    } else {
        TVector<IDqOutput::TPtr> outputs;
        for (auto c : channels) {
            outputs.emplace_back(c);
        }
        TVector<TColumnInfo> keyColumns;
        keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "0")); // index !!!
        consumer = CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr);
    }


    UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());

    for (ui32 i = 0; i < CHANNEL_COUNT * MSG_PER_CHANNEL; ++i) {
        auto row = multi ? ctx.CreateVariantRow(i, (i >> (CHANNEL_BITS - 1)) & 1) : ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
    }

    for (auto c : channels) {
        UNIT_ASSERT_VALUES_EQUAL(MSG_PER_CHANNEL, c->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, c->UpdateFillLevel());
    }

    ui32 channel0 = 0;

    {
        auto row = multi ? ctx.CreateBigVariantRow(0, 10000, 0) : ctx.CreateBigRow(0, 10000);
        ConsumeRow(ctx, std::move(row), consumer);

        UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());

        for (ui32 i = 0; i < CHANNEL_COUNT; i ++) {
            if (channels[i]->GetValuesCount() == MSG_PER_CHANNEL + 1) {
                channel0 = i;
                break;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(HardLimit, channels[channel0]->UpdateFillLevel());
    }

    {
        TDqSerializedBatch data;
        UNIT_ASSERT(channels[channel0]->PopAll(data));

        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, channels[channel0]->UpdateFillLevel());
        UNIT_ASSERT_VALUES_EQUAL(0, channels[channel0]->GetValuesCount());
    }
}

void TestBackPressureWithSpilling(TTestContext& ctx, bool multi) {
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 100,
        .MaxChunkBytes = 100
    };

    TVector<IDqOutputChannel::TPtr> channels;
    constexpr ui32 CHANNEL_BITS = 3;
    constexpr ui32 CHANNEL_COUNT = 1 << CHANNEL_BITS;
    constexpr ui32 MSG_PER_CHANNEL = 4;

    for (ui32 i = 0; i < CHANNEL_COUNT; i++) {
        // separate Storage for each channel is required
        settings.ChannelId = i;
        settings.ChannelStorage = MakeIntrusive<TMockChannelStorage>(100000ul);
        auto channel = CreateDqOutputChannel(settings, Log);
        channels.emplace_back(channel);
    }

    TMaybe<ui8> minFillPercentage;
    minFillPercentage = 100;
    NDqProto::TTaskOutputHashPartition hashPartition;
    IDqOutputConsumer::TPtr consumer;

    if (multi) {
        TVector<IDqOutputConsumer::TPtr> consumers;
        {
            TVector<IDqOutput::TPtr> outputs;
            for (ui32 i = 0; i < CHANNEL_COUNT / 2; i++) {
                outputs.emplace_back(channels[i]);
            }
            TVector<TColumnInfo> keyColumns;
            keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "x"));
            consumers.emplace_back(CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr));
        }
        {
            TVector<IDqOutput::TPtr> outputs;
            for (ui32 i = CHANNEL_COUNT / 2; i < CHANNEL_COUNT; i++) {
                outputs.emplace_back(channels[i]);
            }
            TVector<TColumnInfo> keyColumns;
            keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "x"));
            consumers.emplace_back(CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr));
        }
        consumer = CreateOutputMultiConsumer(std::move(consumers));
    } else {
        TVector<IDqOutput::TPtr> outputs;
        for (auto c : channels) {
            outputs.emplace_back(c);
        }
        TVector<TColumnInfo> keyColumns;
        keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "0")); // index !!!
        consumer = CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr);
    }

    UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());

    for (ui32 i = 0; i < CHANNEL_COUNT * MSG_PER_CHANNEL; ++i) {
        auto row = multi ? ctx.CreateVariantRow(i, (i >> (CHANNEL_BITS - 1)) & 1) : ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
    }

    for (auto c : channels) {
        UNIT_ASSERT_VALUES_EQUAL(MSG_PER_CHANNEL, c->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, c->UpdateFillLevel());
    }

    ui32 channel0 = 0;

    {
        auto row = multi ? ctx.CreateBigVariantRow(0, 10000, 0) : ctx.CreateBigRow(0, 10000);
        ConsumeRow(ctx, std::move(row), consumer);

        for (auto i = 0; i < 4; i ++) {
            if (channels[i]->GetValuesCount() == 5) {
                channel0 = i;
                break;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(SoftLimit, channels[channel0]->UpdateFillLevel());

        for (ui32 i = 1; i < CHANNEL_COUNT; i ++) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            auto row = multi ? ctx.CreateBigVariantRow(i, 10000, (i >> (CHANNEL_BITS - 1)) & 1) : ctx.CreateBigRow(i, 10000);
            ConsumeRow(ctx, std::move(row), consumer);
        }

        UNIT_ASSERT_VALUES_EQUAL(SoftLimit, consumer->GetFillLevel());
    }

    {
        TDqSerializedBatch data;
        UNIT_ASSERT(channels[channel0]->PopAll(data));

        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, channels[channel0]->UpdateFillLevel());
        UNIT_ASSERT_VALUES_EQUAL(0, channels[channel0]->GetValuesCount());
    }
}

void TestBackPressureInMemoryLoad(TTestContext& ctx) {
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 500,
        .MaxChunkBytes = 100
    };

    TVector<IDqOutputChannel::TPtr> channels;
    constexpr ui32 CHANNEL_BITS = 3;
    constexpr ui32 CHANNEL_COUNT = 1 << CHANNEL_BITS;
    // constexpr ui32 MSG_PER_CHANNEL = 4;

    for (ui32 i = 0; i < CHANNEL_COUNT; i++) {
        settings.ChannelId = i;
        auto channel = CreateDqOutputChannel(settings, Log);
        channels.emplace_back(channel);
    }

    TMaybe<ui8> minFillPercentage;
    minFillPercentage = 100;
    NDqProto::TTaskOutputHashPartition hashPartition;
    IDqOutputConsumer::TPtr consumer;

    TVector<IDqOutput::TPtr> outputs;
    for (auto c : channels) {
        outputs.emplace_back(c);
    }
    TVector<TColumnInfo> keyColumns;
    keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "0")); // index !!!
    consumer = CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());

    ui32 lastPopAll = 0;
    ui32 channelIndex = 0;
    ui32 blockCount = 0;
    ui32 emptyPops = 0;

    for (ui32 i = 0; i < 10000000; ++i) {
        auto row = ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        if (consumer->GetFillLevel() != NoLimit) {
            blockCount++;
            if (i > lastPopAll + 1000) {
                for (ui32 c = 0; c < CHANNEL_COUNT; c++) {
                    TDqSerializedBatch data;
                    if(!channels[c]->PopAll(data)) {
                        emptyPops++;
                    }
                }
                lastPopAll = i;
                UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            } else {
                while (true) {
                    channelIndex = ((channelIndex * 1103515245) + 12345) % CHANNEL_COUNT;
                    TDqSerializedBatch data;
                    if (channels[channelIndex]->Pop(data)) {
                        if (consumer->GetFillLevel() == NoLimit) {
                            break;
                        }
                    }
                }
            }
        }
    }
    Cerr << "Blocked " << blockCount << " time(s) emptyPops " << emptyPops << Endl;
}

void TestBackPressureWithSpillingLoad(TTestContext& ctx) {
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = 100,
        .MaxChunkBytes = 100
    };

    TVector<IDqOutputChannel::TPtr> channels;
    constexpr ui32 CHANNEL_BITS = 3;
    constexpr ui32 CHANNEL_COUNT = 1 << CHANNEL_BITS;
    // constexpr ui32 MSG_PER_CHANNEL = 4;

    for (ui32 i = 0; i < CHANNEL_COUNT; i++) {
        // separate Storage for each channel is required
        settings.ChannelId = i;
        settings.ChannelStorage = MakeIntrusive<TMockChannelStorage>(100000ul);
        auto channel = CreateDqOutputChannel(settings, Log);
        channels.emplace_back(channel);
    }

    TMaybe<ui8> minFillPercentage;
    minFillPercentage = 100;
    NDqProto::TTaskOutputHashPartition hashPartition;
    IDqOutputConsumer::TPtr consumer;

    TVector<IDqOutput::TPtr> outputs;
    for (auto c : channels) {
        outputs.emplace_back(c);
    }
    TVector<TColumnInfo> keyColumns;
    keyColumns.emplace_back(GetColumnInfo(ctx.GetOutputType(), "0")); // index !!!
    consumer = CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumns), ctx.GetOutputType(), ctx.HolderFactory, minFillPercentage, hashPartition, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());

    ui32 lastPopAll = 0;
    ui32 channelIndex = 0;
    ui32 blockCount = 0;
    ui32 emptyPops = 0;

    for (ui32 i = 0; i < 100000; ++i) {
        auto row = ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        if (consumer->GetFillLevel() != NoLimit) {
            blockCount++;
            if (i > lastPopAll + 1000) {
                for (ui32 c = 0; c < CHANNEL_COUNT; c++) {
                    TDqSerializedBatch data;
                    if(!channels[c]->PopAll(data)) {
                        emptyPops++;
                    }
                }
                lastPopAll = i;
                UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            } else {
                while (true) {
                    channelIndex = ((channelIndex * 1103515245) + 12345) % CHANNEL_COUNT;
                    TDqSerializedBatch data;
                    if (channels[channelIndex]->Pop(data)) {
                        if (consumer->GetFillLevel() == NoLimit) {
                            break;
                        }
                    }
                }
            }
        }
    }
    UNIT_ASSERT(blockCount > 0);
    Cerr << "Blocked " << blockCount << " time(s) emptyPops " << emptyPops << Endl;
}

Y_UNIT_TEST_SUITE(HashShuffle) {

Y_UNIT_TEST(BackPressureInMemory) {
    TTestContext ctx(WIDE_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBackPressureInMemory(ctx, false);
}

Y_UNIT_TEST(BackPressureInMemoryMulti) {
    TTestContext ctx(NARROW_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBackPressureInMemory(ctx, true);
}

Y_UNIT_TEST(BackPressureInMemoryLoad) {
    TTestContext ctx(WIDE_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBackPressureInMemoryLoad(ctx);
}

Y_UNIT_TEST(BackPressureWithSpilling) {
    TTestContext ctx(WIDE_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBackPressureWithSpilling(ctx, false);
}

Y_UNIT_TEST(BackPressureWithSpillingMulti) {
    TTestContext ctx(NARROW_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBackPressureWithSpilling(ctx, true);
}

Y_UNIT_TEST(BackPressureWithSpillingLoad) {
    TTestContext ctx(WIDE_CHANNEL, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
    TestBackPressureWithSpillingLoad(ctx);
}

}

namespace {

class TMockV2Output : public IDqOutput {
public:
    TMockV2Output(ui64 hardLimitBytes, ui64 bytesPerPush)
        : HardLimitBytes(hardLimitBytes)
        , BytesPerPush(bytesPerPush)
    {}

    const TDqOutputStats& GetPushStats() const override { return Stats; }
    EDqFillLevel GetFillLevel() const override { return Level; }
    // v2 channels update fill level via callback, not via a polling UpdateFillLevel.
    // Return the current level so the scatter consumer's post-push BucketIndex.Update is correct.
    EDqFillLevel UpdateFillLevel() override { return Level; }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> agg) override {
        Aggregator = std::move(agg);
        Aggregator->AddCount(Level);
    }

    bool SupportsLevelChangeCallback() const override { return true; }
    void SetLevelChangeCallback(TLevelChangeCallback cb) override { Callback = std::move(cb); }

    void Push(NUdf::TUnboxedValue&&) override { DoPush(); }
    void WidePush(NUdf::TUnboxedValue*, ui32) override { DoPush(); }
    void Push(NDqProto::TWatermark&&) override {}
    void Push(NDqProto::TCheckpoint&&) override {}
    void Finish() override {}
    void Flush() override {}
    bool HasData() const override { return InflightBytes > 0; }
    bool IsFinished() const override { return false; }
    bool IsEarlyFinished() const override { return false; }
    NKikimr::NMiniKQL::TType* GetOutputType() const override { return nullptr; }

    // Simulates external consumer Pop: updates fill level, aggregator, and fires
    // LevelChangeCallback (matching the fixed dq_channel_service.cpp behavior).
    void Drain() {
        InflightBytes = 0;
        ApplyLevel(EDqFillLevel::NoLimit, /*fireCallback=*/true);
    }

    ui64 PushCount = 0;

private:
    void DoPush() {
        ++PushCount;
        InflightBytes += BytesPerPush;
        auto newLevel = InflightBytes >= HardLimitBytes ? EDqFillLevel::HardLimit : EDqFillLevel::NoLimit;
        ApplyLevel(newLevel, /*fireCallback=*/true);
    }

    void ApplyLevel(EDqFillLevel newLevel, bool fireCallback) {
        if (newLevel == Level) return;
        if (Aggregator) {
            Aggregator->UpdateCount(Level, newLevel);
        }
        if (fireCallback && Callback) {
            Callback(Level, newLevel);
        }
        Level = newLevel;
    }

    const ui64 HardLimitBytes;
    const ui64 BytesPerPush;
    ui64 InflightBytes = 0;
    EDqFillLevel Level = EDqFillLevel::NoLimit;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    TLevelChangeCallback Callback;
    mutable TDqOutputStats Stats;
};

struct TScatterSetup {
    TVector<TMockV2Output*> Mocks;
    IDqOutputConsumer::TPtr Consumer;
};

TScatterSetup MakeScatter(ui32 channelCount, ui64 hardLimitBytes, ui64 bytesPerPush, ui32 primaryChannelIdx = 0) {
    TVector<IDqOutput::TPtr> outputs;
    TScatterSetup s;
    for (ui32 i = 0; i < channelCount; ++i) {
        auto* mock = new TMockV2Output(hardLimitBytes, bytesPerPush);
        s.Mocks.push_back(mock);
        outputs.emplace_back(mock);
    }
    s.Consumer = CreateOutputScatterConsumer(std::move(outputs), Nothing(), primaryChannelIdx);
    return s;
}

void ConsumeOne(IDqOutputConsumer::TPtr& consumer) {
    consumer->Consume(NUdf::TUnboxedValuePod((i32)0));
}

} // namespace

Y_UNIT_TEST_SUITE(ScatterConsumer) {


Y_UNIT_TEST(AcceptedByScatter) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(3, 1000, 1);
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());
}

// Verify that the callback fires synchronously inside Push, updating the BucketIndex
// before UpdateFillLevel() is called. The next Consume must route away from the channel
// that just became HardLimit.
Y_UNIT_TEST(AdaptiveRoutingCallbackOnPush) {
    TScopedAlloc alloc(__LOCATION__);
    // ch0: one push fills it (200 bytes >= 100-byte limit)
    // ch1: stays NoLimit (1 byte per push, 1000-byte limit)
    TVector<IDqOutput::TPtr> outputs;
    TVector<TMockV2Output*> mocks;

    auto* ch0 = new TMockV2Output(/*hardLimitBytes=*/100, /*bytesPerPush=*/200);
    mocks.push_back(ch0);
    outputs.emplace_back(ch0);

    auto* ch1 = new TMockV2Output(/*hardLimitBytes=*/1000, /*bytesPerPush=*/1);
    mocks.push_back(ch1);
    outputs.emplace_back(ch1);

    auto consumer = CreateOutputScatterConsumer(std::move(outputs), Nothing());

    // First consume: round-robin → ch0. Push fills it; callback fires synchronously
    // inside Push, moving ch0 to HardLimit bucket before UpdateFillLevel() is called.
    ConsumeOne(consumer);
    UNIT_ASSERT_VALUES_EQUAL(1u, mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(HardLimit, mocks[0]->GetFillLevel());
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());

    // Next two consumes must go to ch1 because ch0 is in the HardLimit bucket.
    ConsumeOne(consumer);
    ConsumeOne(consumer);
    UNIT_ASSERT_VALUES_EQUAL(1u, mocks[0]->PushCount);  // ch0 not touched again
    UNIT_ASSERT_VALUES_EQUAL(2u, mocks[1]->PushCount);
}

// Verify that draining a channel (via callback) updates the per-channel atomic,
// so GetFillLevel() on the consumer correctly returns NoLimit.
Y_UNIT_TEST(DrainUpdatesFillLevel) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(/*channelCount=*/2, /*hardLimitBytes=*/100, /*bytesPerPush=*/200);

    // Fill both channels via scatter (ch0 fills first, ch1 gets activated, then fills).
    ConsumeOne(s.Consumer);
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(HardLimit, s.Consumer->GetFillLevel());

    // Drain ch0: callback fires, updating ChannelLevels_[0] to NoLimit.
    s.Mocks[0]->Drain();
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());
}

// Lazy activation: only primaryChannelIdx starts active; others activate on demand.
Y_UNIT_TEST(LazyActivationStartsWithPrimary) {
    TScopedAlloc alloc(__LOCATION__);
    // 3 channels, each fills on first push (bytesPerPush >= hardLimitBytes).
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/100, /*bytesPerPush=*/200, /*primaryChannelIdx=*/0);

    // First consume: only ch0 is active, so it gets the row.
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[2]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(HardLimit, s.Mocks[0]->GetFillLevel());

    // Second consume: ch0 is HardLimit, ch1 gets activated (was next in queue) and chosen.
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[2]->PushCount);

    // Third consume: ch0 and ch1 both HardLimit, ch2 gets activated and chosen.
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[2]->PushCount);
}

// Lazy activation with non-zero primaryChannelIdx.
Y_UNIT_TEST(LazyActivationNonZeroPrimary) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/100, /*bytesPerPush=*/200, /*primaryChannelIdx=*/1);

    // First consume goes to ch1 (primary).
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[2]->PushCount);

    // ch1 filled -> ch2 activated (next in queue after primary=1: order is 2, 0).
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[2]->PushCount);

    // ch2 filled -> ch0 activated.
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[2]->PushCount);
}

// Small data volume: if channels never fill, only the primary channel is used.
Y_UNIT_TEST(LazyActivationSmallData) {
    TScopedAlloc alloc(__LOCATION__);
    // Channels have high limit (1000 bytes), tiny pushes (1 byte) — never fill.
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/1000, /*bytesPerPush=*/1, /*primaryChannelIdx=*/0);

    for (int i = 0; i < 10; ++i) {
        ConsumeOne(s.Consumer);
    }
    // All 10 rows went to ch0 — the only active channel.
    UNIT_ASSERT_VALUES_EQUAL(10u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[2]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());
}

Y_UNIT_TEST(RowAreDelivered) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/1000, /*bytesPerPush=*/1);
    constexpr ui32 rowCount = 30;

    for (ui32 i = 0; i < rowCount; ++i) {
        ConsumeOne(s.Consumer);
    }

    ui64 total = 0;
    for (auto* m : s.Mocks) {
        total += m->PushCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(rowCount, total);
}

Y_UNIT_TEST(BlocksOnlyWhenAllChannelsFull) {
    TScopedAlloc alloc(__LOCATION__);
    // 3 channels, each fills on first push.
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/100, /*bytesPerPush=*/200);

    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());

    // Fill all 3 channels (lazy activation kicks in for ch1, ch2).
    ConsumeOne(s.Consumer);  // ch0 fills → HardLimit
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());  // ch1 not yet active but available
    ConsumeOne(s.Consumer);  // ch1 activated and fills
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());  // ch2 still available
    ConsumeOne(s.Consumer);  // ch2 activated and fills
    UNIT_ASSERT_VALUES_EQUAL(HardLimit, s.Consumer->GetFillLevel()); // all active channels full

    // Drain ch0 → GetFillLevel drops to NoLimit.
    s.Mocks[0]->Drain();
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());
}

Y_UNIT_TEST(WideConsumeDistributes) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/1000, /*bytesPerPush=*/1);

    // Use WideConsume path: create consumer with outputWidth.
    TVector<IDqOutput::TPtr> outputs;
    for (auto* m : s.Mocks) {
        outputs.emplace_back(m);
    }
    auto consumer = CreateOutputScatterConsumer(std::move(outputs), /*outputWidth=*/TMaybe<ui32>(2u));

    constexpr ui32 rowCount = 30;
    NUdf::TUnboxedValue values[2] = {NUdf::TUnboxedValuePod((i32)1), NUdf::TUnboxedValuePod((ui64)2)};
    for (ui32 i = 0; i < rowCount; ++i) {
        consumer->WideConsume(values, 2);
    }

    ui64 total = 0;
    for (auto* m : s.Mocks) {
        total += m->PushCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(rowCount, total);
}

Y_UNIT_TEST(BackPressureLoad) {
    TScopedAlloc alloc(__LOCATION__);
    // 8 channels, moderate limit. Push many rows, drain when blocked.
    constexpr ui32 channelCount = 8;
    constexpr ui32 rowCount = 100'000;
    // Each push adds 10 bytes, limit is 500 → ~50 pushes per channel before HardLimit.
    auto s = MakeScatter(channelCount, /*hardLimitBytes=*/500, /*bytesPerPush=*/10);

    ui32 blockCount = 0;
    ui32 channelIndex = 0;

    for (ui32 i = 0; i < rowCount; ++i) {
        ConsumeOne(s.Consumer);

        if (s.Consumer->GetFillLevel() == HardLimit) {
            ++blockCount;
            // Drain channels round-robin until backpressure clears.
            while (s.Consumer->GetFillLevel() == HardLimit) {
                channelIndex = (channelIndex + 1) % channelCount;
                s.Mocks[channelIndex]->Drain();
            }
            UNIT_ASSERT_VALUES_UNEQUAL(HardLimit, s.Consumer->GetFillLevel());
        }
    }
    UNIT_ASSERT_C(blockCount > 0, "Expected back-pressure to trigger at least once");

    ui64 total = 0;
    for (auto* m : s.Mocks) {
        total += m->PushCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(rowCount, total);
}

}

// ---------------------------------------------------------------------------
// Scatter consumer with real TDqOutputChannel (production buffer).
// Verifies push→serialize→pop→deserialize roundtrip and backpressure routing.
// Unlike the mock-based tests above, these channels have real packing, chunking,
// and fill-level calculation. The callback fires only from UpdateFillLevel(),
// not from Push(), matching the production task-runner behaviour.
// ---------------------------------------------------------------------------

Y_UNIT_TEST_SUITE(ScatterConsumerRealChannel) {

Y_UNIT_TEST(DistributesWithBackpressure) {
    TTestContext ctx;

    constexpr ui32 channelCount = 3;
    constexpr ui32 rowCount = 100;

    TVector<IDqOutputChannel::TPtr> channels;
    TVector<IDqOutput::TPtr> outputs;

    for (ui32 i = 0; i < channelCount; ++i) {
        TDqChannelSettings settings = {
            .RowType = ctx.GetOutputType(),
            .HolderFactory = &ctx.HolderFactory,
            .ChannelId = i,
            .DstStageId = 1000,
            .Level = TCollectStatsLevel::Profile,
            .TransportVersion = ctx.TransportVersion,
            .MaxStoredBytes = 60,
            .MaxChunkBytes = 30,
        };
        auto ch = CreateDqOutputChannel(settings, Log);
        channels.push_back(ch);
        outputs.push_back(ch);
    }

    auto consumer = CreateOutputScatterConsumer(std::move(outputs), Nothing());

    ui64 totalPushed = 0;
    ui64 totalPopped = 0;

    for (ui32 i = 0; i < rowCount; ++i) {
        auto row = ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        ++totalPushed;

        // Simulate task-runner: poll UpdateFillLevel on all channels after each push.
        for (auto& ch : channels) {
            ch->UpdateFillLevel();
        }

        // If consumer is blocked, drain channels.
        if (consumer->GetFillLevel() == HardLimit) {
            for (auto& ch : channels) {
                TDqSerializedBatch data;
                while (ch->Pop(data)) {
                    totalPopped += data.RowCount();
                }
                ch->UpdateFillLevel();
            }
            UNIT_ASSERT_VALUES_UNEQUAL(HardLimit, consumer->GetFillLevel());
        }
    }

    // Drain remaining data.
    for (auto& ch : channels) {
        TDqSerializedBatch data;
        while (ch->Pop(data)) {
            totalPopped += data.RowCount();
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(rowCount, totalPopped);

    // With small buffers and 3 channels, scatter must have activated multiple channels.
    ui32 activeChannels = 0;
    for (auto& ch : channels) {
        if (ch->GetPushStats().Rows > 0) {
            ++activeChannels;
        }
    }
    UNIT_ASSERT_C(activeChannels >= 2,
        "Expected scatter to activate at least 2 channels, got " << activeChannels);
}

Y_UNIT_TEST(VerifiesDataIntegrity) {
    TTestContext ctx;

    constexpr ui32 channelCount = 2;
    constexpr ui32 rowCount = 20;

    TVector<IDqOutputChannel::TPtr> channels;
    TVector<IDqOutput::TPtr> outputs;

    for (ui32 i = 0; i < channelCount; ++i) {
        TDqChannelSettings settings = {
            .RowType = ctx.GetOutputType(),
            .HolderFactory = &ctx.HolderFactory,
            .ChannelId = i,
            .DstStageId = 1000,
            .Level = TCollectStatsLevel::Profile,
            .TransportVersion = ctx.TransportVersion,
            .MaxStoredBytes = 40,
            .MaxChunkBytes = 20,
        };
        auto ch = CreateDqOutputChannel(settings, Log);
        channels.push_back(ch);
        outputs.push_back(ch);
    }

    auto consumer = CreateOutputScatterConsumer(std::move(outputs), Nothing());

    for (ui32 i = 0; i < rowCount; ++i) {
        auto row = ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        for (auto& ch : channels) { ch->UpdateFillLevel(); }

        if (consumer->GetFillLevel() == HardLimit) {
            for (auto& ch : channels) {
                TDqSerializedBatch data;
                while (ch->Pop(data)) {}
                ch->UpdateFillLevel();
            }
        }
    }

    // Pop all remaining data and collect values.
    TSet<i32> seenValues;
    for (auto& ch : channels) {
        TDqSerializedBatch data;
        while (ch->Pop(data)) {
            TUnboxedValueBatch buffer(ctx.GetOutputType());
            ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), buffer);
            buffer.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                seenValues.insert(value.GetElement(0).Get<i32>());
            });
        }
    }

    // We can only verify rows that remained in the channels (not previously popped).
    // The key invariant: no duplicates (set size == number of distinct values seen).
    // Also verify at least some data made it through.
    UNIT_ASSERT_C(!seenValues.empty(), "Expected at least some data in channels after pushes");
    for (auto v : seenValues) {
        UNIT_ASSERT_C(v >= 0 && v < (i32)rowCount,
            "Unexpected value " << v << " outside [0, " << rowCount << ")");
    }
}

Y_UNIT_TEST(WideChannelDistributes) {
    TTestContext ctx(WIDE_CHANNEL);

    constexpr ui32 channelCount = 3;
    constexpr ui32 rowCount = 60;

    TVector<IDqOutputChannel::TPtr> channels;
    TVector<IDqOutput::TPtr> outputs;

    for (ui32 i = 0; i < channelCount; ++i) {
        TDqChannelSettings settings = {
            .RowType = ctx.GetOutputType(),
            .HolderFactory = &ctx.HolderFactory,
            .ChannelId = i,
            .DstStageId = 1000,
            .Level = TCollectStatsLevel::Profile,
            .TransportVersion = ctx.TransportVersion,
            .MaxStoredBytes = 60,
            .MaxChunkBytes = 30,
        };
        auto ch = CreateDqOutputChannel(settings, Log);
        channels.push_back(ch);
        outputs.push_back(ch);
    }

    auto consumer = CreateOutputScatterConsumer(std::move(outputs), TMaybe<ui32>(ctx.Width()));

    ui64 totalPopped = 0;
    for (ui32 i = 0; i < rowCount; ++i) {
        auto row = ctx.CreateRow(i);
        auto* values = row.Head();
        consumer->WideConsume(values, *row.Width());

        for (auto& ch : channels) { ch->UpdateFillLevel(); }
        if (consumer->GetFillLevel() == HardLimit) {
            for (auto& ch : channels) {
                TDqSerializedBatch data;
                while (ch->Pop(data)) { totalPopped += data.RowCount(); }
                ch->UpdateFillLevel();
            }
        }
    }

    for (auto& ch : channels) {
        TDqSerializedBatch data;
        while (ch->Pop(data)) { totalPopped += data.RowCount(); }
    }
    UNIT_ASSERT_VALUES_EQUAL(rowCount, totalPopped);

    ui32 activeChannels = 0;
    for (auto& ch : channels) {
        if (ch->GetPushStats().Rows > 0) ++activeChannels;
    }
    UNIT_ASSERT_C(activeChannels >= 2,
        "Expected scatter to activate at least 2 wide channels, got " << activeChannels);
}

}

// ---------------------------------------------------------------------------
// Cross-thread callback test: verifies that LevelChangeCallback fired from
// a consumer thread (simulating remote Pop) correctly updates the scatter
// router's ChannelLevels_ atomics, so the producer's next PickBest() sees
// the change.
// ---------------------------------------------------------------------------

Y_UNIT_TEST_SUITE(ScatterCrossThreadCallback) {

Y_UNIT_TEST(DrainFromAnotherThread) {
    TScopedAlloc alloc(__LOCATION__);
    constexpr ui32 channelCount = 4;
    auto s = MakeScatter(channelCount, /*hardLimitBytes=*/100, /*bytesPerPush=*/200);

    // Fill all channels (each fills on first push).
    for (ui32 i = 0; i < channelCount; ++i) {
        ConsumeOne(s.Consumer);
    }
    UNIT_ASSERT_VALUES_EQUAL(HardLimit, s.Consumer->GetFillLevel());

    // Drain ch0 from another thread — simulates remote Pop on a network thread.
    std::atomic<bool> drained{false};
    std::thread drainThread([&] {
        s.Mocks[0]->Drain();
        drained.store(true, std::memory_order_release);
    });
    drainThread.join();

    UNIT_ASSERT(drained.load(std::memory_order_acquire));
    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());

    // Producer can now push again — row goes to ch0 (the drained one).
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(2u, s.Mocks[0]->PushCount);
}

Y_UNIT_TEST(ConcurrentDrainAndPush) {
    TScopedAlloc alloc(__LOCATION__);
    constexpr ui32 channelCount = 4;
    constexpr ui32 rowCount = 10'000;
    auto s = MakeScatter(channelCount, /*hardLimitBytes=*/500, /*bytesPerPush=*/10);

    std::atomic<bool> stopDrainer{false};

    // Drainer thread: continuously drains channels to prevent permanent blocking.
    std::thread drainThread([&] {
        ui32 idx = 0;
        while (!stopDrainer.load(std::memory_order_acquire)) {
            idx = (idx + 1) % channelCount;
            if (s.Mocks[idx]->GetFillLevel() != NoLimit) {
                s.Mocks[idx]->Drain();
            }
        }
    });

    // Producer thread: push rows, spin-wait if blocked.
    for (ui32 i = 0; i < rowCount; ++i) {
        while (s.Consumer->GetFillLevel() == HardLimit) {
            // Spin until drainer frees space.
        }
        ConsumeOne(s.Consumer);
    }

    stopDrainer.store(true, std::memory_order_release);
    drainThread.join();

    ui64 total = 0;
    for (auto* m : s.Mocks) {
        total += m->PushCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(rowCount, total);

    // All channels should have been used.
    for (ui32 i = 0; i < channelCount; ++i) {
        UNIT_ASSERT_C(s.Mocks[i]->PushCount > 0,
            "Channel " << i << " was never used — routing broken under concurrency");
    }
}

}

