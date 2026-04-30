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

#include <algorithm>
#include <util/generic/set.h>

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

class TMockOutput : public IDqOutput {
public:
    TMockOutput(ui64 hardLimitBytes, ui64 bytesPerPush)
        : HardLimitBytes(hardLimitBytes)
        , BytesPerPush(bytesPerPush)
    {}

    const TDqOutputStats& GetPushStats() const override { return Stats; }
    EDqFillLevel GetFillLevel() const override { return Level; }
    EDqFillLevel UpdateFillLevel() override { return Level; }

    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> agg) override {
        Aggregator = std::move(agg);
        Aggregator->AddCount(Level);
    }

    bool SupportsLevelChangeCallback() const override { return true; }
    void SetLevelChangeCallback(TLevelChangeCallback cb) override { Callback = std::move(cb); }

    void Push(NUdf::TUnboxedValue&&) override {
        if (Finished_) return;
        DoPush();
    }
    void WidePush(NUdf::TUnboxedValue*, ui32) override {
        if (Finished_) return;
        DoPush();
    }
    void Push(NDqProto::TWatermark&&) override { ++WatermarkCount; }
    void Push(NDqProto::TCheckpoint&&) override { ++CheckpointCount; }
    void Finish() override { Finished_ = true; }
    void Flush() override {}
    bool HasData() const override { return InflightBytes > 0; }
    bool IsFinished() const override { return Finished_; }
    bool IsEarlyFinished() const override { return false; }
    NKikimr::NMiniKQL::TType* GetOutputType() const override { return nullptr; }

    // Simulates external consumer Pop: updates fill level, aggregator, and fires LevelChangeCallback.
    void Drain() {
        InflightBytes = 0;
        ApplyLevel(EDqFillLevel::NoLimit, /*fireCallback=*/true);
    }

    // Simulates a downstream EarlyFinish on this channel: pin the fill level at
    // HardLimit so the scatter router avoids it (matches production behaviour —
    // see TLocalBuffer::EarlyFinish, which keeps FillLevel frozen to prevent
    // silent data drops).
    void PinHardLimit() {
        ApplyLevel(EDqFillLevel::HardLimit, /*fireCallback=*/true);
        Pinned = true;
    }

    ui64 PushCount = 0;
    ui64 CheckpointCount = 0;
    ui64 WatermarkCount = 0;

private:
    void DoPush() {
        ++PushCount;
        InflightBytes += BytesPerPush;
        if (Pinned) return;
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
    bool Pinned = false;
    bool Finished_ = false;
    std::shared_ptr<TDqFillAggregator> Aggregator;
    TLevelChangeCallback Callback;
    mutable TDqOutputStats Stats;
};

struct TScatterSetup {
    TVector<TMockOutput*> Mocks;
    IDqOutputConsumer::TPtr Consumer;
};

TScatterSetup MakeScatter(ui32 channelCount, ui64 hardLimitBytes, ui64 bytesPerPush, ui32 primaryChannelIdx = 0) {
    TVector<IDqOutput::TPtr> outputs;
    TScatterSetup s;
    for (ui32 i = 0; i < channelCount; ++i) {
        auto* mock = new TMockOutput(hardLimitBytes, bytesPerPush);
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


Y_UNIT_TEST(AdaptiveRoutingCallbackOnPush) {
    TScopedAlloc alloc(__LOCATION__);
    // ch0: one push fills it (200 bytes >= 100-byte limit)
    // ch1: stays NoLimit (1 byte per push, 1000-byte limit)
    TVector<IDqOutput::TPtr> outputs;
    TVector<TMockOutput*> mocks;

    auto* ch0 = new TMockOutput(/*hardLimitBytes=*/100, /*bytesPerPush=*/200);
    mocks.push_back(ch0);
    outputs.emplace_back(ch0);

    auto* ch1 = new TMockOutput(/*hardLimitBytes=*/1000, /*bytesPerPush=*/1);
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

// Lazy activation: only primaryChannelIdx starts active.
// — With high pressure (bytesPerPush > limit) each push fills the active channel
//   and triggers activation of the next one in cyclic order after primaryIdx.
// — With low pressure (bytesPerPush << limit) channels never fill, so only the
//   primary channel is ever used.
Y_UNIT_TEST(LazyActivation) {
    // High-pressure sub-case: verify cyclic activation order for two primary values.
    auto runHighPressure = [](ui32 primary, const TVector<ui32>& expectedOrder) {
        TScopedAlloc alloc(__LOCATION__);
        const ui32 channelCount = 3;
        auto s = MakeScatter(channelCount, /*hardLimitBytes=*/100, /*bytesPerPush=*/200, primary);

        TVector<ui32> order;
        for (ui32 i = 0; i < channelCount; ++i) {
            ConsumeOne(s.Consumer);
            for (ui32 j = 0; j < channelCount; ++j) {
                if (s.Mocks[j]->PushCount == 1 && std::find(order.begin(), order.end(), j) == order.end()) {
                    order.push_back(j);
                    break;
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(order.size(), expectedOrder.size(), "primary=" << primary);
        for (ui32 i = 0; i < order.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(order[i], expectedOrder[i],
                "primary=" << primary << " step=" << i);
        }
        for (ui32 j = 0; j < channelCount; ++j) {
            UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[j]->PushCount);
        }
    };
    runHighPressure(0, {0, 1, 2});
    runHighPressure(1, {1, 2, 0});

    {
        TScopedAlloc alloc(__LOCATION__);
        auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/1000, /*bytesPerPush=*/1, /*primaryChannelIdx=*/0);
        for (int i = 0; i < 10; ++i) {
            ConsumeOne(s.Consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(10u, s.Mocks[0]->PushCount);
        UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[1]->PushCount);
        UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[2]->PushCount);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());
    }
}


Y_UNIT_TEST(ControlMessagesBroadcast) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/1000, /*bytesPerPush=*/1);

    // Only ch0 gets a data row.
    ConsumeOne(s.Consumer);
    UNIT_ASSERT_VALUES_EQUAL(1u, s.Mocks[0]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[1]->PushCount);
    UNIT_ASSERT_VALUES_EQUAL(0u, s.Mocks[2]->PushCount);

    s.Consumer->Consume(NDqProto::TCheckpoint{});
    s.Consumer->Consume(NDqProto::TWatermark{});

    for (ui32 i = 0; i < 3; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(1u, s.Mocks[i]->CheckpointCount, "ch=" << i);
        UNIT_ASSERT_VALUES_EQUAL_C(1u, s.Mocks[i]->WatermarkCount, "ch=" << i);
    }
}


Y_UNIT_TEST(EarlyFinishedChannelAvoided) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/100, /*bytesPerPush=*/200);

    ConsumeOne(s.Consumer); // ch0 fills -> HardLimit
    ConsumeOne(s.Consumer); // activates ch1, fills -> HardLimit
    ConsumeOne(s.Consumer); // activates ch2, fills -> HardLimit
    for (auto* m : s.Mocks) { m->Drain(); }

    s.Mocks[0]->PinHardLimit();
    const ui64 ch0Before = s.Mocks[0]->PushCount;

    constexpr ui32 kRows = 30;
    for (ui32 i = 0; i < kRows; ++i) {
        ConsumeOne(s.Consumer);
        s.Mocks[1]->Drain();
        s.Mocks[2]->Drain();
    }

    UNIT_ASSERT_VALUES_EQUAL_C(ch0Before, s.Mocks[0]->PushCount,
        "ch0 was pinned at HardLimit but received " << (s.Mocks[0]->PushCount - ch0Before) << " extra rows");
    UNIT_ASSERT_VALUES_EQUAL(kRows + 3u,
        s.Mocks[0]->PushCount + s.Mocks[1]->PushCount + s.Mocks[2]->PushCount);
    UNIT_ASSERT_C(s.Mocks[1]->PushCount > 0 && s.Mocks[2]->PushCount > 0,
        "Healthy channels ch1/ch2 must share the load, got ch1="
        << s.Mocks[1]->PushCount << " ch2=" << s.Mocks[2]->PushCount);
}

Y_UNIT_TEST(HeterogeneousDrainSpeed) {
    TScopedAlloc alloc(__LOCATION__);
    constexpr ui32 channelCount = 3;
    // bytesPerPush > hardLimitBytes → a single push fills the channel.
    auto s = MakeScatter(channelCount, /*hardLimitBytes=*/10, /*bytesPerPush=*/20);

    // Activate all three channels up-front so the test doesn't race against
    // lazy activation.
    for (ui32 i = 0; i < 3; ++i) {
        ConsumeOne(s.Consumer);
    }
    for (auto* m : s.Mocks) { m->Drain(); }
    const ui64 baseline0 = s.Mocks[0]->PushCount;
    const ui64 baseline1 = s.Mocks[1]->PushCount;
    const ui64 baseline2 = s.Mocks[2]->PushCount;

    constexpr ui32 kIterations = 1000;
    for (ui32 i = 0; i < kIterations; ++i) {
        ConsumeOne(s.Consumer);
        s.Mocks[0]->Drain();                   // fast consumer: always NoLimit
        if (i % 10 == 9) {                     // slow consumers: drained rarely
            s.Mocks[1]->Drain();
            s.Mocks[2]->Drain();
        }
    }

    const ui64 ch0 = s.Mocks[0]->PushCount - baseline0;
    const ui64 ch1 = s.Mocks[1]->PushCount - baseline1;
    const ui64 ch2 = s.Mocks[2]->PushCount - baseline2;

    UNIT_ASSERT_VALUES_EQUAL(kIterations, ch0 + ch1 + ch2);
    UNIT_ASSERT_C(ch0 >= 5 * std::max(ch1, ch2),
        "Fast channel should dominate routing. Got ch0=" << ch0
        << " ch1=" << ch1 << " ch2=" << ch2);
}

Y_UNIT_TEST(BlocksOnlyWhenAllChannelsFull) {
    TScopedAlloc alloc(__LOCATION__);
    auto s = MakeScatter(/*channelCount=*/3, /*hardLimitBytes=*/100, /*bytesPerPush=*/200);

    UNIT_ASSERT_VALUES_EQUAL(NoLimit, s.Consumer->GetFillLevel());

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

}

// Scatter consumer with real TDqOutputChannel not the mock one.
// Verifies push→serialize→pop→deserialize roundtrip and backpressure routing.
Y_UNIT_TEST_SUITE(ScatterConsumerRealChannel) {

Y_UNIT_TEST(RealChannelBackpressure) {
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
    UNIT_ASSERT_VALUES_EQUAL(rowCount, totalPushed);

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

// One channel stops draining entirely, others works normally
Y_UNIT_TEST(StuckChannelAvoided) {
    TTestContext ctx;

    constexpr ui32 channelCount = 3;
    constexpr ui32 rowCount = 200;
    constexpr ui32 stuckIdx = 0;

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

    // Warmup a few rows so lazy activation brings all 3 channels up.
    for (ui32 i = 0; i < 3; ++i) {
        auto row = ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        for (auto& ch : channels) { ch->UpdateFillLevel(); }
    }
    for (ui32 i = 0; i < channelCount; ++i) {
        if (i == stuckIdx) continue;
        TDqSerializedBatch data;
        while (channels[i]->Pop(data)) {}
        channels[i]->UpdateFillLevel();
    }

    ui64 totalPopped = 0;
    for (ui32 i = 3; i < rowCount; ++i) {
        auto row = ctx.CreateRow(i);
        ConsumeRow(ctx, std::move(row), consumer);
        for (auto& ch : channels) { ch->UpdateFillLevel(); }

        if (consumer->GetFillLevel() == HardLimit) {
            // Only drain the healthy channels.
            for (ui32 j = 0; j < channelCount; ++j) {
                if (j == stuckIdx) continue;
                TDqSerializedBatch data;
                while (channels[j]->Pop(data)) {
                    totalPopped += data.RowCount();
                }
                channels[j]->UpdateFillLevel();
            }
            UNIT_ASSERT_VALUES_UNEQUAL_C(HardLimit, consumer->GetFillLevel(),
                "Scatter must not be blocked when a non-stuck channel has room");
        }
    }

    for (ui32 j = 0; j < channelCount; ++j) {
        if (j == stuckIdx) continue;
        TDqSerializedBatch data;
        while (channels[j]->Pop(data)) {
            totalPopped += data.RowCount();
        }
    }

    const ui64 stuckRows = channels[stuckIdx]->GetPushStats().Rows;
    UNIT_ASSERT_VALUES_EQUAL(rowCount, totalPopped + stuckRows);
    UNIT_ASSERT_C(stuckRows < rowCount / 2,
        "Stuck channel should have received only a small fraction of rows, got " << stuckRows);
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


