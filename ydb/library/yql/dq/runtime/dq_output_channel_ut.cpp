#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_channel_service_impl.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/dq/runtime/ut/ut_helper.h>

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

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
        , Ds(TypeEnv, HolderFactory, TransportVersion, EValuePackerVersion::V0, DefaultDatumTestValidationMode)
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

Y_UNIT_TEST_SUITE(Scatter) {

namespace {

TVector<IDqOutputChannel::TPtr> MakeChannels(TTestContext& ctx, ui32 count, ui64 maxStoredBytes = 1_MB, bool spilling = false) {
    TDqChannelSettings settings = {
        .RowType = ctx.GetOutputType(),
        .HolderFactory = &ctx.HolderFactory,
        .DstStageId = 1000,
        .Level = TCollectStatsLevel::Profile,
        .TransportVersion = ctx.TransportVersion,
        .MaxStoredBytes = maxStoredBytes,
        .MaxChunkBytes = 100
    };

    TVector<IDqOutputChannel::TPtr> channels;
    for (ui32 i = 0; i < count; ++i) {
        settings.ChannelId = i;
        if (spilling) {
            settings.ChannelStorage = MakeIntrusive<TMockChannelStorage>(1_MB);
        }
        channels.emplace_back(CreateDqOutputChannel(settings, Log));
    }
    return channels;
}

IDqOutputConsumer::TPtr MakeScatterConsumer(TTestContext& ctx, const TVector<IDqOutputChannel::TPtr>& channels) {
    TVector<IDqOutput::TPtr> outputs;
    for (auto c : channels) {
        outputs.emplace_back(c);
    }
    return CreateOutputScatterConsumer(std::move(outputs),
        ctx.IsWide ? TMaybe<ui32>(ctx.Width()) : TMaybe<ui32>());
}

TVector<ui32> DrainRows(TTestContext& ctx, const IDqOutputChannel::TPtr& channel) {
    TDqSerializedBatch data;
    UNIT_ASSERT(channel->PopAll(data));
    TUnboxedValueBatch batch(ctx.GetOutputType());
    ctx.Ds.Deserialize(std::move(data), ctx.GetOutputType(), batch);
    TVector<ui32> result;
    const auto append = [&](ui32 value, ui64 square) {
        UNIT_ASSERT_VALUES_EQUAL(square, ui64(value) * value);
        result.push_back(value);
    };
    if (ctx.IsWide) {
        batch.ForEachRowWide([&](const NUdf::TUnboxedValue* values, ui32 width) {
            UNIT_ASSERT_VALUES_EQUAL(width, ctx.Width());
            append(values[0].Get<ui32>(), values[1].Get<ui64>());
        });
    } else {
        batch.ForEachRow([&](const NUdf::TUnboxedValue& value) {
            append(value.GetElement(0).Get<ui32>(), value.GetElement(1).Get<ui64>());
        });
    }
    return result;
}

class TControlledChannelBuffer : public IChannelBuffer {
public:
    explicit TControlledChannelBuffer(const TChannelFullInfo& info)
        : IChannelBuffer(info)
    {}

    EDqFillLevel GetFillLevel() const override { return Level; }
    void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) override {
        Aggregator = std::move(aggregator);
        Aggregator->AddCount(Level);
    }
    void SetLevel(EDqFillLevel level) {
        if (Aggregator) {
            Aggregator->UpdateCount(Level, level);
        }
        Level = level;
    }
    void Push(TDataChunk&& data) override {
        Rows += data.Rows;
        FinishCount += data.Finished;
        CheckpointCount += data.Checkpoint.Defined();
        WatermarkCount += data.Watermark.Defined();
    }
    bool IsFinished() override { return false; }
    bool IsEarlyFinished() override { return false; }
    bool IsEmpty() override { return true; }
    bool Pop(TDataChunk&) override { return false; }
    void EarlyFinish() override {}
    void ExportPushStats(TDqAsyncStats& stats) override { stats.Rows = Rows; }
    void ExportPopStats(TDqAsyncStats&) override {}

    ui64 Rows = 0;
    ui32 FinishCount = 0;
    ui32 CheckpointCount = 0;
    ui32 WatermarkCount = 0;

private:
    EDqFillLevel Level = NoLimit;
    std::shared_ptr<TDqFillAggregator> Aggregator;
};

} // namespace

Y_UNIT_TEST(RowsSpreadEvenly) {
    TTestContext ctx(WIDE_CHANNEL);
    constexpr ui32 CHANNEL_COUNT = 4;
    constexpr ui32 ROWS_PER_CHANNEL = 5;

    auto channels = MakeChannels(ctx, CHANNEL_COUNT);
    auto consumer = MakeScatterConsumer(ctx, channels);

    for (ui32 i = 0; i < CHANNEL_COUNT * ROWS_PER_CHANNEL; ++i) {
        ConsumeRow(ctx, ctx.CreateRow(i), consumer);
    }

    for (auto c : channels) {
        UNIT_ASSERT_VALUES_EQUAL(ROWS_PER_CHANNEL, c->GetValuesCount());
    }
}

Y_UNIT_TEST(UnevenRowCountDiffersByOne) {
    TTestContext ctx(WIDE_CHANNEL);
    constexpr ui32 CHANNEL_COUNT = 4;
    constexpr ui32 ROWS = 4 * 3 + 2;

    auto channels = MakeChannels(ctx, CHANNEL_COUNT);
    auto consumer = MakeScatterConsumer(ctx, channels);

    for (ui32 i = 0; i < ROWS; ++i) {
        ConsumeRow(ctx, ctx.CreateRow(i), consumer);
    }

    ui32 total = 0;
    for (auto c : channels) {
        const auto count = c->GetValuesCount();
        UNIT_ASSERT_C(count == ROWS / CHANNEL_COUNT || count == ROWS / CHANNEL_COUNT + 1,
            "channel got " << count << " rows, expected " << (ROWS / CHANNEL_COUNT) << " or one more");
        total += count;
    }
    UNIT_ASSERT_VALUES_EQUAL(ROWS, total);
}

Y_UNIT_TEST(FullChannelIsBypassed) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 2, /* maxStoredBytes */ 100);
        auto consumer = MakeScatterConsumer(ctx, channels);

        ConsumeRow(ctx, ctx.CreateBigRow(0, 10000), consumer);
        UNIT_ASSERT_VALUES_EQUAL(HardLimit, channels[0]->UpdateFillLevel());
        for (ui32 row : {1, 2}) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateRow(row), consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, channels[0]->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(2, channels[1]->GetValuesCount());
        UNIT_ASSERT(DrainRows(ctx, channels[0]) == (TVector<ui32>{0}));
        UNIT_ASSERT(DrainRows(ctx, channels[1]) == (TVector<ui32>{1, 2}));
    }
}

Y_UNIT_TEST(AllFullBlocksUntilAnyChannelDrains) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 3, 100);
        auto consumer = MakeScatterConsumer(ctx, channels);
        for (ui32 row = 0; row < channels.size(); ++row) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateBigRow(row, 10000), consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());
        UNIT_ASSERT(DrainRows(ctx, channels[1]) == (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
        ConsumeRow(ctx, ctx.CreateRow(3), consumer);
        UNIT_ASSERT_VALUES_EQUAL(1, channels[0]->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(1, channels[2]->GetValuesCount());
        UNIT_ASSERT(DrainRows(ctx, channels[1]) == (TVector<ui32>{3}));
    }
}

Y_UNIT_TEST(WritableChannelsRemainRoundRobin) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 3, 1000);
        auto consumer = MakeScatterConsumer(ctx, channels);
        ConsumeRow(ctx, ctx.CreateBigRow(0, 10000), consumer);
        for (ui32 row = 1; row <= 12; ++row) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateRow(row), consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, channels[0]->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(6, channels[1]->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(6, channels[2]->GetValuesCount());
        UNIT_ASSERT(DrainRows(ctx, channels[0]) == (TVector<ui32>{0}));
        for (ui32 row = 13; row <= 15; ++row) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateRow(row), consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, channels[0]->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(7, channels[1]->GetValuesCount());
        UNIT_ASSERT_VALUES_EQUAL(7, channels[2]->GetValuesCount());
    }
}

Y_UNIT_TEST(SingleChannelKeepsBackpressure) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 1, 100);
        auto consumer = MakeScatterConsumer(ctx, channels);
        ConsumeRow(ctx, ctx.CreateBigRow(0, 10000), consumer);
        UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());
        UNIT_ASSERT(DrainRows(ctx, channels[0]) == (TVector<ui32>{0}));
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
        ConsumeRow(ctx, ctx.CreateRow(1), consumer);
        UNIT_ASSERT(DrainRows(ctx, channels[0]) == (TVector<ui32>{1}));
    }
}

Y_UNIT_TEST(PressureChangingAfterFillCheckPreservesFetchedRow) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 2, 100);
        auto consumer = MakeScatterConsumer(ctx, channels);
        PushRow(ctx, ctx.CreateBigRow(0, 10000), channels[0]);
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
        // Inject a pressure change between the task runner's check and Consume.
        PushRow(ctx, ctx.CreateBigRow(1, 10000), channels[1]);
        ConsumeRow(ctx, ctx.CreateRow(2), consumer);
        UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());
        UNIT_ASSERT(DrainRows(ctx, channels[0]) == (TVector<ui32>{0}));
        UNIT_ASSERT(DrainRows(ctx, channels[1]) == (TVector<ui32>{1, 2}));
    }
}

Y_UNIT_TEST(V2WaitsForBindingAndBypassesFullBoundChannel) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        for (bool local : {false, true}) {
            TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
            const TChannelFullInfo info(0, {}, {}, 0, 1, TCollectStatsLevel::Profile);
            auto ready = std::make_shared<TControlledChannelBuffer>(info);
            TDqChannelSettings settings = {
                .RowType = ctx.GetOutputType(),
                .HolderFactory = &ctx.HolderFactory,
                .MaxChunkBytes = 100
            };
            TVector<IDqOutputChannel::TPtr> channels;
            auto pending = MakeIntrusive<TFastDqOutputChannel>(std::weak_ptr<TDqChannelService>{}, settings,
                std::make_shared<TChannelStub>(info), local);
            channels.push_back(pending);
            channels.push_back(MakeIntrusive<TFastDqOutputChannel>(std::weak_ptr<TDqChannelService>{}, settings, ready, local));
            auto consumer = MakeScatterConsumer(ctx, channels);
            UNIT_ASSERT_VALUES_EQUAL(HardLimit, channels[0]->UpdateFillLevel());
            UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());
            UNIT_ASSERT_VALUES_EQUAL(pending->Aggregator->UnboundCount.load(), 1);

            auto bound = std::make_shared<TControlledChannelBuffer>(info);
            bound->SetLevel(HardLimit);
            // Mirror Bind's buffer replacement without creating channel transport.
            bound->SetFillAggregator(pending->Aggregator);
            pending->Serializer->Buffer = bound;
            UNIT_ASSERT_VALUES_EQUAL(pending->Aggregator->UnboundCount.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(pending->Aggregator->TotalCount.load(), 2);
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ready->SetLevel(HardLimit);
            ConsumeRow(ctx, ctx.CreateBigRow(0, 10000), consumer);
            consumer->Flush();
            UNIT_ASSERT_VALUES_EQUAL(ready->Rows, 1);
            UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());
            ready->SetLevel(NoLimit);
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateBigRow(1, 10000), consumer);
            consumer->Flush();
            UNIT_ASSERT_VALUES_EQUAL(ready->Rows, 2);
            UNIT_ASSERT_VALUES_EQUAL(bound->Rows, 0);

            NDqProto::TWatermark watermark;
            watermark.SetTimestampUs(12345);
            consumer->Consume(std::move(watermark));
            NDqProto::TCheckpoint checkpoint;
            checkpoint.SetId(42);
            consumer->Consume(std::move(checkpoint));
            consumer->Finish();
            for (const auto& buffer : {bound, ready}) {
                UNIT_ASSERT_VALUES_EQUAL(buffer->WatermarkCount, 1);
                UNIT_ASSERT_VALUES_EQUAL(buffer->CheckpointCount, 1);
                UNIT_ASSERT_VALUES_EQUAL(buffer->FinishCount, 1);
            }
        }
    }
}

Y_UNIT_TEST(SoftChannelIsSkippedWhenAnotherIsWritable) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 2, 100);
        channels[0] = MakeChannels(ctx, 1, 100, /* spilling */ true)[0];
        auto consumer = MakeScatterConsumer(ctx, channels);
        PushRow(ctx, ctx.CreateBigRow(0, 10000), channels[0]);
        UNIT_ASSERT_VALUES_EQUAL(SoftLimit, channels[0]->UpdateFillLevel());
        for (ui32 row : {1, 2}) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateRow(row), consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, channels[0]->GetValuesCount());
        ConsumeRow(ctx, ctx.CreateBigRow(3, 10000), consumer);
        UNIT_ASSERT_VALUES_EQUAL(HardLimit, channels[1]->UpdateFillLevel());
        UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());
        UNIT_ASSERT(DrainRows(ctx, channels[1]) == (TVector<ui32>{1, 2, 3}));
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
    }
}

Y_UNIT_TEST(AllSoftKeepsBackpressure) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 2, 100, /* spilling */ true);
        auto consumer = MakeScatterConsumer(ctx, channels);
        for (ui32 row = 0; row < channels.size(); ++row) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateBigRow(row, 10000), consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(SoftLimit, consumer->GetFillLevel());
        UNIT_ASSERT(DrainRows(ctx, channels[0]) == (TVector<ui32>{0}));
        UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
    }
}

Y_UNIT_TEST(RepeatedFillAndDrainDeliversEveryRowOnce) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 3, 100);
        auto consumer = MakeScatterConsumer(ctx, channels);
        TVector<ui32> received;
        for (ui32 row = 0; row < 90; ++row) {
            if (consumer->GetFillLevel() == HardLimit) {
                const auto drained = DrainRows(ctx, channels[(row / 3) % channels.size()]);
                received.insert(received.end(), drained.begin(), drained.end());
            }
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateBigRow(row, 10000), consumer);
        }
        for (auto& channel : channels) {
            const auto drained = DrainRows(ctx, channel);
            received.insert(received.end(), drained.begin(), drained.end());
        }
        std::sort(received.begin(), received.end());
        UNIT_ASSERT_VALUES_EQUAL(received.size(), 90);
        for (ui32 row = 0; row < received.size(); ++row) {
            UNIT_ASSERT_VALUES_EQUAL(received[row], row);
        }
    }
}

Y_UNIT_TEST(ControlMessagesGoToEveryChannel) {
    TTestContext ctx(WIDE_CHANNEL);
    constexpr ui32 CHANNEL_COUNT = 3;

    auto channels = MakeChannels(ctx, CHANNEL_COUNT);
    auto consumer = MakeScatterConsumer(ctx, channels);

    NDqProto::TWatermark watermark;
    watermark.SetTimestampUs(12345);
    consumer->Consume(std::move(watermark));

    for (auto c : channels) {
        NDqProto::TWatermark popped;
        UNIT_ASSERT_C(c->Pop(popped), "channel did not receive the watermark");
        UNIT_ASSERT_VALUES_EQUAL(12345, popped.GetTimestampUs());
    }
}

Y_UNIT_TEST(ControlMessagesAndFinishReachFullChannels) {
    for (auto width : {NARROW_CHANNEL, WIDE_CHANNEL}) {
        TTestContext ctx(width, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0, true);
        auto channels = MakeChannels(ctx, 3, 100);
        auto consumer = MakeScatterConsumer(ctx, channels);
        for (ui32 row = 0; row < channels.size(); ++row) {
            UNIT_ASSERT_VALUES_EQUAL(NoLimit, consumer->GetFillLevel());
            ConsumeRow(ctx, ctx.CreateBigRow(row, 10000), consumer);
        }
        UNIT_ASSERT_VALUES_EQUAL(HardLimit, consumer->GetFillLevel());
        NDqProto::TWatermark watermark;
        watermark.SetTimestampUs(12345);
        consumer->Consume(std::move(watermark));
        NDqProto::TCheckpoint checkpoint;
        checkpoint.SetId(42);
        consumer->Consume(std::move(checkpoint));
        consumer->Finish();
        for (ui32 i = 0; i < channels.size(); ++i) {
            UNIT_ASSERT(DrainRows(ctx, channels[i]) == (TVector<ui32>{i}));
            NDqProto::TWatermark poppedWatermark;
            UNIT_ASSERT(channels[i]->Pop(poppedWatermark));
            UNIT_ASSERT_VALUES_EQUAL(poppedWatermark.GetTimestampUs(), 12345);
            NDqProto::TCheckpoint poppedCheckpoint;
            UNIT_ASSERT(channels[i]->Pop(poppedCheckpoint));
            UNIT_ASSERT_VALUES_EQUAL(poppedCheckpoint.GetId(), 42);
            UNIT_ASSERT(channels[i]->IsFinished());
        }
    }
}

}
