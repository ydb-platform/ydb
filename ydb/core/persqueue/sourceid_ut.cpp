#include "sourceid.h"

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/key.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NPQ {

Y_UNIT_TEST_SUITE(TSourceIdTests) {
    inline static TString TestSourceId(ui64 idx = 0) {
        return TStringBuilder() << "testSourceId" << idx;
    }

    inline static TStringBuf TestOwner(TStringBuf sourceId) {
        UNIT_ASSERT(sourceId.SkipPrefix("test"));
        return sourceId;
    }

    static constexpr ui32 TestPartition = 1;

    Y_UNIT_TEST(SourceIdWriterAddMessage) {
        TSourceIdWriter writer(ESourceIdFormat::Raw);

        const auto sourceId = TestSourceId(1);
        const auto sourceIdInfo = TSourceIdInfo(1, 10, TInstant::Seconds(100));

        writer.RegisterSourceId(sourceId, sourceIdInfo);
        UNIT_ASSERT_VALUES_EQUAL(writer.GetSourceIdsToWrite().size(), 1);

        {
            auto it = writer.GetSourceIdsToWrite().find(sourceId);
            UNIT_ASSERT_UNEQUAL(it, writer.GetSourceIdsToWrite().end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, sourceIdInfo);
        }

        const auto anotherSourceId = TestSourceId(2);
        const auto anotherSourceIdInfo = TSourceIdInfo(2, 20, TInstant::Seconds(200));
        UNIT_ASSERT_VALUES_UNEQUAL(sourceIdInfo, anotherSourceIdInfo);

        {
            auto it = writer.GetSourceIdsToWrite().find(anotherSourceId);
            UNIT_ASSERT_EQUAL(it, writer.GetSourceIdsToWrite().end());
        }
    }

    Y_UNIT_TEST(SourceIdWriterClean) {
        TSourceIdWriter writer(ESourceIdFormat::Raw);

        writer.RegisterSourceId(TestSourceId(), 1, 10, TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(writer.GetSourceIdsToWrite().size(), 1);

        writer.Clear();
        UNIT_ASSERT_VALUES_EQUAL(writer.GetSourceIdsToWrite().size(), 0);
    }

    Y_UNIT_TEST(SourceIdWriterFormCommand) {
        TSourceIdWriter writer(ESourceIdFormat::Raw);
        auto actualRequest = MakeHolder<TEvKeyValue::TEvRequest>();
        auto expectedRequest = MakeHolder<TEvKeyValue::TEvRequest>();

        const auto sourceId = TestSourceId(1);
        const auto sourceIdInfo = TSourceIdInfo(1, 10, TInstant::Seconds(100));
        writer.RegisterSourceId(sourceId, sourceIdInfo);
        UNIT_ASSERT_VALUES_EQUAL(writer.GetSourceIdsToWrite().size(), 1);
        {
            TKeyPrefix key(TKeyPrefix::TypeInfo, TestPartition, TKeyPrefix::MarkSourceId);
            TBuffer data;

            TSourceIdWriter::FillKeyAndData(ESourceIdFormat::Raw, sourceId, sourceIdInfo, key, data);
            auto write = expectedRequest.Get()->Record.AddCmdWrite();
            write->SetKey(key.Data(), key.Size());
            write->SetValue(data.Data(), data.Size());
            write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

            writer.FillRequest(actualRequest.Get(), TestPartition);
            UNIT_ASSERT_VALUES_EQUAL(actualRequest.Get()->ToString(), expectedRequest.Get()->ToString());
        }

        const auto anotherSourceId = TestSourceId(2);
        const auto anotherSourceIdInfo = TSourceIdInfo(2, 20, TInstant::Seconds(200));
        writer.RegisterSourceId(anotherSourceId, anotherSourceIdInfo);
        UNIT_ASSERT_VALUES_EQUAL(writer.GetSourceIdsToWrite().size(), 2);
        {
            TKeyPrefix key(TKeyPrefix::TypeInfo, TestPartition + 1, TKeyPrefix::MarkSourceId);
            TBuffer data;

            for (const auto& [sourceId, sourceIdInfo] : writer.GetSourceIdsToWrite()) {
                TSourceIdWriter::FillKeyAndData(ESourceIdFormat::Raw, sourceId, sourceIdInfo, key, data);
                auto write = expectedRequest.Get()->Record.AddCmdWrite();
                write->SetKey(key.Data(), key.Size());
                write->SetValue(data.Data(), data.Size());
                write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
            }

            writer.FillRequest(actualRequest.Get(), TestPartition + 1);
            UNIT_ASSERT_VALUES_EQUAL(actualRequest.Get()->ToString(), expectedRequest.Get()->ToString());
        }
    }

    Y_UNIT_TEST(SourceIdStorageAdd) {
        TSourceIdStorage storage;

        const auto sourceId = TestSourceId(1);
        const auto sourceIdInfo = TSourceIdInfo(1, 10, TInstant::Seconds(100));
        const auto anotherSourceId = TestSourceId(2);
        const auto anotherSourceIdInfo = TSourceIdInfo(2, 20, TInstant::Seconds(200));

        storage.RegisterSourceId(sourceId, sourceIdInfo);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetInMemorySourceIds().size(), 1);
        {
            auto it = storage.GetInMemorySourceIds().find(sourceId);
            UNIT_ASSERT_UNEQUAL(it, storage.GetInMemorySourceIds().end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, sourceIdInfo);
        }
        {
            auto it = storage.GetInMemorySourceIds().find(anotherSourceId);
            UNIT_ASSERT_EQUAL(it, storage.GetInMemorySourceIds().end());
        }

        storage.RegisterSourceId(anotherSourceId, anotherSourceIdInfo);
        storage.RegisterSourceId(sourceId, anotherSourceIdInfo);
        UNIT_ASSERT_VALUES_EQUAL(storage.GetInMemorySourceIds().size(), 2);
        {
            auto it = storage.GetInMemorySourceIds().find(sourceId);
            UNIT_ASSERT_UNEQUAL(it, storage.GetInMemorySourceIds().end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, anotherSourceIdInfo);
        }
    }

    void SourceIdStorageParseAndAdd(TKeyPrefix::EMark mark, ESourceIdFormat format) {
        const auto sourceId = TestSourceId();
        const auto sourceIdInfo = TSourceIdInfo(1, 10, TInstant::Seconds(100));

        TKeyPrefix ikey(TKeyPrefix::TypeInfo, TestPartition, mark);
        TBuffer idata;
        TSourceIdWriter::FillKeyAndData(format, sourceId, sourceIdInfo, ikey, idata);

        TString key;
        ikey.AsString(key);
        TString data;
        idata.AsString(data);

        TSourceIdStorage storage;
        storage.LoadSourceIdInfo(key, data, TInstant());

        auto it = storage.GetInMemorySourceIds().find(sourceId);
        UNIT_ASSERT_UNEQUAL(it, storage.GetInMemorySourceIds().end());
        UNIT_ASSERT_VALUES_EQUAL(it->second, sourceIdInfo);
    }

    Y_UNIT_TEST(SourceIdStorageParseAndAdd) {
        SourceIdStorageParseAndAdd(TKeyPrefix::MarkSourceId, ESourceIdFormat::Raw);
    }

    Y_UNIT_TEST(ProtoSourceIdStorageParseAndAdd) {
        SourceIdStorageParseAndAdd(TKeyPrefix::MarkProtoSourceId, ESourceIdFormat::Proto);
    }

    Y_UNIT_TEST(SourceIdStorageMinDS) {
        const auto now = TInstant::Now();
        TSourceIdStorage storage;

        const auto sourceId = TestSourceId(1);
        storage.RegisterSourceId(sourceId, 1, 10, TInstant::Seconds(100));
        {
            auto ds = storage.MinAvailableTimestamp(now);
            UNIT_ASSERT_VALUES_EQUAL(ds, TInstant::Seconds(100));
        }

        const auto anotherSourceId = TestSourceId(2);
        storage.RegisterSourceId(anotherSourceId, 2, 20, TInstant::Seconds(200));
        {
            auto ds = storage.MinAvailableTimestamp(now);
            UNIT_ASSERT_VALUES_EQUAL(ds, TInstant::Seconds(100));
        }

        storage.RegisterSourceId(sourceId, 3, 30, TInstant::Seconds(300));
        {
            auto ds = storage.MinAvailableTimestamp(now);
            UNIT_ASSERT_VALUES_EQUAL(ds, TInstant::Seconds(200));
        }
    }

    Y_UNIT_TEST(SourceIdStorageTestClean) {
        TSourceIdStorage storage;
        for (ui64 i = 1; i <= 10000; ++i) {
            storage.RegisterSourceId(TestSourceId(i), i, i, TInstant::Seconds(10 * i));
        }

        NKikimrPQ::TPartitionConfig config;
        config.SetSourceIdLifetimeSeconds(TDuration::Hours(1).Seconds());

        // sources are dropped before startOffset = 20
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(2), 20, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, true);
            UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2 * 19); // first 19 sources are dropped
        }

        // expired sources are dropped
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(2), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, true);
            UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2 * 341); // another 341 (360 - 19) sources are dropped
        }

        // move to the past
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(1), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, false); // nothing to drop (everything is dropped)
        }

        // move to the future
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(3), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, true);
            UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2 * 360); // more 360 sources are dropped
        }
    }

    Y_UNIT_TEST(SourceIdStorageDeleteByMaxCount) {
        TSourceIdStorage storage;
        for (ui64 i = 1; i <= 10000; ++i) {
            storage.RegisterSourceId(TestSourceId(i), i, i, TInstant::Seconds(10 * i));
        }

        NKikimrPQ::TPartitionConfig config;

        config.SetSourceIdMaxCounts(10000);
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(1), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, false);
        }

        config.SetSourceIdMaxCounts(9900); // decrease by 100
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(1), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, true);
            UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2 * 100); // 100 sources are dropped
        }
        {
            auto it = storage.GetInMemorySourceIds().find(TestSourceId(100)); // 100th source is dropped
            UNIT_ASSERT_EQUAL(it, storage.GetInMemorySourceIds().end());
        }
        {
            auto it = storage.GetInMemorySourceIds().find(TestSourceId(101)); // 101th source is alive
            UNIT_ASSERT_UNEQUAL(it, storage.GetInMemorySourceIds().end());
        }
    }

    Y_UNIT_TEST(SourceIdStorageComplexDelete) {
        TSourceIdStorage storage;
        for (ui64 i = 1; i <= 10000 + 1; ++i) { // add 10000 + one extra sources
            storage.RegisterSourceId(TestSourceId(i), i, i, TInstant::Seconds(10 * i));
        }

        NKikimrPQ::TPartitionConfig config;

        config.SetSourceIdLifetimeSeconds(TDuration::Hours(1).Seconds());
        config.SetSourceIdMaxCounts(10000); // limit to 10000
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(2), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, true);
            UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2 * 360); // first 360 sources are dropped
        }

        config.SetSourceIdLifetimeSeconds((TDuration::Hours(1) - TDuration::Minutes(1)).Seconds());
        config.SetSourceIdMaxCounts(10000 - 360);
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(2), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, true);
            UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2 * 6); // another 6 sources are dropped by retention
        }

        config.SetSourceIdMaxCounts(10000 - 370);
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(2), 10000, TestPartition, config);
            UNIT_ASSERT_EQUAL(dropped, true);
            UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2 * 5); // more 5 sources are dropped
        }
    }

    Y_UNIT_TEST(SourceIdStorageDeleteAndOwnersMark) {
        TSourceIdStorage storage;
        THashMap<TString, TOwnerInfo> owners;
        for (ui64 i = 1; i <= 2; ++i) { // add two sources
            const auto sourceId = TestSourceId(i);
            const auto owner = TestOwner(sourceId);

            storage.RegisterSourceId(sourceId, i, i, TInstant::Hours(i));
            storage.RegisterSourceIdOwner(sourceId, owner);
            owners[owner];
        }

        auto request = MakeHolder<TEvKeyValue::TEvRequest>();
        NKikimrPQ::TPartitionConfig config;
        config.SetSourceIdMaxCounts(1); // limit to one

        const auto dropped = storage.DropOldSourceIds(request.Get(), TInstant::Hours(1), 10000, TestPartition, config);
        UNIT_ASSERT_EQUAL(dropped, true);
        UNIT_ASSERT_VALUES_EQUAL(request.Get()->Record.CmdDeleteRangeSize(), 2); // first source is dropped by retention

        storage.MarkOwnersForDeletedSourceId(owners);
        {
            auto it = owners.find(TestOwner(TestSourceId(1)));
            UNIT_ASSERT_UNEQUAL(it, owners.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.SourceIdDeleted, true);
        }
        {
            auto it = owners.find(TestOwner(TestSourceId(2)));
            UNIT_ASSERT_UNEQUAL(it, owners.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.SourceIdDeleted, false);
        }
    }

} // TSourceIdTests

} // NPQ
} // NKikimr
