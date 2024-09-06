#include "keyvalue_storage_read_request.h"

#include <ydb/core/util/testactorsys.h>
#include <ydb/core/base/blobstorage_common.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace NKeyValue {

struct TBlobStorageMockState {
    struct TBlob {
        NKikimrProto::EReplyStatus Status = NKikimrProto::NODATA;
        TString Buffer;
    };

    struct TGroup {
        std::unordered_map<TLogoBlobID, TBlob, THash<TLogoBlobID>> Blobs;
        NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
        std::optional<ui32> GroupId;
        std::optional<ui32> Cookie;
    };

    std::unordered_map<ui32, TGroup> Groups;

    void Put(ui32 groupId, TLogoBlobID blobId, NKikimrProto::EReplyStatus status, const TString &buffer) {
        TBlob &blob = Groups[groupId].Blobs[blobId];
        blob.Status = status;
        blob.Buffer = buffer;
    }

    std::unique_ptr<TEvBlobStorage::TEvGetResult> MakeGetResult(ui32 groupId, TEvBlobStorage::TEvGet *get,
            std::function<std::pair<NKikimrProto::EReplyStatus, TString>(TLogoBlobID blobId)> getBlob)
    {
        TGroup &group = Groups[groupId];
        if (group.GroupId) {
            groupId = *group.GroupId;
        }
        std::unique_ptr<TEvBlobStorage::TEvGetResult> getResult = std::make_unique<TEvBlobStorage::TEvGetResult>(
                group.Status, get->QuerySize, TGroupId::FromValue(groupId));
        getResult->Responses.Reset(new TEvBlobStorage::TEvGetResult::TResponse[get->QuerySize]);
        for (ui32 queryIdx = 0; queryIdx < get->QuerySize; ++queryIdx) {
            auto &query = get->Queries[queryIdx];
            auto &response = getResult->Responses[queryIdx];
            response.Id = query.Id;
            response.Shift = query.Shift;
            response.RequestedSize = query.Size;

            TString r;
            std::tie(response.Status, r) = getBlob(query.Id);

            if (response.Status == NKikimrProto::OK) {
                const size_t shift = Min<size_t>(query.Shift, r.size());
                const size_t size = Min<size_t>(query.Size ? query.Size : Max<size_t>(), r.size() - shift);
                TString buffer = TString::Uninitialized(size);
                memcpy(buffer.Detach(), r.data() + shift, size);
                response.Buffer = TRope(std::move(buffer));
            }
        }
        return getResult;
    }

    std::unique_ptr<TEvBlobStorage::TEvGetResult> OnGet(ui32 groupId, TEvBlobStorage::TEvGet *get) {
        TGroup &group = Groups[groupId];

        if (group.Status != NKikimrProto::OK) {
            TString str("");
            return MakeGetResult(groupId, get, [&](...){ return std::make_pair(group.Status, str); });
        }

        return MakeGetResult(groupId, get, [&group](TLogoBlobID blobId) {
            TBlob &blob = group.Blobs[blobId];
            return std::make_pair(blob.Status, blob.Buffer);
        });
    }
};

struct TBlobStorageMock : TActorBootstrapped<TBlobStorageMock> {
    ui32 GroupId;
    TBlobStorageMockState *State;

    TBlobStorageMock(ui32 groupId, TBlobStorageMockState *state)
        : GroupId(groupId)
        , State(state)
    {}

    void Bootstrap() {
        Become(&TThis::Waiting);
    }

    void Handle(TEvBlobStorage::TEvGet::TPtr &ev) {
        std::unique_ptr<TEvBlobStorage::TEvGetResult> result = State->OnGet(GroupId, ev->Get());
        auto &group = State->Groups[GroupId];
        ui64 cookie = ev->Cookie;
        if (group.Cookie) {
            cookie = *group.Cookie;
        }
        Send(ev->Sender, result.release(), ev->Flags, cookie, std::move(ev->TraceId));
    }

    STATEFN(Waiting) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGet, Handle);
        default:
            Y_ABORT();
        }
   }
};


struct TTestEnv {
    TBlobStorageMockState BlobStorageState;

    std::unique_ptr<TTabletStorageInfo> TabletInfo;

    std::unordered_map<ui64, TActorId> GroupActors; // [groupId, actorId]

    TTestEnv()
        : TabletInfo(std::make_unique<TTabletStorageInfo>(1, TTabletTypes::KeyValue))
    {
    }

    void AddStorageGroup(TTestActorSystem &runtime, ui64 groupId) {
        if (GroupActors.count(groupId)) {
            return;
        }

        TActorId groupActor = runtime.Register(new TBlobStorageMock(groupId, &BlobStorageState), 1);
        TActorId proxyId = MakeBlobStorageProxyID(groupId);
        runtime.RegisterService(proxyId, groupActor);
        GroupActors[groupId] = groupActor;
    }

    void AddStorageGroups(TTestActorSystem &runtime, const std::vector<ui32> &groupIds) {
        for (ui32 groupId : groupIds) {
            AddStorageGroup(runtime, groupId);
        }
    }

    void BindGroupsToChannel(TTestActorSystem &runtime, const std::vector<ui32> &groupIds)
    {
        AddStorageGroups(runtime, groupIds);
        for (ui32 channelIdx = 0; channelIdx < groupIds.size(); ++channelIdx) {
            TabletInfo->Channels.emplace_back(channelIdx, TErasureType::Erasure4Plus2Block);
            TabletInfo->Channels.back().History.emplace_back(1, groupIds[channelIdx]);
        }
    }
};


struct TBuilderResult {
    THolder<TIntermediate> Intermediate;
    std::unordered_map<std::string, std::string> Values;
};


struct TReadItem {
    std::string Value;
    TLogoBlobID BlobId;
    ui32 Offset;
    ui32 Size;

    TReadItem(const std::string &value, TLogoBlobID blobId, ui32 offset, ui32 size)
        : Value(value)
        , BlobId(blobId)
        , Offset(offset)
        , Size(size)
    {}
};


struct TReadRequestBuilder {
    std::string Key;
    std::vector<TReadItem> Items;

    TReadRequestBuilder(const std::string &key)
        : Key(key)
    {}

    TReadRequestBuilder& AddToEnd(const std::string &partOfValue, TLogoBlobID blobId, ui32 offset, ui32 size) {
        Items.emplace_back(partOfValue, blobId, offset, size);
        return *this;
    }

    TBuilderResult Build(TActorId respondTo, TActorId keyValueActorId, ui32 channelGeneration = 1, ui32 channelStep = 1)
    {
        std::unique_ptr<TIntermediate> intermediate = std::make_unique<TIntermediate>(respondTo, keyValueActorId,
                channelGeneration, channelStep, TRequestType::ReadOnly, NWilson::TTraceId());
        TStringBuilder valueBuilder;
        for (auto &[value, blobId, offset, size] : Items) {
                valueBuilder << value;
        }
        std::string value = valueBuilder;


        intermediate->ReadCommand = TIntermediate::TRead(TString(Key), value.size(), 0,
                NKikimrClient::TKeyValueRequest::MAIN);
        TIntermediate::TRead &read = std::get<TIntermediate::TRead>(*intermediate->ReadCommand);
        ui32 valueOffset = 0;
        for (auto &[value, blobId, offset, size] : Items) {
            read.ReadItems.emplace_back(blobId, offset, size, valueOffset);
            valueOffset += size;
        }
        TBuilderResult res;
        res.Values[Key] = value;
        res.Intermediate.Reset(intermediate.release());
        return res;
    }
};


struct TRangeReadRequestBuilder {
    std::map<std::string, std::vector<TReadItem>> Reads;
    ui64 CmdLimitBytes = Max<ui64>();
    bool IncludeData = true;

    TRangeReadRequestBuilder()
    {}

    TRangeReadRequestBuilder& AddRead(const std::string &key, std::vector<TReadItem> &&items) {
        Reads[key] = std::move(items);
        return *this;
    }

    TRangeReadRequestBuilder& AddRead(const std::string &key, const std::string &value, TLogoBlobID blobId) {
        Reads[key] = {TReadItem(value, blobId, 0, value.size())};
        return *this;
    }

    TBuilderResult Build(TActorId respondTo, TActorId keyValueActorId, ui32 channelGeneration = 1, ui32 channelStep = 1)
    {
        std::unique_ptr<TIntermediate> intermediate = std::make_unique<TIntermediate>(respondTo, keyValueActorId,
                channelGeneration, channelStep, TRequestType::ReadOnly, NWilson::TTraceId());

        TBuilderResult res;
        intermediate->ReadCommand = TIntermediate::TRangeRead();
        auto &range = std::get<TIntermediate::TRangeRead>(*intermediate->ReadCommand);

        for (auto &[key, items] : Reads) {
            TStringBuilder valueBuilder;
            for (auto &[value, blobId, offset, size] : items) {
                    valueBuilder << value;
            }
            std::string value = valueBuilder;

            range.Reads.emplace_back(TString(key), value.size(), 0, NKikimrClient::TKeyValueRequest::MAIN);
            TIntermediate::TRead &read = range.Reads.back();
            ui32 valueOffset = 0;
            for (auto &[value, blobId, offset, size] : items) {
                read.ReadItems.emplace_back(blobId, offset, size, valueOffset);
                valueOffset += size;
            }
            res.Values[key] = value;
        }

        res.Intermediate.Reset(intermediate.release());
        return res;
    }
};


Y_UNIT_TEST_SUITE(KeyValueReadStorage) {

void RunTest(TTestEnv &env, TReadRequestBuilder &builder,
        const std::vector<ui32> &groupIds, NKikimrKeyValue::Statuses::ReplyStatus status = NKikimrKeyValue::Statuses::RSTATUS_OK) {
    TTestActorSystem runtime(1);
    runtime.Start();
    runtime.SetLogPriority(NKikimrServices::KEYVALUE, NLog::PRI_DEBUG);
    env.BindGroupsToChannel(runtime, groupIds);

    TActorId edgeActor = runtime.AllocateEdgeActor(1);
    auto [intermediate, expectedValues] = builder.Build(edgeActor, edgeActor, 1, 1);

    runtime.Register(CreateKeyValueStorageReadRequest(std::move(intermediate), env.TabletInfo.release(), 1), 1);

    std::unique_ptr<IEventHandle> ev = runtime.WaitForEdgeActorEvent({edgeActor});
    UNIT_ASSERT(ev->Type == TEvKeyValue::EvReadResponse);
    TEvKeyValue::TEvReadResponse *response = ev->Get<TEvKeyValue::TEvReadResponse>();
    NKikimrKeyValue::ReadResult &record = response->Record;

    if (status == NKikimrKeyValue::Statuses::RSTATUS_OK) {
        UNIT_ASSERT_C(record.status() == NKikimrKeyValue::Statuses::RSTATUS_OK, "Expected# " << NKikimrKeyValue::Statuses::ReplyStatus_Name(status)
                << " Received# " <<  NKikimrKeyValue::Statuses::ReplyStatus_Name(record.status())
                << " Message# " << record.msg());
        UNIT_ASSERT_VALUES_EQUAL(record.value(), expectedValues[record.requested_key()]);
    } else {
        UNIT_ASSERT_C(record.status() == status, "Expected# " << NKikimrKeyValue::Statuses::ReplyStatus_Name(status)
                << " received# " <<  NKikimrKeyValue::Statuses::ReplyStatus_Name(record.status())
                << " Message# " << record.msg());
    }

    runtime.Stop();
}

Y_UNIT_TEST(ReadOk) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TReadRequestBuilder builder("a");
    TLogoBlobID id(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Put(groupIds[2], id, NKikimrProto::OK, "b");
    builder.AddToEnd("b", id, 0, 1);

    RunTest(env, builder, groupIds);
}

Y_UNIT_TEST(ReadWithTwoPartsOk) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TReadRequestBuilder builder("a");
    TLogoBlobID id1(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Put(groupIds[2], id1, NKikimrProto::OK, "b");
    builder.AddToEnd("b", id1, 0, 1);

    TLogoBlobID id2(1, 2, 3, 2, 1, 1);
    env.BlobStorageState.Put(groupIds[2], id2, NKikimrProto::OK, "c");
    builder.AddToEnd("c", id2, 0, 1);

    RunTest(env, builder, groupIds);
}

Y_UNIT_TEST(ReadNotWholeBlobOk) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TReadRequestBuilder builder("a");
    TLogoBlobID id(1, 2, 3, 2, 3, 0);
    env.BlobStorageState.Put(groupIds[2], id, NKikimrProto::OK, "abc");
    builder.AddToEnd("b", id, 1, 1);

    RunTest(env, builder, groupIds);
}

Y_UNIT_TEST(ReadError) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TReadRequestBuilder builder("a");
    TLogoBlobID id(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Groups[groupIds[2]].Status = NKikimrProto::ERROR;
    env.BlobStorageState.Put(groupIds[2], id, NKikimrProto::OK, "b");
    builder.AddToEnd("b", id, 0, 1);

    RunTest(env, builder, groupIds, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
}

Y_UNIT_TEST(ReadOneItemError) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TReadRequestBuilder builder("a");
    TLogoBlobID id(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Put(groupIds[2], id, NKikimrProto::ERROR, "b");
    builder.AddToEnd("b", id, 0, 1);

    RunTest(env, builder, groupIds, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
}

Y_UNIT_TEST(ReadErrorWithWrongGroupId) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TReadRequestBuilder builder("a");
    TLogoBlobID id(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Groups[groupIds[2]].GroupId = groupIds[1];
    env.BlobStorageState.Put(groupIds[2], id, NKikimrProto::OK, "b");
    builder.AddToEnd("b", id, 0, 1);

    RunTest(env, builder, groupIds, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
}

Y_UNIT_TEST(ReadErrorWithUncorrectCookie) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TReadRequestBuilder builder("a");
    TLogoBlobID id(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Groups[groupIds[2]].Cookie = 1000;
    env.BlobStorageState.Put(groupIds[2], id, NKikimrProto::OK, "b");
    builder.AddToEnd("b", id, 0, 1);

    RunTest(env, builder, groupIds, NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
}


void RunTest(TTestEnv &env, TRangeReadRequestBuilder &builder, const std::vector<ui32> &groupIds,
        NKikimrKeyValue::Statuses::ReplyStatus status = NKikimrKeyValue::Statuses::RSTATUS_OK)
{
    TTestActorSystem runtime(1);
    runtime.Start();
    runtime.SetLogPriority(NKikimrServices::KEYVALUE, NLog::PRI_DEBUG);
    env.BindGroupsToChannel(runtime, groupIds);

    TActorId edgeActor = runtime.AllocateEdgeActor(1);
    auto [intermediate, expectedValues] = builder.Build(edgeActor, edgeActor, 1, 1);

    runtime.Register(CreateKeyValueStorageReadRequest(std::move(intermediate), env.TabletInfo.release(), 1), 1);

    std::unique_ptr<IEventHandle> ev = runtime.WaitForEdgeActorEvent({edgeActor});
    UNIT_ASSERT(ev->Type == TEvKeyValue::EvReadRangeResponse);
    TEvKeyValue::TEvReadRangeResponse *response = ev->Get<TEvKeyValue::TEvReadRangeResponse>();
    NKikimrKeyValue::ReadRangeResult &record = response->Record;

    if (status == NKikimrKeyValue::Statuses::RSTATUS_OK) {
        UNIT_ASSERT_C(record.status() == NKikimrKeyValue::Statuses::RSTATUS_OK,
                "Expected# " << NKikimrKeyValue::Statuses::ReplyStatus_Name(status)
                << " Received# " <<  NKikimrKeyValue::Statuses::ReplyStatus_Name(record.status())
                << " Message# " << record.msg());
        for (auto &kvp : record.pair()) {
            UNIT_ASSERT_VALUES_EQUAL(kvp.value(), expectedValues[kvp.key()]);
        }
    } else {
        UNIT_ASSERT_C(record.status() == status,
                "Expected# " << NKikimrKeyValue::Statuses::ReplyStatus_Name(status)
                << " received# " <<  NKikimrKeyValue::Statuses::ReplyStatus_Name(record.status())
                << " Message# " << record.msg());
    }

    runtime.Stop();
}

Y_UNIT_TEST(ReadRangeOk1Key) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TRangeReadRequestBuilder builder;
    TLogoBlobID id(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Put(groupIds[2], id, NKikimrProto::OK, "b");
    builder.AddRead("key", "b", id);

    RunTest(env, builder, groupIds);
}

Y_UNIT_TEST(ReadRangeOk) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TRangeReadRequestBuilder builder;
    TLogoBlobID id1(1, 2, 3, 2, 1, 0);
    env.BlobStorageState.Put(groupIds[2], id1, NKikimrProto::OK, "b");
    builder.AddRead("key", "b", id1);
    TLogoBlobID id2(1, 2, 4, 2, 1, 0);
    env.BlobStorageState.Put(groupIds[2], id2, NKikimrProto::OK, "c");
    builder.AddRead("key2", "c", id2);

    RunTest(env, builder, groupIds);
}


Y_UNIT_TEST(ReadRangeNoData) {
    TTestEnv env;
    std::vector<ui32> groupIds = {1, 2, 3};

    TRangeReadRequestBuilder builder;

    RunTest(env, builder, groupIds, NKikimrKeyValue::Statuses::RSTATUS_OK);
}

} // Y_UNIT_TEST_SUITE(KeyValueReadStorage)

} // NKeyValue
} // NKikimr
