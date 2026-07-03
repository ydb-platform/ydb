#include "keyvalue_storage_read_request.h"
#include "keyvalue_const.h"
#include "keyvalue_state.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <util/generic/overloaded.h>
#include <library/cpp/time_provider/time_provider.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KEYVALUE


namespace NKikimr {
namespace NKeyValue {

class TKeyValueStorageReadRequest : public TActorBootstrapped<TKeyValueStorageReadRequest> {
    struct TGetBatch {
        TStackVec<ui32, 1> ReadItemIndecies;
        ui32 GroupId;
        ui32 Cookie;
        TInstant SentTime;

        TGetBatch(ui32 groupId, ui32 cookie)
            : GroupId(groupId)
            , Cookie(cookie)
        {}
    };

    struct TReadItemInfo {
        TIntermediate::TRead *Read;
        TIntermediate::TRead::TReadItem *ReadItem;
    };

    THolder<TIntermediate> IntermediateResult;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    ui32 TabletGeneration;
    TStackVec<TGetBatch, 1> Batches;

    ui32 ReceivedGetResults = 0;
    TString ErrorDescription;

    TStackVec<TReadItemInfo, 1> ReadItems;
    TKeyValueState *State;
    std::weak_ptr<TKeyValueStateLifetimeToken> StateLifetimeToken;

    NWilson::TSpan Span;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KEYVALUE_ACTOR;
    }

    std::variant<TIntermediate::TRead, TIntermediate::TRangeRead>& GetCommand() const {
        return *IntermediateResult->ReadCommand;
    }

    bool IsRead() const {
        return std::holds_alternative<TIntermediate::TRead>(GetCommand());
    }

    NKikimrBlobStorage::EGetHandleClass GetHandleClass() const {
        auto visitor = [&] (auto &request) {
            return request.HandleClass;
        };
        return std::visit(visitor, GetCommand());
    }

    void Bootstrap() {
        if (IntermediateResult->Deadline != TInstant::Max()) {
            TInstant now = TActivationContext::Now();
            if (IntermediateResult->Deadline <= now) {
                YDB_LOG_ERROR("Deadline reached before processing request",
                    {"marker", "KV313"},
                    {"errorDescription", ErrorDescription},
                    {"keyValue", TabletInfo->TabletID},
                    {"deadline", IntermediateResult->Deadline.MilliSeconds()},
                    {"now", now.MilliSeconds()},
                    {"gotAt", IntermediateResult->Stat.IntermediateCreatedAt.MilliSeconds()},
                    {"enqueuedAs", IntermediateResult->Stat.EnqueuedAs});
                ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT);
                return;
            }

            const TDuration timeout = IntermediateResult->Deadline - now;
            Schedule(timeout, new TEvents::TEvWakeup());
        }

        ui32 readCount = 0;
        auto addRead = [&](TIntermediate::TRead& read) {
            for (auto& readItem : read.ReadItems) {
                ReadItems.push_back({&read, &readItem});
            }
            ++readCount;
        };
        std::visit(TOverloaded{
            [&](TIntermediate::TRead& read) {
                addRead(read);
            },
            [&](TIntermediate::TRangeRead& rangeRead) {
                for (auto& read : rangeRead.Reads) {
                    addRead(read);
                }
            }
        }, GetCommand());

        if (ReadItems.empty()) {
            auto getStatus = [&](auto &request) {
                return request.Status;
            };
            NKikimrProto::EReplyStatus status = std::visit(getStatus, GetCommand());

            YDB_LOG_INFO("Inline read request",
                {"marker", "KV320"},
                {"keyValue", TabletInfo->TabletID},
                {"status", status});
            bool isError = status != NKikimrProto::OK
                    && status != NKikimrProto::UNKNOWN
                    && status != NKikimrProto::NODATA
                    && status != NKikimrProto::OVERRUN;
            if (isError) {
                YDB_LOG_ERROR("Expected OK, UNKNOWN, NODATA or OVERRUN but given status",
                    {"marker", "KV321"},
                    {"errorDescription", ErrorDescription},
                    {"status", NKikimrProto::EReplyStatus_Name(status)});
                ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
            } else {
                YDB_LOG_DEBUG("Expected OK or UNKNOWN and given",
                    {"marker", "KV322"},
                    {"status", NKikimrProto::EReplyStatus_Name(status)},
                    {"readCount", readCount});

                NKikimrKeyValue::Statuses::ReplyStatus replyStatus;
                if (status == NKikimrProto::UNKNOWN || (!IsRead() && status == NKikimrProto::NODATA)) {
                    replyStatus = NKikimrKeyValue::Statuses::RSTATUS_OK;
                } else {
                    replyStatus = ConvertStatus(status);
                }

                SendResponseAndPassAway(replyStatus);
            }
        }

        Become(&TThis::StateWait);
        SendGets();
    }

    void SendGets() {
        THashMap<ui32, ui32> mapFromGroupToBatch;

        for (ui32 readItemIdx = 0; readItemIdx < ReadItems.size(); ++readItemIdx) {
            TIntermediate::TRead::TReadItem &readItem = *ReadItems[readItemIdx].ReadItem;
            TLogoBlobID &id = readItem.LogoBlobId;
            ui32 group = TabletInfo->GroupFor(id.Channel(), id.Generation());

            // INVALID GROUP
            if (group == Max<ui32>()) {
                YDB_LOG_ERROR("InternalError can't find correct group",
                    {"marker", "KV315"},
                    {"errorDescription", ErrorDescription},
                    {"keyValue", TabletInfo->TabletID},
                    {"channel", id.Channel()},
                    {"generation", id.Generation()});
                ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
                return;
            }

            auto it = mapFromGroupToBatch.find(group);
            if (it == mapFromGroupToBatch.end()) {
                it = mapFromGroupToBatch.emplace(group, Batches.size()).first;
                Batches.emplace_back(group, Batches.size());
            }
            TGetBatch &batch = Batches[it->second];
            batch.ReadItemIndecies.push_back(readItemIdx);
        }

        NKikimrBlobStorage::EGetHandleClass handleClass = GetHandleClass();

        for (TGetBatch &batch : Batches) {
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> readQueries(
                    new TEvBlobStorage::TEvGet::TQuery[batch.ReadItemIndecies.size()]);
            for (ui32 readQueryIdx = 0; readQueryIdx < batch.ReadItemIndecies.size(); ++readQueryIdx) {
                ui32 readItemIdx = batch.ReadItemIndecies[readQueryIdx];
                TIntermediate::TRead::TReadItem &readItem = *ReadItems[readItemIdx].ReadItem;
                readQueries[readQueryIdx].Set(readItem.LogoBlobId, readItem.BlobOffset, readItem.BlobSize);
                readItem.InFlight = true;
            }

            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
                    readQueries, batch.ReadItemIndecies.size(), IntermediateResult->Deadline, handleClass, false);
            ev->ReaderTabletData = {TabletInfo->TabletID, TabletGeneration};

            SendToBSProxy(TActivationContext::AsActorContext(), batch.GroupId, ev.release(),
                    batch.Cookie, Span.GetTraceId());
            batch.SentTime = TActivationContext::Now();
        }
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev) {
        TEvBlobStorage::TEvGetResult *result = ev->Get();
        YDB_LOG_INFO("Received GetResult",
            {"marker", "KV20"},
            {"keyValue", TabletInfo->TabletID},
            {"groupId", result->GroupId},
            {"status", result->Status},
            {"responseSz", result->ResponseSz},
            {"errorReason", result->ErrorReason},
            {"readRequestCookie", IntermediateResult->Cookie});

        if (ev->Cookie >= Batches.size()) {
            YDB_LOG_ERROR("Received EvGetResult with an unexpected cookie",
                {"marker", "KV319"},
                {"errorDescription", ErrorDescription},
                {"keyValue", TabletInfo->TabletID},
                {"cookie", ev->Cookie},
                {"sentGets", Batches.size()},
                {"groupId", result->GroupId},
                {"status", result->Status},
                {"deadline", IntermediateResult->Deadline.MilliSeconds()},
                {"now", TActivationContext::Now().MilliSeconds()},
                {"gotAt", IntermediateResult->Stat.IntermediateCreatedAt.MilliSeconds()},
                {"errorReason", result->ErrorReason});
            ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
            return;
        }

        TGetBatch &batch = Batches[ev->Cookie];

        if (result->GroupId != batch.GroupId) {
            YDB_LOG_ERROR("Received EvGetResult from an unexpected storage group",
                {"marker", "KV318"},
                {"errorDescription", ErrorDescription},
                {"keyValue", TabletInfo->TabletID},
                {"groupId", result->GroupId},
                {"expecetedGroupId", batch.GroupId},
                {"status", result->Status},
                {"deadline", IntermediateResult->Deadline.MilliSeconds()},
                {"now", TActivationContext::Now().MilliSeconds()},
                {"sentAt", batch.SentTime},
                {"gotAt", IntermediateResult->Stat.IntermediateCreatedAt.MilliSeconds()},
                {"errorReason", result->ErrorReason});
            ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
            return;
        }

        if (result->Status == NKikimrProto::BLOCKED) {
            YDB_LOG_ERROR("Received BLOCKED EvGetResult",
                {"marker", "KV323"},
                {"errorDescription", ErrorDescription},
                {"keyValue", TabletInfo->TabletID},
                {"status", result->Status},
                {"deadline", IntermediateResult->Deadline.MilliSeconds()},
                {"now", TActivationContext::Now().MilliSeconds()},
                {"sentAt", batch.SentTime},
                {"gotAt", IntermediateResult->Stat.IntermediateCreatedAt.MilliSeconds()},
                {"errorReason", result->ErrorReason});
            // kill the key value tablet due to it's obsolete generation
            ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_BLOCKED);
            Send(IntermediateResult->KeyValueActorId, new TKikimrEvents::TEvPoisonPill);
            return;
        }

        if (result->Status != NKikimrProto::OK) {
            YDB_LOG_ERROR("Unexpected EvGetResult",
                {"marker", "KV316"},
                {"errorDescription", ErrorDescription},
                {"keyValue", TabletInfo->TabletID},
                {"status", result->Status},
                {"deadline", IntermediateResult->Deadline.MilliSeconds()},
                {"now", TActivationContext::Now().MilliSeconds()},
                {"sentAt", batch.SentTime},
                {"gotAt", IntermediateResult->Stat.IntermediateCreatedAt.MilliSeconds()},
                {"errorReason", result->ErrorReason});
            ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
            return;
        }


        bool hasErrorResponses = false;
        for (ui32 readQueryIdx = 0; readQueryIdx < batch.ReadItemIndecies.size(); ++readQueryIdx) {
            ui32 readItemIdx = batch.ReadItemIndecies[readQueryIdx];
            TEvBlobStorage::TEvGetResult::TResponse &response = ev->Get()->Responses[readQueryIdx];
            TIntermediate::TRead &read = *ReadItems[readItemIdx].Read;
            TIntermediate::TRead::TReadItem &readItem = *ReadItems[readItemIdx].ReadItem;
            read.Status = response.Status;

            if (response.Status == NKikimrProto::OK) {
                Y_VERIFY_S(response.Buffer.size() == readItem.BlobSize,
                        "response.Buffer.size()# " << response.Buffer.size()
                        << " readItem.BlobSize# " << readItem.BlobSize);
                Y_VERIFY_S(readItem.ValueOffset + readItem.BlobSize <= read.ValueSize,
                        "readItem.ValueOffset# " << readItem.ValueOffset
                        << " readItem.BlobSize# " << readItem.BlobSize
                        << " read.ValueSize# " << read.ValueSize);
                IntermediateResult->Stat.GroupReadBytes[std::make_pair(response.Id.Channel(), batch.GroupId)] += response.Buffer.size();
                IntermediateResult->Stat.GroupReadIops[std::make_pair(response.Id.Channel(), batch.GroupId)] += 1;
                read.Value.Write(readItem.ValueOffset, std::move(response.Buffer));
            } else if (response.Status == NKikimrProto::NODATA) {
                const ui32 refCount = StateLifetimeToken.lock() ? State->GetRefCount(readItem.LogoBlobId) : 0;
                if (refCount != 0) {
                    TStringStream str;
                    str << "NODATA received for TEvGet, but blob is still referenced"
                        << " TabletId# " << TabletInfo->TabletID
                        << " BlobId# " << readItem.LogoBlobId.ToString()
                        << " GroupId# " << batch.GroupId
                        << " RefCount# " << refCount;
                    Y_DEBUG_ABORT_UNLESS(false, "%s", str.Str().c_str());
                    YDB_LOG_ERROR("NODATA received for TEvGet, but blob is still referenced",
                        {"marker", "KV324"},
                        {"errorDescription", ErrorDescription},
                        {"keyValue", TabletInfo->TabletID},
                        {"blobId", readItem.LogoBlobId},
                        {"groupId", batch.GroupId},
                        {"refCount", refCount});
                    ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
                    return;
                }
            } else {
                YDB_LOG_ERROR("Unexpected EvGetResult",
                    {"marker", "KV317"},
                    {"errorDescription", ErrorDescription},
                    {"keyValue", TabletInfo->TabletID},
                    {"status", result->Status},
                    {"id", response.Id},
                    {"responseStatus", response.Status},
                    {"deadline", IntermediateResult->Deadline},
                    {"now", TActivationContext::Now()},
                    {"sentAt", batch.SentTime},
                    {"gotAt", IntermediateResult->Stat.IntermediateCreatedAt},
                    {"errorReason", result->ErrorReason});
                hasErrorResponses = true;
            }

            Y_ABORT_UNLESS(response.Status != NKikimrProto::UNKNOWN);
            readItem.Status = response.Status;
            readItem.InFlight = false;
        }
        if (hasErrorResponses) {
            ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR);
            return;
        }

        ReceivedGetResults++;
        if (ReceivedGetResults == Batches.size()) {
            auto status = NKikimrKeyValue::Statuses::RSTATUS_OK;
            if (IntermediateResult->IsTruncated) {
                status = NKikimrKeyValue::Statuses::RSTATUS_OVERRUN;
            } else if (IsRead()) {
                status = ConvertStatus(std::get<TIntermediate::TRead>(GetCommand()).CumulativeStatus());
            }
            SendResponseAndPassAway(status);
        }
    }

    void SendNotify(NKikimrKeyValue::Statuses::ReplyStatus status) {
        IntermediateResult->UpdateStat();
        Send(IntermediateResult->KeyValueActorId, new TEvKeyValue::TEvNotify(
            IntermediateResult->RequestUid,
            IntermediateResult->CreatedAtGeneration, IntermediateResult->CreatedAtStep,
            IntermediateResult->Stat, status, std::move(IntermediateResult->RefCountsIncr)));
    }

    std::unique_ptr<TEvKeyValue::TEvReadResponse> CreateReadResponse(NKikimrKeyValue::Statuses::ReplyStatus status,
            const TString &errorDescription)
    {
        auto response = std::make_unique<TEvKeyValue::TEvReadResponse>();
        response->Record.set_status(status);
        if (errorDescription) {
            response->Record.set_msg(errorDescription);
        }
        if (IntermediateResult->HasCookie) {
            response->Record.set_cookie(IntermediateResult->Cookie);
        }

        return response;
    }

    std::unique_ptr<TEvKeyValue::TEvReadRangeResponse> CreateReadRangeResponse(
            NKikimrKeyValue::Statuses::ReplyStatus status, const TString &errorDescription)
    {
        auto response = std::make_unique<TEvKeyValue::TEvReadRangeResponse>();
        response->Record.set_status(status);
        if (errorDescription) {
            response->Record.set_msg(errorDescription);
        }
        return response;
    }

    std::unique_ptr<IEventBase> MakeErrorResponse(NKikimrKeyValue::Statuses::ReplyStatus status) {
        if (IsRead()) {
            auto response = CreateReadResponse(status, ErrorDescription);
            auto &cmd = GetCommand();
            Y_ABORT_UNLESS(std::holds_alternative<TIntermediate::TRead>(cmd));
            auto& intermediateRead = std::get<TIntermediate::TRead>(cmd);
            response->Record.set_requested_key(intermediateRead.Key);
            response->Record.set_requested_offset(intermediateRead.Offset);
            response->Record.set_requested_size(intermediateRead.RequestedSize);
            return response;
        } else {
            return CreateReadRangeResponse(status, ErrorDescription);
        }
    }

    void ReplyErrorAndPassAway(NKikimrKeyValue::Statuses::ReplyStatus status) {
        std::unique_ptr<IEventBase> response = MakeErrorResponse(status);
        Send(IntermediateResult->RespondTo, response.release());
        IntermediateResult->IsReplied = true;
        SendNotify(status);
        Span.EndError(TStringBuilder() << NKikimrKeyValue::Statuses::ReplyStatus_Name(status));
        PassAway();
    }

    TString MakeErrorMsg(const TString &msg) const {
        TStringBuilder builder;
        if (ErrorDescription) {
            builder << ErrorDescription << ';';
        }
        if (msg) {
            builder << "Message# " << msg << ';';
        }
        return builder;
    }

    std::unique_ptr<TEvKeyValue::TEvReadResponse> MakeReadResponse(NKikimrKeyValue::Statuses::ReplyStatus status) {
        auto &cmd = GetCommand();
        Y_ABORT_UNLESS(std::holds_alternative<TIntermediate::TRead>(cmd));
        TIntermediate::TRead &interRead = std::get<TIntermediate::TRead>(cmd);

        TString errorMsg = MakeErrorMsg(interRead.Message);
        std::unique_ptr<TEvKeyValue::TEvReadResponse> response = CreateReadResponse(status, errorMsg);

        response->Record.set_requested_key(interRead.Key);
        response->Record.set_requested_offset(interRead.Offset);
        response->Record.set_requested_size(interRead.RequestedSize);

        TRope value = interRead.BuildRope();
        if (IntermediateResult->UsePayloadInResponse) {
            response->SetBuffer(std::move(value));
        } else {
            const TContiguousSpan span = value.GetContiguousSpan();
            response->Record.set_value(span.data(), span.size());
        }

        if (IntermediateResult->RespondTo.NodeId() != SelfId().NodeId()) {
            response->Record.set_node_id(SelfId().NodeId());
        }

        return response;
    }

    NKikimrKeyValue::Statuses::ReplyStatus ConvertStatus(NKikimrProto::EReplyStatus status) {
        if (status == NKikimrProto::OK) {
            return NKikimrKeyValue::Statuses::RSTATUS_OK;
        } else if (status == NKikimrProto::NODATA) {
            return NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND;
        } else if (status == NKikimrProto::OVERRUN) {
            return NKikimrKeyValue::Statuses::RSTATUS_OVERRUN;
        } else if (status == NKikimrProto::BLOCKED) {
            return NKikimrKeyValue::Statuses::RSTATUS_BLOCKED;
        } else {
            return NKikimrKeyValue::Statuses::RSTATUS_INTERNAL_ERROR;
        }
    }

    std::unique_ptr<TEvKeyValue::TEvReadRangeResponse> MakeReadRangeResponse(NKikimrKeyValue::Statuses::ReplyStatus status) {
        auto &cmd = GetCommand();
        Y_ABORT_UNLESS(std::holds_alternative<TIntermediate::TRangeRead>(cmd));
        TIntermediate::TRangeRead &interRange = std::get<TIntermediate::TRangeRead>(cmd);

        TStringBuilder msgBuilder;
        if (ErrorDescription) {
            msgBuilder << ErrorDescription << ';';
        }
        for (ui32 idx = 0; idx < interRange.Reads.size(); ++idx) {
            auto &interRead = interRange.Reads[idx];
            if (interRead.Message) {
                msgBuilder << "Messages[" << idx << "]# " << interRead.Message << ';';
            }
        }

        std::unique_ptr<TEvKeyValue::TEvReadRangeResponse> response = CreateReadRangeResponse(status, msgBuilder);
        NKikimrKeyValue::ReadRangeResult &readRangeResult = response->Record;

        for (ui32 idx = 0; idx < interRange.Reads.size(); ++idx) {
            auto &interRead = interRange.Reads[idx];
            auto *kvp = readRangeResult.add_pair();
            kvp->set_key(interRead.Key);

            TRope value = interRead.BuildRope();
            if (IntermediateResult->UsePayloadInResponse) {
                response->SetBuffer(std::move(value), idx);
            } else {
                const TContiguousSpan span = value.GetContiguousSpan();
                kvp->set_value(span.data(), span.size());
            }

            kvp->set_value_size(interRead.ValueSize);
            kvp->set_creation_unix_time(interRead.CreationUnixTime);
            ui32 storageChannel = MainStorageChannelInPublicApi;
            if (interRead.StorageChannel == NKikimrClient::TKeyValueRequest::INLINE) {
                storageChannel = InlineStorageChannelInPublicApi;
            } else {
                storageChannel = interRead.StorageChannel + MainStorageChannelInPublicApi;
            }
            kvp->set_storage_channel(storageChannel);
            kvp->set_status(NKikimrKeyValue::Statuses::RSTATUS_OK);
        }
        readRangeResult.set_status(status);

        if (IntermediateResult->RespondTo.NodeId() != SelfId().NodeId()) {
            readRangeResult.set_node_id(SelfId().NodeId());
        }

        return response;
    }

    std::unique_ptr<IEventBase> MakeResponse(NKikimrKeyValue::Statuses::ReplyStatus status) {
        if (IsRead()) {
            return MakeReadResponse(status);
        } else {
            return MakeReadRangeResponse(status);
        }
    }

    void SendResponseAndPassAway(NKikimrKeyValue::Statuses::ReplyStatus status = NKikimrKeyValue::Statuses::RSTATUS_OK) {
        YDB_LOG_INFO("Send respose",
            {"marker", "KV34"},
            {"keyValue", TabletInfo->TabletID},
            {"status", NKikimrKeyValue::Statuses_ReplyStatus_Name(status)},
            {"readRequestCookie", IntermediateResult->Cookie});
        std::unique_ptr<IEventBase> response = MakeResponse(status);
        Send(IntermediateResult->RespondTo, response.release());
        IntermediateResult->IsReplied = true;
        SendNotify(status);
        Span.EndOk();
        PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
        default:
            Y_ABORT();
        }
   }

    TKeyValueStorageReadRequest(THolder<TIntermediate> &&intermediate,
            const TTabletStorageInfo *tabletInfo, ui32 tabletGeneration,
            TKeyValueState *state,
            std::weak_ptr<TKeyValueStateLifetimeToken> stateLifetimeToken)
        : IntermediateResult(std::move(intermediate))
        , TabletInfo(const_cast<TTabletStorageInfo*>(tabletInfo))
        , TabletGeneration(tabletGeneration)
        , State(state)
        , StateLifetimeToken(std::move(stateLifetimeToken))
        , Span(TWilsonTablet::TabletBasic, IntermediateResult->Span.GetTraceId(), "KeyValue.StorageReadRequest")
    {
        IntermediateResult->Stat.KeyvalueStorageRequestSentAt = TAppData::TimeProvider->Now();
    }
};


IActor* CreateKeyValueStorageReadRequest(THolder<TIntermediate>&& intermediate,
        const TTabletStorageInfo *tabletInfo, ui32 tabletGeneration,
        TKeyValueState *state,
        std::weak_ptr<TKeyValueStateLifetimeToken> stateLifetimeToken)
{
    return new TKeyValueStorageReadRequest(std::move(intermediate), tabletInfo, tabletGeneration, state, std::move(stateLifetimeToken));
}

} // NKeyValue

} // NKikimr
