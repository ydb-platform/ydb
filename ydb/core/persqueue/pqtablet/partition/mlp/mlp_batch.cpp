#include "mlp_batch.h"


namespace NKikimr::NPQ::NMLP {

void TBatch::Add(TEvPersQueue::TEvMLPReadRequest::TPtr& ev, std::vector<TMessageId>&& messages) {
    if (!ReadResponse) {
        ReadResponse = std::make_unique<TEvPQ::TEvMLPReadBatchResponse>();
    }

    ReadResponse->Responses.emplace_back(ev->Sender, ev->Cookie, std::move(messages));
}

void TBatch::Add(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    CommitResponses.emplace_back(ev->Sender, ev->Cookie, std::make_unique<TEvPersQueue::TEvMLPCommitResponse>(NPersQueue::NErrorCode::EErrorCode::OK));
}

void TBatch::Add(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev) {
    UnlockResponses.emplace_back(ev->Sender, ev->Cookie, std::make_unique<TEvPersQueue::TEvMLPUnlockResponse>(NPersQueue::NErrorCode::EErrorCode::OK));
}

void TBatch::Add(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    ChangeMessageDeadlineResponses.emplace_back(ev->Sender, ev->Cookie, std::make_unique<TEvPersQueue::TEvMLPChangeMessageDeadlineResponse>(NPersQueue::NErrorCode::EErrorCode::OK));
}

void TBatch::Clear() {
    ReadResponse.reset();
    CommitResponses.clear();
    UnlockResponses.clear();
    ChangeMessageDeadlineResponses.clear();
}

namespace {
template<typename T>
void ReplyAll(const TActorIdentity& selfActorId, std::vector<TResponseHolder<T>>& queue) {
    for (auto& response : queue) {
        selfActorId.Send(response.Sender, response.Response.release(), 0, response.Cookie);
    }
    queue.clear();
}
}

void TBatch::Commit() {
    if (ReadResponse) {
        SelfActorId.Send(PartitionActorId, ReadResponse.release());
        ReadResponse.reset();
    }

    ReplyAll(SelfActorId, CommitResponses);
    ReplyAll(SelfActorId, UnlockResponses);
    ReplyAll(SelfActorId, ChangeMessageDeadlineResponses);
}

namespace {

template<typename T>
void ReplyError(const TActorIdentity& selfActorId, const TActorId& sender, ui64 cookie) {
    selfActorId.Send(sender, new T(NPersQueue::NErrorCode::EErrorCode::ERROR), 0, cookie);
}

template<typename T>
void ReplyErrorAll(const TActorIdentity& selfActorId, std::vector<TResponseHolder<T>>& queue) {
    for (auto& response : queue) {
        ReplyError<T>(selfActorId, response.Sender, response.Cookie);
    }
    queue.clear();
}

}

void TBatch::Rollback() {
    if (ReadResponse) {
        for (auto& response : ReadResponse->Responses) {
            ReplyError<TEvPersQueue::TEvMLPReadResponse>(SelfActorId, response.Sender, response.Cookie);
        }
        ReadResponse.reset();
    }

    ReplyErrorAll(SelfActorId, CommitResponses);
    ReplyErrorAll(SelfActorId, UnlockResponses);
    ReplyErrorAll(SelfActorId, ChangeMessageDeadlineResponses);
}

bool TBatch::Empty() const {
    return !ReadResponse && CommitResponses.empty() && UnlockResponses.empty() && ChangeMessageDeadlineResponses.empty();
}

}