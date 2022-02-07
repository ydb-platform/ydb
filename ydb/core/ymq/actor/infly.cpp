#include "infly.h"

#include <vector>

namespace NKikimr::NSQS {

bool TCmpByVisibilityDeadline::Compare(const TInflyMessageWithVisibilityDeadlineKey& l, const TInflyMessageWithVisibilityDeadlineKey& r) {
    return l.Message().GetVisibilityDeadline() < r.Message().GetVisibilityDeadline();
}

bool TCmpByVisibilityDeadline::Compare(const TInflyMessageWithVisibilityDeadlineKey& l, TInstant r) {
    return l.Message().GetVisibilityDeadline() < r;
}

bool TCmpByVisibilityDeadline::Compare(TInstant l, const TInflyMessageWithVisibilityDeadlineKey& r) {
    return l < r.Message().GetVisibilityDeadline();
}

bool TCmpByOffset::Compare(const TInflyMessageWithOffsetKey& l, const TInflyMessageWithOffsetKey& r) {
    return l.Message().GetOffset() < r.Message().GetOffset();
}

bool TCmpByOffset::Compare(const TInflyMessageWithOffsetKey& l, ui64 r) {
    return l.Message().GetOffset() < r;
}

bool TCmpByOffset::Compare(ui64 l, const TInflyMessageWithOffsetKey& r) {
    return l < r.Message().GetOffset();
}

TInflyMessage& TInflyMessageWithVisibilityDeadlineKey::Message() {
    return *static_cast<TInflyMessage*>(this);
}

const TInflyMessage& TInflyMessageWithVisibilityDeadlineKey::Message() const {
    return *static_cast<const TInflyMessage*>(this);
}

TInflyMessage& TInflyMessageWithOffsetKey::Message() {
    return *static_cast<TInflyMessage*>(this);
}

const TInflyMessage& TInflyMessageWithOffsetKey::Message() const {
    return *static_cast<const TInflyMessage*>(this);
}

struct TInflyMessages::TDestroyInflyMessages : public TInflyMessages::TOffsetTree::TDestroy {
    void operator()(TInflyMessageWithOffsetKey& v) const noexcept {
        TDestroy::operator()(v); // remove from tree
        delete static_cast<TInflyMessage*>(&v);
    }
};

TInflyMessages::~TInflyMessages() {
    Y_ASSERT(SetVisibilityDeadlineCandidates.Empty());
    MessagesByVisibilityDeadline.Clear();

    // MessagesByOffset clear
    MessagesByOffset.ForEachNoOrder(TDestroyInflyMessages());
    MessagesByOffset.Init();
}

void TInflyMessages::Add(THolder<TInflyMessage> msg) {
    ++Size;
    MessagesByVisibilityDeadline.Insert(msg.Get());
    MessagesByOffset.Insert(msg.Release());
}

THolder<TInflyMessage> TInflyMessages::Delete(ui64 offset) {
    auto* msg = MessagesByOffset.Find(offset);
    if (msg) {
        Y_ASSERT(Size > 0);
        --Size;
        TInflyMessageWithVisibilityDeadlineKey* byVisibilityDeadline = &msg->Message();
        byVisibilityDeadline->UnLink();
        msg->UnLink();
        return THolder<TInflyMessage>(&msg->Message());
    }
    return nullptr;
}

TInflyMessages::TReceiveCandidates TInflyMessages::Receive(size_t maxCount, TInstant now) {
    maxCount = Min(maxCount, Size);
    size_t added = 0;
    THolder<TOffsetTree> tree;
    while (added < maxCount && MessagesByVisibilityDeadline.Begin()->Message().GetVisibilityDeadline() < now) {
        if (!tree) {
            tree = MakeHolder<TOffsetTree>();
        }
        ++added;
        TInflyMessage* msg = &MessagesByVisibilityDeadline.Begin()->Message();
        TInflyMessageWithVisibilityDeadlineKey* byVisibilityDeadline = msg;
        byVisibilityDeadline->UnLink();
        TInflyMessageWithOffsetKey* byOffset = msg;
        byOffset->UnLink();
        tree->Insert(msg);
    }
    if (!added) {
        return {};
    }
    HoldCount += added;
    Size -= added;
    return TReceiveCandidates(this, std::move(tree));
}

TInflyMessages::TReceiveCandidates::TReceiveCandidates(TIntrusivePtr<TInflyMessages> parent, THolder<TOffsetTree> messages)
    : Parent(std::move(parent))
    , ReceivedMessages(std::move(messages))
{
}

void TInflyMessages::TReceiveCandidates::SetVisibilityDeadlineAndReceiveCount(ui64 offset, TInstant visibilityDeadline, const ui32 receiveCount) {
    Y_ASSERT(Parent && ReceivedMessages);
    if (auto* msg = ReceivedMessages->Find(offset)) {
        msg->Message().SetVisibilityDeadline(visibilityDeadline);
        msg->Message().SetReceiveCount(receiveCount);
    }
}

THolder<TInflyMessage> TInflyMessages::TReceiveCandidates::Delete(ui64 offset) {
    Y_ASSERT(Parent && ReceivedMessages);
    if (auto* msg = ReceivedMessages->Find(offset)) {
        Y_ASSERT(Parent->HoldCount > 0);
        --Parent->HoldCount;
        msg->UnLink();
        return THolder<TInflyMessage>(&msg->Message());
    }
    return nullptr;
}

bool TInflyMessages::TReceiveCandidates::Has(ui64 offset) const {
    Y_ASSERT(Parent && ReceivedMessages);
    return ReceivedMessages->Find(offset) != nullptr;
}

struct TInflyMessages::TReturnToParent : public TInflyMessages::TOffsetTree::TDestroy {
    explicit TReturnToParent(TIntrusivePtr<TInflyMessages> parent)
        : Parent(std::move(parent))
    {
    }

    void operator()(TInflyMessageWithOffsetKey& v) const noexcept {
        TDestroy::operator()(v); // remove from tree
        TInflyMessageWithVisibilityDeadlineKey* byVisibilityDeadline = &v.Message();
        byVisibilityDeadline->UnLink();
        Parent->Add(THolder<TInflyMessage>(static_cast<TInflyMessage*>(&v)));
        --Parent->HoldCount;
    }

    TIntrusivePtr<TInflyMessages> Parent;
};

TInflyMessages::TReceiveCandidates::~TReceiveCandidates() {
    if (ReceivedMessages) {
        Y_ASSERT(Parent);
        ReceivedMessages->ForEachNoOrder(TReturnToParent(std::move(Parent)));
        ReceivedMessages->Init();
    }
}

TInflyMessages::TChangeVisibilityCandidates::TChangeVisibilityCandidates(TIntrusivePtr<TInflyMessages> parent)
    : Parent(std::move(parent))
{
}

TInflyMessages::TChangeVisibilityCandidates::~TChangeVisibilityCandidates() {
    if (Messages) {
        Y_ASSERT(Parent);
        Messages->ForEachNoOrder(TReturnToParent(std::move(Parent)));
        Messages->Init();
    }
}

bool TInflyMessages::TChangeVisibilityCandidates::Add(ui64 offset) {
    Y_ASSERT(Parent);
    auto* byOffset = Parent->MessagesByOffset.Find(offset);
    if (byOffset) {
        if (!Messages) {
            Messages = MakeHolder<TOffsetTree>();
        }

        TInflyMessageWithVisibilityDeadlineKey* byVisibilityDeadline = &byOffset->Message();
        byOffset->UnLink();
        byVisibilityDeadline->UnLink();
        Parent->SetVisibilityDeadlineCandidates.Insert(byVisibilityDeadline);

        ++Parent->HoldCount;
        --Parent->Size;

        Messages->Insert(byOffset);
        return true;
    }
    return false;
}

void TInflyMessages::TChangeVisibilityCandidates::SetVisibilityDeadline(ui64 offset, TInstant visibilityDeadline) {
    Y_ASSERT(Parent);
    if (!Messages) {
        return;
    }
    if (auto* msg = Messages->Find(offset)) {
        TInflyMessageWithVisibilityDeadlineKey* byVisibilityDeadline = &msg->Message();
        // reinsert in SetVisibilityDeadlineCandidates tree by different key
        byVisibilityDeadline->UnLink();
        msg->Message().SetVisibilityDeadline(visibilityDeadline);
        Parent->SetVisibilityDeadlineCandidates.Insert(byVisibilityDeadline);
    }
}

THolder<TInflyMessage> TInflyMessages::TChangeVisibilityCandidates::Delete(ui64 offset) {
    Y_ASSERT(Parent);
    if (!Messages) {
        return nullptr;
    }
    if (auto* msg = Messages->Find(offset)) {
        Y_ASSERT(Parent->HoldCount > 0);
        --Parent->HoldCount;
        msg->UnLink();
        TInflyMessageWithVisibilityDeadlineKey* byVisibilityDeadline = &msg->Message();
        byVisibilityDeadline->UnLink();
        return THolder<TInflyMessage>(&msg->Message());
    }
    return nullptr;
}

bool TInflyMessages::TChangeVisibilityCandidates::Has(ui64 offset) const {
    Y_ASSERT(Parent);
    return Messages && Messages->Find(offset) != nullptr;
}

} // namespace NKikimr::NSQS
