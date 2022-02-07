#pragma once
#include "defs.h"
#include <library/cpp/containers/intrusive_rb_tree/rb_tree.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>

#include <functional>
#include <tuple>

namespace NKikimr::NSQS {

class TInflyMessage;
struct TInflyMessageWithVisibilityDeadlineKey;
struct TInflyMessageWithOffsetKey;

struct TCmpByVisibilityDeadline {
    static bool Compare(const TInflyMessageWithVisibilityDeadlineKey& l, const TInflyMessageWithVisibilityDeadlineKey& r);
    static bool Compare(const TInflyMessageWithVisibilityDeadlineKey& l, TInstant r);
    static bool Compare(TInstant l, const TInflyMessageWithVisibilityDeadlineKey& r);
};

struct TCmpByOffset {
    static bool Compare(const TInflyMessageWithOffsetKey& l, const TInflyMessageWithOffsetKey& r);
    static bool Compare(const TInflyMessageWithOffsetKey& l, ui64 r);
    static bool Compare(ui64 l, const TInflyMessageWithOffsetKey& r);
};

struct TInflyMessageWithVisibilityDeadlineKey : public TRbTreeItem<TInflyMessageWithVisibilityDeadlineKey, TCmpByVisibilityDeadline> {
    TInflyMessage& Message();
    const TInflyMessage& Message() const;
};

struct TInflyMessageWithOffsetKey : public TRbTreeItem<TInflyMessageWithOffsetKey, TCmpByOffset> {
    TInflyMessage& Message();
    const TInflyMessage& Message() const;
};

class TInflyMessage : public TInflyMessageWithVisibilityDeadlineKey, public TInflyMessageWithOffsetKey {
public:
    TInflyMessage() = default;

    TInflyMessage(const ui64 offset, const ui64 randomId, const TInstant visibilityDeadline, const ui32 receiveCount)
        : Offset(offset)
        , RandomId(randomId)
        , VisibilityDeadline(visibilityDeadline)
        , ReceiveCount(receiveCount)
    {
    }

    ui64 GetOffset() const {
        return Offset;
    }

    ui64 GetRandomId() const {
        return RandomId;
    }

    TInstant GetVisibilityDeadline() const {
        return VisibilityDeadline;
    }

    ui32 GetReceiveCount() const {
        return ReceiveCount;
    }

    void SetVisibilityDeadline(const TInstant visibilityDeadline) {
        VisibilityDeadline = visibilityDeadline;
    }

    void SetReceiveCount(const ui32 receiveCount) {
        ReceiveCount = receiveCount;
    }

private:
    ui64 Offset = 0;
    ui64 RandomId = 0;
    TInstant VisibilityDeadline;
    ui32 ReceiveCount = 0;
};

class TInflyMessages : public TAtomicRefCount<TInflyMessages> {
    struct TReturnToParent;
public:
    using TVisibilityDeadlineTree = TRbTree<TInflyMessageWithVisibilityDeadlineKey, TCmpByVisibilityDeadline>;
    using TOffsetTree = TRbTree<TInflyMessageWithOffsetKey, TCmpByOffset>;
    struct TDestroyInflyMessages;

    // Struct to temporarily hold messages that are about to receive
    class TReceiveCandidates {
        friend class TInflyMessages;
        TReceiveCandidates(TIntrusivePtr<TInflyMessages> parent, THolder<TOffsetTree> messages);

    public:
        TReceiveCandidates() = default;
        TReceiveCandidates(TReceiveCandidates&& msgs) = default;
        TReceiveCandidates(const TReceiveCandidates&) = delete;
        ~TReceiveCandidates();

        TReceiveCandidates& operator=(TReceiveCandidates&& msgs) = default;
        TReceiveCandidates& operator=(const TReceiveCandidates&) = delete;

        bool operator!() const {
            return !ReceivedMessages;
        }

        operator bool() const {
            return !operator!();
        }

        void SetVisibilityDeadlineAndReceiveCount(ui64 offset, TInstant visibilityDeadline, const ui32 receiveCount);

        THolder<TInflyMessage> Delete(ui64 offset);
        bool Has(ui64 offset) const;

        TOffsetTree::TIterator Begin() const {
            return ReceivedMessages->Begin();
        }

        TOffsetTree::TIterator End() const {
            return ReceivedMessages->End();
        }

    private:
        TIntrusivePtr<TInflyMessages> Parent;
        THolder<TOffsetTree> ReceivedMessages;
    };

    class TChangeVisibilityCandidates {
    public:
        explicit TChangeVisibilityCandidates(TIntrusivePtr<TInflyMessages> parent);
        TChangeVisibilityCandidates() = default;
        TChangeVisibilityCandidates(TChangeVisibilityCandidates&& msgs) = default;
        TChangeVisibilityCandidates(const TChangeVisibilityCandidates&) = delete;
        ~TChangeVisibilityCandidates();

        TChangeVisibilityCandidates& operator=(TChangeVisibilityCandidates&& msgs) = default;
        TChangeVisibilityCandidates& operator=(const TChangeVisibilityCandidates&) = delete;

        bool operator!() const {
            return !Messages;
        }

        operator bool() const {
            return !operator!();
        }

        bool Add(ui64 offset);
        void SetVisibilityDeadline(ui64 offset, TInstant visibilityDeadline);
        THolder<TInflyMessage> Delete(ui64 offset);
        bool Has(ui64 offset) const;

    private:
        TIntrusivePtr<TInflyMessages> Parent;
        THolder<TOffsetTree> Messages;
    };

public:
    TInflyMessages() = default;
    ~TInflyMessages();

    void Add(THolder<TInflyMessage> msg);
    THolder<TInflyMessage> Delete(ui64 offset);
    TReceiveCandidates Receive(size_t maxCount, TInstant now);

    size_t GetInflyCount(TInstant now) const {
        const size_t infly = Size ? MessagesByVisibilityDeadline.NotLessCount(now) : 0;
        const size_t setVisibilityDeadlineCandidates = SetVisibilityDeadlineCandidates.Empty() ? 0 : SetVisibilityDeadlineCandidates.NotLessCount(now);
        return infly + setVisibilityDeadlineCandidates;
    }

    size_t GetCapacity() const {
        return Size + HoldCount;
    }

private:
    TVisibilityDeadlineTree MessagesByVisibilityDeadline;
    TOffsetTree MessagesByOffset;
    TVisibilityDeadlineTree SetVisibilityDeadlineCandidates; // to properly calculate infly cout
    size_t Size = 0;
    size_t HoldCount = 0;
};

} // namespace NKikimr::NSQS
