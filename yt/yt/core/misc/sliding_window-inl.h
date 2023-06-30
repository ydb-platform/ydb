#ifndef SLIDING_WINDOW_INL_H_
#error "Direct inclusion of this file is not allowed, include sliding_window.h"
// For the sake of sane code completion.
#include "sliding_window.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TPacket>
TSlidingWindow<TPacket>::TSlidingWindow(ssize_t maxSize)
    : MaxSize_(maxSize)
{ }

template <class TPacket>
template <class TCallback>
void TSlidingWindow<TPacket>::AddPacket(
    ssize_t sequenceNumber,
    TPacket&& packet,
    const TCallback& callback)
{
    if (sequenceNumber < GetNextSequenceNumber()) {
        THROW_ERROR_EXCEPTION("Packet sequence number is too small")
            << TErrorAttribute("sequence_number", sequenceNumber)
            << TErrorAttribute("min_sequence_number", GetNextSequenceNumber());
    }

    if (Window_.find(sequenceNumber) != Window_.end()) {
        THROW_ERROR_EXCEPTION("Packet with this sequence number is already queued")
            << TErrorAttribute("sequence_number", sequenceNumber);
    }

    if (std::ssize(Window_) >= MaxSize_) {
        THROW_ERROR_EXCEPTION("Packet window overflow")
            << TErrorAttribute("max_size", MaxSize_);
    }

    Window_[sequenceNumber] = std::move(packet);

    for (auto it = Window_.find(NextPacketSequenceNumber_);
        it != Window_.end();
        it = Window_.find(++NextPacketSequenceNumber_))
    {
        callback(std::move(it->second));
        Window_.erase(it);
    }
}

template <class TPacket>
ssize_t TSlidingWindow<TPacket>::GetNextSequenceNumber() const
{
    return NextPacketSequenceNumber_;
}

template <class TPacket>
bool TSlidingWindow<TPacket>::IsEmpty() const
{
    return Window_.empty();
}

////////////////////////////////////////////////////////////////////////////////

template <class TPacket>
TMultiSlidingWindow<TPacket>::TMultiSlidingWindow(ssize_t maxSize)
    : MaxSize_(maxSize)
{ }

template <class TPacket>
template <class TCallback>
void TMultiSlidingWindow<TPacket>::AddPacket(
    TMultiSlidingWindowSequenceNumber sequenceNumber,
    TPacket&& packet,
    const TCallback& callback)
{
    auto it = WindowPerSource_.find(sequenceNumber.SourceId);
    if (it == WindowPerSource_.end()) {
        it = WindowPerSource_.emplace(sequenceNumber.SourceId, TSlidingWindow<TPacket>(MaxSize_)).first;
    }
    it->second.AddPacket(sequenceNumber.Value, std::forward<TPacket>(packet), callback);
}

template <class TPacket>
std::optional<TMultiSlidingWindowSequenceNumber> TMultiSlidingWindow<TPacket>::TryGetMissingSequenceNumber() const
{
    for (const auto& [sourceId, window] : WindowPerSource_) {
        if (!window.IsEmpty()) {
            return TMultiSlidingWindowSequenceNumber{
                .SourceId = sourceId,
                .Value = window.GetNextSequenceNumber()
            };
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
