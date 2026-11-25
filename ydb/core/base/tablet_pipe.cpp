#include "tablet_pipe.h"

namespace NKikimr {
namespace {

class TReservedPipeChunk final : public IContiguousChunk {
public:
    TReservedPipeChunk(TRcBuf buffer, std::shared_ptr<void> reservation)
        : Reservation(std::move(reservation))
        , Buffer(std::move(buffer))
    {}

    TContiguousSpan GetData() const override { return Buffer.GetContiguousSpan(); }
    TMutableContiguousSpan UnsafeGetDataMut() override { return {Buffer.UnsafeGetDataMut(), Buffer.GetSize()}; }
    size_t GetOccupiedMemorySize() const override { return Buffer.GetOccupiedMemorySize(); }
    TPtr Clone() override { return MakeIntrusive<TReservedPipeChunk>(TRcBuf::Copy(GetData()), Reservation); }

private:
    std::shared_ptr<void> Reservation;
    TRcBuf Buffer;
};

} // namespace

bool TEvTabletPipe::TEvPush::SerializeToArcadiaStream(NActors::TChunkSerializer* serializer) const {
    if (!MemoryReservation || !dynamic_cast<NActors::TAllocChunkSerializer*>(serializer)) {
        return TEventPBBase::SerializeToArcadiaStream(serializer);
    }

    NActors::TAllocChunkSerializer buffer;
    if (!TEventPBBase::SerializeToArcadiaStream(&buffer)) {
        return false;
    }
    const auto serialized = buffer.Release({})->GetRope();
    // Preserialization destroys TEvPush. Transfer its reservation to the
    // chunks without copying the inline buffer or any external payload bytes.
    TRope reserved;
    for (auto it = serialized.Begin(); it.Valid(); it.AdvanceToNextContiguousBlock()) {
        TRcBuf chunk(TRcBuf::Piece, it.ContiguousData(), it.ContiguousSize(), it.GetChunk());
        reserved.Insert(reserved.End(), TRope(MakeIntrusive<TReservedPipeChunk>(std::move(chunk), MemoryReservation)));
    }
    return serializer->WriteRope(&reserved);
}

} // namespace NKikimr
