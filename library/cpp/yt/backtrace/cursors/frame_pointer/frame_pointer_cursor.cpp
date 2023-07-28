#include "frame_pointer_cursor.h"

#include <util/generic/size_literals.h>

#include <array>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

TFramePointerCursor::TFramePointerCursor(
    TSafeMemoryReader* memoryReader,
    const TFramePointerCursorContext& context)
    : MemoryReader_(memoryReader)
    , Rip_(reinterpret_cast<const void*>(context.Rip))
    , Rbp_(reinterpret_cast<const void*>(context.Rbp))
    , StartRsp_(reinterpret_cast<const void*>(context.Rsp))
{ }

bool TFramePointerCursor::IsFinished() const
{
    return Finished_;
}

const void* TFramePointerCursor::GetCurrentIP() const
{
    return Rip_;
}

void TFramePointerCursor::MoveNext()
{
    if (Finished_) {
        return;
    }

    auto add = [] (auto ptr, auto delta) {
        return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) + delta);
    };

    auto checkPtr = [&] (auto ptr) {
        ui8 data;
        return MemoryReader_->Read(ptr, &data);
    };

    // We try unwinding stack manually by following frame pointers.
    //
    // We assume that stack does not span more than 4mb.

    if (First_) {
        First_ = false;

        // For the first frame there are three special cases where naive
        // unwinding would skip the caller frame.
        //
        //   1) Right after call instruction, rbp points to frame of a caller.
        //   2) Right after "push rbp" instruction.
        //   3) Right before ret instruction, rbp points to frame of a caller.
        //
        // We read current instruction and try to detect such cases.
        //
        //  55               push %rbp
        //  48 89 e5         mov %rsp, %rbp
        //  c3               retq

        std::array<ui8, 3> data;
        if (!MemoryReader_->Read(Rip_, &data)) {
            Finished_ = true;
            return;
        }

        if (data[0] == 0xc3 || data[0] == 0x55) {
            void* savedRip;
            if (!MemoryReader_->Read(StartRsp_, &savedRip)) {
                Finished_ = true;
                return;
            }

            // Avoid infinite loop.
            if (Rip_ == savedRip) {
                Finished_ = true;
                return;
            }

            // Detect garbage pointer.
            if (!checkPtr(savedRip)) {
                Finished_ = true;
                return;
            }

            Rip_ = savedRip;
            return;
        }

        if (data[0] == 0x48 && data[1] == 0x89 && data[2] == 0xe5) {
            void* savedRip;
            if (!MemoryReader_->Read(add(StartRsp_, 8), &savedRip)) {
                Finished_ = true;
                return;
            }

            // Avoid infinite loop.
            if (Rip_ == savedRip) {
                Finished_ = true;
                return;
            }

            // Detect garbage pointer.
            if (!checkPtr(savedRip)) {
                Finished_ = true;
                return;
            }

            Rip_ = savedRip;
            return;
        }
    }

    const void* savedRbp;
    const void* savedRip;
    if (!MemoryReader_->Read(Rbp_, &savedRbp) || !MemoryReader_->Read(add(Rbp_, 8), &savedRip)) {
        Finished_ = true;
        return;
    }

    if (!checkPtr(savedRbp)) {
        Finished_ = true;
        return;
    }

    if (!checkPtr(savedRip)) {
        Finished_ = true;
        return;
    }

    if (savedRbp < StartRsp_ || savedRbp > add(StartRsp_, 4_MB)) {
        Finished_ = true;
        return;
    }

    Rip_ = savedRip;
    Rbp_ = savedRbp;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
