#include "framing.h"

#include "http.h"

#include <util/generic/yexception.h>
#include <util/system/byteorder.h>

namespace NYT::NHttpClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUnframingInputStream
    : public IAbortableInputStream
{
public:
    explicit TUnframingInputStream(std::unique_ptr<IAbortableInputStream> underlying)
        : Underlying_(std::move(underlying))
    { }

    void Abort() override
    {
        Underlying_->Abort();
    }

    bool IsAborted() const override
    {
        return Underlying_->IsAborted();
    }

private:
    size_t DoRead(void* buf, size_t len) override
    {
        if (!MoveToDataFrame()) {
            return 0;
        }
        auto read = Underlying_->Read(buf, Min<size_t>(len, RemainingFrameSize_));
        if (read == 0) {
            ythrow yexception() << "Unexpected end of stream inside data frame";
        }
        RemainingFrameSize_ -= read;
        return read;
    }

    size_t DoSkip(size_t len) override
    {
        if (!MoveToDataFrame()) {
            return 0;
        }
        auto skipped = Underlying_->Skip(Min<size_t>(len, RemainingFrameSize_));
        if (skipped == 0) {
            ythrow yexception() << "Unexpected end of stream inside data frame";
        }
        RemainingFrameSize_ -= skipped;
        return skipped;
    }

    //! Skips keep-alive and empty data frames until a frame with payload is found.
    //! Returns false on end of stream.
    bool MoveToDataFrame()
    {
        while (RemainingFrameSize_ == 0) {
            ui8 frameTypeByte;
            if (Underlying_->Read(&frameTypeByte, sizeof(frameTypeByte)) == 0) {
                return false;
            }
            switch (static_cast<EFrameType>(frameTypeByte)) {
                case EFrameType::KeepAlive:
                    break;
                case EFrameType::Data: {
                    ui32 littleEndianSize;
                    if (Underlying_->Load(&littleEndianSize, sizeof(littleEndianSize)) != sizeof(littleEndianSize)) {
                        ythrow yexception() << "Unexpected end of stream inside data frame header";
                    }
                    RemainingFrameSize_ = LittleToHost(littleEndianSize);
                    break;
                }
                default:
                    ythrow yexception() << "Unknown frame type " << static_cast<int>(frameTypeByte);
            }
        }
        return true;
    }

private:
    const std::unique_ptr<IAbortableInputStream> Underlying_;
    i64 RemainingFrameSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::unique_ptr<IAbortableInputStream> CreateUnframingInputStream(
    std::unique_ptr<IAbortableInputStream> underlying)
{
    return std::make_unique<TUnframingInputStream>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpClient
