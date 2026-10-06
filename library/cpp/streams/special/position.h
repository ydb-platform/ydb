#pragma once

#include <util/generic/ptr.h>
#include <util/stream/input.h>

class TPositionStream: public IInputStream {
public:
    TPositionStream(TSimpleSharedPtr<IInputStream> slave);

    inline ui64 SkipTo(ui64 position) {
        if (position <= Position)
            return Position;
        Position += Slave->Skip(position - Position);
        return Position;
    }

    inline ui64 GetPosition() const {
        return Position;
    }

protected:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoSkip(size_t len) override;

private:
    TSimpleSharedPtr<IInputStream> Slave;
    ui64 Position;
};
