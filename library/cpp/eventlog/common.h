#pragma once

template <class T>
class TPacketInputStream {
public:
    virtual bool Avail() const = 0;
    virtual T operator*() const = 0;
    virtual bool Next() = 0;
    virtual ~TPacketInputStream() = default;
};
