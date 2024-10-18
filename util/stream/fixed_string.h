#pragma once

#include <util/stream/input.h>
#include <util/generic/string.h>

class TFixedStringStream: public IInputStream {
public:
    TFixedStringStream(const TString& data);
    TFixedStringStream(TString&& data);

    void MovePointer(size_t position = 0);

protected:
    size_t DoRead(void* buf, size_t len) override;
    size_t DoSkip(size_t len) override;
    size_t DoReadTo(TString& st, char ch) override;
    ui64 DoReadAll(IOutputStream& out) override;

private:
    TString Data;
    size_t Position = 0;
};
