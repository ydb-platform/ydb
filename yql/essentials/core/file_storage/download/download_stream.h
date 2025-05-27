#pragma once

#include <util/generic/yexception.h>
#include <util/stream/input.h>

namespace NYql {

class TDownloadError : public yexception {
};

class TDownloadStream : public IInputStream {
public:
    explicit TDownloadStream(IInputStream& delegatee);

private:
    size_t DoRead(void* buf, size_t len) override;

private:
    IInputStream& Delegatee_;
};
}
