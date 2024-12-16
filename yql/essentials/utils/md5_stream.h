#pragma once

#include <util/stream/output.h>
#include <library/cpp/digest/md5/md5.h>

namespace NYql {
class TMd5OutputStream : public IOutputStream {
public:
    explicit TMd5OutputStream(IOutputStream& delegatee);
    TString Finalize();

private:
    void DoWrite(const void* buf, size_t len) override;

private:
    IOutputStream& Delegatee_;
    MD5 Accumulator_;
};
}
