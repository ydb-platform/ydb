#pragma once

#include <util/system/types.h>

#include <ydb/library/actors/util/rc_buf.h>

namespace NKikimr::NChangeMirroring {

class IReader {
public:
    virtual ~IReader() {};

    virtual bool HasNext() const = 0;
    virtual ui64 Remaining() const = 0;
    virtual TRcBuf ReadNext() = 0;

    virtual bool NeedPoll() const = 0;
    virtual void Poll() = 0;
};

} // namespace NKikimr::NChangeMirroring
