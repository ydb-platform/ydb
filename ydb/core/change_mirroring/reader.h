#pragma once

#include <util/system/types.h>

#include <ydb/library/actors/util/rc_buf.h>

namespace NKikimr::NChangeMirroring {

class IReader {
public:
    virtual ~IReader() {};

    /* Returns whether there are
     * any polled but unprocessed messages
     */
    virtual bool HasNext() const = 0;
    /* Returns amount of polled messages
     * remaining to process
     */
    virtual ui64 Remaining() const = 0;
    /* Returns next unprocessed message
     * Previous one can be cleaned up
     */
    virtual TRcBuf ReadNext() = 0;

    /* Return false if reading stream is closed
     * or all data read (e.g. from S3 storage)
     */
    virtual bool NeedPoll() const = 0;
    /* Polls new messages
     * it's up to specific implementation to distinguish
     * how to notify the user about polling result
     */
    virtual void Poll() = 0;
};

} // namespace NKikimr::NChangeMirroring
