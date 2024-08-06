#pragma once

#include <ydb/library/actors/util/rope.h>

#include <util/generic/buffer.h>
#include <util/generic/yexception.h>


namespace NYql::NDq {

class TDqChannelStorageException : public yexception {
};

class IDqChannelStorage : public TSimpleRefCount<IDqChannelStorage> {
public:
    using TPtr = TIntrusivePtr<IDqChannelStorage>;

public:
    virtual ~IDqChannelStorage() = default;

    virtual bool IsEmpty() = 0;
    virtual bool IsFull() = 0;

    // methods Put/Get can throw `TDqChannelStorageException`

    // Data should be owned by `blob` argument since the Put() call is actually asynchronous
    virtual void Put(ui64 blobId, TRope&& blob, ui64 cookie = 0) = 0;

    // TODO: there is no way for client to delete blob.
    // It is better to replace Get() with Pull() which will delete blob after read
    // (current clients read each blob exactly once)
    // Get() will return false if data is not ready yet. Client should repeat Get() in this case
    virtual bool Get(ui64 blobId, TBuffer& data, ui64 cookie = 0)  = 0;
};

} // namespace NYql::NDq
