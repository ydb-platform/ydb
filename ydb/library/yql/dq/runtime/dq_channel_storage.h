#pragma once

#include <yql/essentials/utils/chunked_buffer.h>

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

    // methods Push/Pop can throw `TDqChannelStorageException`

    // Data should be owned by `blob` argument since the Push() call is actually asynchronous
    virtual void Push(TChunkedBuffer&& blob) = 0;

    // Pop() will return false if data is not ready yet. Client should repeat Pop() in this case
    virtual bool Pop(TBuffer& data)  = 0;
};

} // namespace NYql::NDq
