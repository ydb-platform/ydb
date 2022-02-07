#pragma once

#include <util/generic/buffer.h>
#include <util/generic/yexception.h>


namespace NYql::NDq {

class TDqChannelStorageException : public yexception {
};

class IDqChannelStorage : public TSimpleRefCount<IDqChannelStorage> {
public:
    using TPtr = TIntrusivePtr<IDqChannelStorage>;

    using TWakeUpCallback = std::function<void()>;

public:
    virtual ~IDqChannelStorage() = default;

    virtual bool IsEmpty() const = 0;
    virtual bool IsFull() const = 0;

    // these methods can throw `TDqChannelStorageException`
    virtual void Put(ui64 blobId, TBuffer&& blob) = 0;
    virtual bool Get(ui64 blobId, TBuffer& data)  = 0;
};

} // namespace NYql::NDq
