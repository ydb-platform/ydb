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

    // methods Put/Get can throw `TDqChannelStorageException`

    // TODO: support IZeroCopyInput
    virtual void Put(ui64 blobId, TBuffer&& blob) = 0;

    // TODO: there is no way for client to delete blob.
    // It is better to replace Get() with Pull() which will delete blob after read
    // (current clients read each blob exactly once)
    virtual bool Get(ui64 blobId, TBuffer& data)  = 0;
};

} // namespace NYql::NDq
