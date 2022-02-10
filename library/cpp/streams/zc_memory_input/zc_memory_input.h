#pragma once

#include <util/stream/mem.h>
#include <util/system/defaults.h>
#include <util/generic/yexception.h>

/// Zero-copy memory input with fixed read
class TZCMemoryInput: public TMemoryInput {
public:
    TZCMemoryInput() {
    }

    TZCMemoryInput(const char* dataPtr, size_t size)
        : TMemoryInput(dataPtr, size)
    {
    }

    TZCMemoryInput(TMemoryInput& rhs)
        : TMemoryInput(rhs.Buf(), rhs.Avail())
    {
    }

    /// if there's 'size' data read it, otherwise just return false
    Y_FORCE_INLINE bool ReadFixed(const char*& buf, size_t size) {
        if (Avail() >= size) {
            buf = Buf();
            Reset(Buf() + size, Avail() - size);
            return true;
        }
        return false;
    }

    template <class T>
    Y_FORCE_INLINE T LoadPOD() {
        const char* buf = nullptr;
        if (!ReadFixed(buf, sizeof(T)))
            ythrow yexception() << "TZCMemoryInput::LoadPOD failed: not enough data ("
                                << Avail() << " of " << sizeof(T) << " bytes)";
        T res;
        memcpy(&res, buf, sizeof(T));
        return res;
    }

    template <class T>
    Y_FORCE_INLINE void ReadPOD(T& x) {
        x = LoadPOD<T>();
    }
};
