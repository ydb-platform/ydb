#pragma once

#include "pg_proxy_types.h"

namespace NPG {

template<typename TMessage>
class TPGStream : public TBuffer {
public:
    using TBase = TBuffer;

    TPGStream() {
        TMessage header;
        TBase::Append(reinterpret_cast<const char*>(&header), sizeof(header));
    }

    void UpdateLength() {
        TPGMessage* header = reinterpret_cast<TPGMessage*>(TBase::Data());
        header->Length = htonl(TBase::Size() - sizeof(char));
    }

    TPGStream& operator <<(uint16_t v) {
        v = htons(v);
        TBase::Append(reinterpret_cast<const char*>(&v), sizeof(v));
        return *this;
    }

    TPGStream& operator <<(uint32_t v) {
        v = htonl(v);
        TBase::Append(reinterpret_cast<const char*>(&v), sizeof(v));
        return *this;
    }

    TPGStream& operator <<(char v) {
        TBase::Append(v);
        return *this;
    }

    TPGStream& operator <<(TStringBuf s) {
        TBase::Append(s.data(), s.size());
        return *this;
    }
};

}
