#pragma once

#include "pg_proxy_types.h"
#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>

namespace NPG {

template<typename TMessage>
class TPGStreamOutput : public TBuffer {
public:
    using TBase = TBuffer;

    TPGStreamOutput() {
        TMessage header;
        TBase::Append(reinterpret_cast<const char*>(&header), sizeof(header));
    }

    void UpdateLength() {
        TPGMessage* header = reinterpret_cast<TPGMessage*>(TBase::Data());
        header->Length = htonl(TBase::Size() - sizeof(char));
    }

    TPGStreamOutput& operator <<(uint16_t v) {
        v = htons(v);
        TBase::Append(reinterpret_cast<const char*>(&v), sizeof(v));
        return *this;
    }

    TPGStreamOutput& operator <<(uint32_t v) {
        v = htonl(v);
        TBase::Append(reinterpret_cast<const char*>(&v), sizeof(v));
        return *this;
    }

    TPGStreamOutput& operator <<(char v) {
        TBase::Append(v);
        return *this;
    }

    TPGStreamOutput& operator <<(TStringBuf s) {
        TBase::Append(s.data(), s.size());
        return *this;
    }

    TPGStreamOutput& operator <<(const std::vector<uint8_t>& s) {
        TBase::Append(reinterpret_cast<const char*>(s.data()), s.size());
        return *this;
    }
};

class TPGStreamInput {
public:
    TPGStreamInput(const TPGMessage& message)
        : Buffer(message.GetData(), message.GetDataSize())
    {
    }

    TPGStreamInput& operator >>(TString& s) {
        s = Buffer.NextTok('\0');
        return *this;
    }

    TPGStreamInput& operator >>(TStringBuf& s) {
        s = Buffer.NextTok('\0');
        return *this;
    }

    TPGStreamInput& operator >>(char& v) {
        if (Buffer.size() >= sizeof(v)) {
            v = *reinterpret_cast<const char*>(Buffer.data());
            Buffer.Skip(sizeof(v));
        } else {
            v = {};
        }
        return *this;
    }

    TPGStreamInput& operator >>(uint16_t& v) {
        if (Buffer.size() >= sizeof(v)) {
            v = ntohs(*reinterpret_cast<const uint16_t*>(Buffer.data()));
            Buffer.Skip(sizeof(v));
        } else {
            v = {};
        }
        return *this;
    }

    TPGStreamInput& operator >>(uint32_t& v) {
        if (Buffer.size() >= sizeof(v)) {
            v = ntohl(*reinterpret_cast<const uint32_t*>(Buffer.data()));
            Buffer.Skip(sizeof(v));
        } else {
            v = {};
        }
        return *this;
    }

    TPGStreamInput& Read(std::vector<uint8_t>& data, uint32_t size) {
        size = std::min<uint32_t>(size, Buffer.size());
        data.resize(size);
        memcpy(data.data(), Buffer.data(), size);
        Buffer.Skip(size);
        return *this;
    }

    bool Empty() const {
        return Buffer.Empty();
    }

protected:
    TStringBuf Buffer;
};

}
