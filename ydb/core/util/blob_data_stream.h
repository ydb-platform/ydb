#pragma once

#include "defs.h"

#include <util/generic/buffer.h>

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
class TFlatBlobDataInputStream {
public:
    //
    TFlatBlobDataInputStream() {}
    TFlatBlobDataInputStream(const TStringBuf& buffer)
        : Buffer(buffer)
    {}
    ~TFlatBlobDataInputStream() {}

    //
    const void* ReadAt(ui32 offset, ui32 size) const {
        Y_VERIFY_DEBUG(Buffer.size() >= offset + size);
        Y_UNUSED(size);

        return Buffer.data() + offset;
    }

    //
    template<typename T>
    const T* ReadAt(ui32 offset) const {
        return (const T*)ReadAt(offset, sizeof(T));
    }

private:
    //
    TStringBuf Buffer;
};

////////////////////////////////////////////
class TFlatBlobDataOutputStream {
public:
    //
    TFlatBlobDataOutputStream()
    {}
    TFlatBlobDataOutputStream(TString bufferToGrab)
        : Buffer(bufferToGrab)
    {}
    ~TFlatBlobDataOutputStream() {}

    // IBlobDataStream intarface
    ui32 GetCurrentOffset() const {
        return Buffer.size();
    }

    template<typename T>
    void WritePOD(const T& dt) {
        Write((const char*)&dt, (ui32)sizeof(T));
    }

    void Write(const char* data, ui32 size) {
        Buffer.AppendNoAlias(data, size);
    }

    void WriteAtPosition(ui32 position, const char* data, ui32 size) {
        memcpy((char*)Buffer.data() + position, data, size);
    }

    void Write(const TFlatBlobDataOutputStream* anotherStream) {
        TStringBuf buf = anotherStream->CurrentBuffer();
        Write(buf.data(), buf.size());
    }

    TStringBuf CurrentBuffer() const {
        return TStringBuf(Buffer.data(), Buffer.size());
    }

    const TString& GetBuffer() const {
        return Buffer;
    }

    ////////////////////////////////////////////
    class TFutureValueBase {
    public:
        //
        TFutureValueBase(TFlatBlobDataOutputStream* dataStream, ui32 position, ui32 size)
            : DataStream(dataStream)
            , Position(position)
            , Size(size)
        {}
        ~TFutureValueBase() {}

        //
        bool IsValid() const { return DataStream != nullptr; }
        explicit operator bool() const { return IsValid(); }
        bool operator !() const { return !IsValid(); }

    protected:
        //
        TFlatBlobDataOutputStream* DataStream;
        ui32 Position;
        ui32 Size;
    };


    ////////////////////////////////////////////
    class TFutureValue : public TFutureValueBase {
    public:
        //
        TFutureValue(TFlatBlobDataOutputStream* dataStream = nullptr, ui32 position = 0, ui32 size = 0)
            : TFutureValueBase(dataStream, position, size)
        {}
        ~TFutureValue() {}

        //
        template<typename T>
        void SetValue(const T& value) {
            Y_VERIFY(sizeof(T) == Size);
            DataStream->WriteAtPosition(Position, (const char*)&value, sizeof(T));
        }
    };

    ////////////////////////////////////////////
    template<typename T>
    class TFutureValuePOD : public TFutureValueBase {
    public:
        //
        TFutureValuePOD(TFlatBlobDataOutputStream* dataStream = nullptr, ui32 position = 0, ui32 size = 0)
            : TFutureValueBase(dataStream, position, size)
        {}
        ~TFutureValuePOD() {}

        //
        void SetValue(const T& value) {
            Y_VERIFY(sizeof(T) == Size);
            DataStream->WriteAtPosition(Position, (const char*)&value, sizeof(T));
        }
    };

    //
    TFutureValue FutureWrite(const char* data, ui32 size) {
        ui32 currentOffset = GetCurrentOffset();
        Write(data, size);
        return TFutureValue(this, currentOffset, size);
    }

    template <typename T>
    TFutureValuePOD<T> FutureWritePOD(const T& dt) {
        ui32 currentOffset = GetCurrentOffset();
        WritePOD<T>(dt);
        return TFutureValuePOD<T>(this, currentOffset, (ui32)sizeof(T));
    }

private:
    //
    TString Buffer;
};

////////////////////////////////////////////
class TBufferReader {
public:
    //
    TBufferReader(const TStringBuf& buffer)
        : Buffer(buffer)
        , Offset(0)
    {}
    ~TBufferReader() {}

    void SetOffset(ui32 newOffset) {
        Offset = newOffset;
        Y_VERIFY_DEBUG(Offset <= Buffer.size());
    }
    void AddOffset(ui32 incOffset) {
        Offset += incOffset;
        Y_VERIFY_DEBUG(Offset <= Buffer.size());
    }

    //
    template<typename T>
    const T* ReadData() {
        Y_VERIFY_DEBUG(Offset + sizeof(T) <= Buffer.size());
        T* retVal = (T*) (Buffer.data() + Offset);
        Offset += sizeof(T);
        return retVal;
    }

    TStringBuf ReadData(ui32 len) {
        Y_VERIFY_DEBUG(Offset + len <= Buffer.size());
        TStringBuf retVal(Buffer.data() + Offset, len);
        Offset += len;
        return retVal;
    }

    TStringBuf GetBuffer() const {
        Y_VERIFY_DEBUG(Buffer.size() > Offset);
        return TStringBuf(Buffer.data() + Offset, Buffer.size() - Offset);
    }

private:
    //
    TStringBuf Buffer;
    ui32 Offset;
};

} // end of the NKikimr namespace

