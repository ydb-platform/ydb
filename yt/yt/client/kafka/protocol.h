#pragma once

#include "public.h"

#include "error.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

struct IKafkaProtocolReader
{
    virtual ~IKafkaProtocolReader() = default;

    virtual char ReadByte() = 0;
    virtual int16_t ReadInt16() = 0;
    virtual int32_t ReadInt32() = 0;
    virtual int64_t ReadInt64() = 0;
    virtual ui64 ReadUnsignedVarInt() = 0;

    virtual bool ReadBool() = 0;

    virtual TString ReadString() = 0;
    virtual TString ReadBytes() = 0;
    virtual TString ReadCompactString() = 0;
    virtual TGUID ReadUuid() = 0;
    virtual void ReadString(TString* result, int length) = 0;

    virtual i32 StartReadBytes() = 0;
    virtual i32 GetReadBytesCount() = 0;
    virtual void FinishReadBytes() = 0;

    virtual TSharedRef GetSuffix() const = 0;

    //! Returns true if input is fully consumed and false otherwise.
    virtual bool IsFinished() const = 0;
    //! Throws an error if input is not fully consumed. Does nothing otherwise.
    virtual void ValidateFinished() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IKafkaProtocolReader> CreateKafkaProtocolReader(TSharedRef data);

////////////////////////////////////////////////////////////////////////////////

struct IKafkaProtocolWriter
{
    virtual ~IKafkaProtocolWriter() = default;

    virtual void WriteByte(char value) = 0;
    virtual void WriteInt16(int16_t value) = 0;
    virtual void WriteInt32(int32_t value) = 0;
    virtual void WriteInt64(int64_t value) = 0;
    virtual void WriteUnsignedVarInt(uint64_t value) = 0;
    virtual void WriteErrorCode(EErrorCode value) = 0;

    virtual void WriteBool(bool value) = 0;

    virtual void WriteNullableString(const std::optional<TString>& value) = 0;
    virtual void WriteString(const TString& value) = 0;
    virtual void WriteCompactString(const TString& value) = 0;
    virtual void WriteBytes(const TString& value) = 0;

    virtual void StartBytes() = 0;
    virtual void FinishBytes() = 0;

    virtual TSharedRef Finish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IKafkaProtocolWriter> CreateKafkaProtocolWriter();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
