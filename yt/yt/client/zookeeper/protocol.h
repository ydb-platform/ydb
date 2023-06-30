#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

struct IZookeeperProtocolReader
{
    virtual ~IZookeeperProtocolReader() = default;

    virtual char ReadByte() = 0;
    virtual int ReadInt() = 0;
    virtual i64 ReadLong() = 0;

    virtual bool ReadBool() = 0;

    virtual TString ReadString() = 0;
    virtual void ReadString(TString* result) = 0;

    virtual TSharedRef GetSuffix() const = 0;

    //! Returns true if input is fully consumed and false otherwise.
    virtual bool IsFinished() const = 0;
    //! Throws an error if input is not fully consumed. Does nothing otherwise.
    virtual void ValidateFinished() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IZookeeperProtocolReader> CreateZookeeperProtocolReader(TSharedRef data);

////////////////////////////////////////////////////////////////////////////////

struct IZookeeperProtocolWriter
{
    virtual ~IZookeeperProtocolWriter() = default;

    virtual void WriteByte(char value) = 0;
    virtual void WriteInt(int value) = 0;
    virtual void WriteLong(i64 value) = 0;

    virtual void WriteBool(bool value) = 0;

    virtual void WriteString(const TString& value) = 0;

    virtual TSharedRef Finish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IZookeeperProtocolWriter> CreateZookeeperProtocolWriter();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
