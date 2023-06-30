#pragma once

#include "lenval_table_reader.h"

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

class TRawTableReader;
class TNodeTableReader;

////////////////////////////////////////////////////////////////////////////////

class TProtoTableReader
    : public IProtoReaderImpl
{
public:
    explicit TProtoTableReader(
        ::TIntrusivePtr<TRawTableReader> input,
        TVector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TProtoTableReader() override;

    void ReadRow(Message* row) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsEndOfStream() const override;
    bool IsRawReaderExhausted() const override;

private:
    THolder<TNodeTableReader> NodeReader_;
    TVector<const ::google::protobuf::Descriptor*> Descriptors_;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoTableReader
    : public IProtoReaderImpl
    , public TLenvalTableReader
{
public:
    explicit TLenvalProtoTableReader(
        ::TIntrusivePtr<TRawTableReader> input,
        TVector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TLenvalProtoTableReader() override;

    void ReadRow(Message* row) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsEndOfStream() const override;
    bool IsRawReaderExhausted() const override;

protected:
    void SkipRow() override;

private:
    TVector<const ::google::protobuf::Descriptor*> Descriptors_;
};

// Sometime useful outside mapreduce/yt
void ReadMessageFromNode(const TNode& node, Message* row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
