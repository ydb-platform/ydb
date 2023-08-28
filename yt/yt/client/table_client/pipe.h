#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A pipe connecting a schemaful writer to a schemaful reader.
class TSchemafulPipe
    : public TRefCounted
{
public:
    explicit TSchemafulPipe(IMemoryChunkProviderPtr chunkProvider);
    ~TSchemafulPipe();

    //! Returns the reader side of the pipe.
    ISchemafulUnversionedReaderPtr GetReader() const;

    //! Returns the writer side of the pipe.
    IUnversionedRowsetWriterPtr GetWriter() const;

    //! When called, propagates the error to the reader.
    void Fail(const TError& error);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    struct TData;
    using TDataPtr = TIntrusivePtr<TData>;

    class TReader;
    using TReaderPtr = TIntrusivePtr<TReader>;

    class TWriter;
    using TWriterPtr = TIntrusivePtr<TWriter>;


    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSchemafulPipe)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
