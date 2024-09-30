#pragma once

#include "public.h"
#include "schema.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWireProtocolCommand,
    // Read commands:

    ((LookupRows)(1))
    // Finds rows with given keys and fetches their components.
    //
    // Input:
    //   * TReqLookupRows
    //   * Unversioned rowset containing N keys
    //
    // Output:
    //   * N unversioned rows

    ((VersionedLookupRows)(2))
    // Finds rows with given keys and fetches their components.
    //
    // Input:
    //   * TReqLookupRows
    //   * Unversioned rowset containing N keys
    //
    // Output:
    //   * N versioned rows

    // Write commands:

    ((WriteRow)(100))
    // Inserts a new row or completely replaces an existing one with matching key.
    //
    // Input:
    //   * Unversioned row
    // Output:
    //   None

    ((DeleteRow)(101))
    // Deletes a row with a given key, if it exists.
    //
    // Input:
    //   * Key
    // Output:
    //   None

    ((VersionedWriteRow)(102))
    // Writes a versioned row (possibly inserting new values and/or delete timestamps).
    // Currently only used by replicator.
    //
    // Input:
    //   * Versioned row
    // Output:
    //   None

    // Other commands:

    ((ReadLockWriteRow)(103))
    // Take primary read lock and optionally modify row
    // Deprecated.
    //
    // Input:
    //   * Key
    // Output:
    //   None

    ((WriteAndLockRow)(104))
    // Take locks on row and optionally modify row
    //
    // Input:
    //   * Unversioned row
    //   * Lock mask
    // Output:
    //   None
);

////////////////////////////////////////////////////////////////////////////////

struct TWriteRowCommand
{
    TUnversionedRow Row;
};

struct TDeleteRowCommand
{
    TUnversionedRow Row;
};

struct TVersionedWriteRowCommand
{
    // Versioned write uses versioned rows for sorted tables
    // and unversioned rows for ordered tables.
    union
    {
        TUnversionedRow UnversionedRow;
        TVersionedRow VersionedRow;
    };
};

struct TWriteAndLockRowCommand
{
    TUnversionedRow Row;
    TLockMask LockMask;
};

using TWireProtocolWriteCommand = std::variant<
    TWriteRowCommand,
    TDeleteRowCommand,
    TVersionedWriteRowCommand,
    TWriteAndLockRowCommand
>;

////////////////////////////////////////////////////////////////////////////////

EWireProtocolCommand GetWireProtocolCommand(const TWireProtocolWriteCommand& command);

////////////////////////////////////////////////////////////////////////////////

//! Builds wire-encoded stream.
struct IWireProtocolWriter
{
public:
    virtual ~IWireProtocolWriter() = default;

    virtual size_t GetByteSize() const = 0;

    virtual void WriteCommand(EWireProtocolCommand command) = 0;

    virtual void WriteLegacyLockBitmap(TLegacyLockBitmap lockBitmap) = 0;

    virtual void WriteLockMask(TLockMask lockMask) = 0;

    virtual void WriteTableSchema(const NTableClient::TTableSchema& schema) = 0;

    virtual void WriteMessage(const ::google::protobuf::MessageLite& message) = 0;

    virtual void WriteInt64(i64 value) = 0;

    virtual size_t WriteUnversionedRow(
        NTableClient::TUnversionedRow row,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr) = 0;
    virtual size_t WriteSchemafulRow(
        NTableClient::TUnversionedRow row,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr) = 0;
    virtual size_t WriteVersionedRow(NTableClient::TVersionedRow row) = 0;

    virtual void WriteUnversionedValueRange(
        NTableClient::TUnversionedValueRange valueRange,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr) = 0;

    virtual void WriteSerializedRowset(
        size_t rowCount,
        const std::vector<TSharedRef>& serializedRowset) = 0;

    virtual void WriteUnversionedRowset(
        TRange<NTableClient::TUnversionedRow> rowset,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr) = 0;
    virtual void WriteSchemafulRowset(
        TRange<NTableClient::TUnversionedRow> rowset,
        const NTableClient::TNameTableToSchemaIdMapping* idMapping = nullptr) = 0;
    virtual void WriteVersionedRowset(TRange<NTableClient::TVersionedRow> rowset) = 0;

    virtual std::vector<TSharedRef> Finish() = 0;

    template <class TRow>
    inline void WriteRowset(TRange<TRow> rowset);
};

template <>
inline void IWireProtocolWriter::WriteRowset<NTableClient::TUnversionedRow>(
    TRange<NTableClient::TUnversionedRow> rowset)
{
    return WriteUnversionedRowset(rowset);
}

template <>
inline void IWireProtocolWriter::WriteRowset<NTableClient::TVersionedRow>(
    TRange<NTableClient::TVersionedRow> rowset)
{
    return WriteVersionedRowset(rowset);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWireProtocolWriter> CreateWireProtocolWriter();

////////////////////////////////////////////////////////////////////////////////

//! Reads wire-encoded stream.
/*!
 *  All |ReadXXX| methods obey the following convention.
 *  Rows are captured by the row buffer passed in ctor.
 *  Values are either captured or not depending on |captureValues| argument.
 */
struct IWireProtocolReader
{
public:
    using TIterator = const char*;

    virtual ~IWireProtocolReader() = default;

    virtual const NTableClient::TRowBufferPtr& GetRowBuffer() const = 0;

    virtual bool IsFinished() const = 0;
    virtual TIterator GetBegin() const = 0;
    virtual TIterator GetEnd() const = 0;

    virtual TIterator GetCurrent() const = 0;
    virtual void SetCurrent(TIterator) = 0;

    virtual TSharedRef Slice(TIterator begin, TIterator end) = 0;

    virtual EWireProtocolCommand ReadCommand() = 0;

    virtual TLegacyLockBitmap ReadLegacyLockBitmap() = 0;

    virtual TLockMask ReadLockMask() = 0;

    virtual NTableClient::TTableSchema ReadTableSchema() = 0;

    virtual void ReadMessage(::google::protobuf::MessageLite* message) = 0;

    virtual i64 ReadInt64() = 0;

    virtual NTableClient::TUnversionedRow ReadUnversionedRow(
        bool captureValues,
        const TIdMapping* idMapping = nullptr) = 0;
    virtual NTableClient::TUnversionedRow ReadSchemafulRow(
        const TSchemaData& schemaData,
        bool captureValues) = 0;
    virtual NTableClient::TVersionedRow ReadVersionedRow(
        const TSchemaData& schemaData,
        bool captureValues,
        const TIdMapping* valueIdMapping = nullptr) = 0;

    virtual TSharedRange<NTableClient::TUnversionedRow> ReadUnversionedRowset(
        bool captureValues,
        const TIdMapping* idMapping = nullptr) = 0;
    virtual TSharedRange<NTableClient::TUnversionedRow> ReadSchemafulRowset(
        const TSchemaData& schemaData,
        bool captureValues) = 0;
    virtual TSharedRange<NTableClient::TVersionedRow> ReadVersionedRowset(
        const TSchemaData& schemaData,
        bool captureValues,
        const TIdMapping* valueIdMapping = nullptr) = 0;

    virtual TWireProtocolWriteCommand ReadWriteCommand(
        const TSchemaData& schemaData,
        bool captureValues,
        bool versionedWriteIsUnversioned = false) = 0;

    static TSchemaData GetSchemaData(
        const NTableClient::TTableSchema& schema,
        const NTableClient::TColumnFilter& filter);
    static TSchemaData GetSchemaData(
        const NTableClient::TTableSchema& schema);
};

////////////////////////////////////////////////////////////////////////////////

//! Creates wire protocol reader.
/*!
 *  If #rowBuffer is null, a default one is created.
 */
std::unique_ptr<IWireProtocolReader> CreateWireProtocolReader(
    TSharedRef data,
    TRowBufferPtr rowBuffer = TRowBufferPtr());

////////////////////////////////////////////////////////////////////////////////

struct IWireProtocolRowsetReader
    : public NTableClient::ISchemafulUnversionedReader
{ };

DEFINE_REFCOUNTED_TYPE(IWireProtocolRowsetReader)

IWireProtocolRowsetReaderPtr CreateWireProtocolRowsetReader(
    const std::vector<TSharedRef>& compressedBlocks,
    NCompression::ECodec codecId,
    NTableClient::TTableSchemaPtr schema,
    bool schemaful,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

struct IWireProtocolRowsetWriter
    : public NTableClient::IUnversionedRowsetWriter
{
    virtual std::vector<TSharedRef> GetCompressedBlocks() = 0;
};

DEFINE_REFCOUNTED_TYPE(IWireProtocolRowsetWriter)

IWireProtocolRowsetWriterPtr CreateWireProtocolRowsetWriter(
    NCompression::ECodec codecId,
    i64 desiredUncompressedBlockSize,
    NTableClient::TTableSchemaPtr schema,
    bool isSchemaful,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

