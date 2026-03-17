#pragma once

#include "clickhouse_config.h"

#if USE_MONGODB
#error #include <DBPoco/MongoDB/Element.h>
#error #include <DBPoco/MongoDB/Array.h>

#include <Processors/ISource.h>
#include <Core/ExternalResultDescription.h>

#include <Core/Field.h>


namespace DBPoco
{
namespace MongoDB
{
    class Connection;
    class Document;
    class Cursor;
    class OpMsgCursor;
}
}

namespace DB
{

class Block;

struct MongoDBPocoLegacyArrayInfo
{
    size_t num_dimensions;
    Field default_value;
    std::function<Field(const DBPoco::MongoDB::Element & value, const std::string & name)> parser;
};

void authenticate(DBPoco::MongoDB::Connection & connection, const std::string & database, const std::string & user, const std::string & password);

bool isMongoDBWireProtocolOld(DBPoco::MongoDB::Connection & connection_, const std::string & database_name_);

/// Deprecated, will be removed soon.
class MongoDBPocoLegacyCursor
{
public:
    MongoDBPocoLegacyCursor(
        const std::string & database,
        const std::string & collection,
        const Block & sample_block_to_select,
        const DBPoco::MongoDB::Document & query,
        DBPoco::MongoDB::Connection & connection);

    DBPoco::MongoDB::Document::Vector nextDocuments(DBPoco::MongoDB::Connection & connection);

    Int64 cursorID() const;

private:
    const bool is_wire_protocol_old;
    std::unique_ptr<DBPoco::MongoDB::Cursor> old_cursor;
    std::unique_ptr<DBPoco::MongoDB::OpMsgCursor> new_cursor;
    Int64 cursor_id = 0;
};

/// Converts MongoDB Cursor to a stream of Blocks. Deprecated, will be removed soon.
class MongoDBPocoLegacySource final : public ISource
{
public:
    MongoDBPocoLegacySource(
        std::shared_ptr<DBPoco::MongoDB::Connection> & connection_,
        const String & database_name_,
        const String & collection_name_,
        const DBPoco::MongoDB::Document & query_,
        const Block & sample_block,
        UInt64 max_block_size_);

    ~MongoDBPocoLegacySource() override;

    String getName() const override { return "MongoDB"; }

private:
    Chunk generate() override;

    std::shared_ptr<DBPoco::MongoDB::Connection> connection;
    MongoDBPocoLegacyCursor cursor;
    const UInt64 max_block_size;
    ExternalResultDescription description;
    bool all_read = false;

    std::unordered_map<size_t, MongoDBPocoLegacyArrayInfo> array_info;
};

}
#endif
