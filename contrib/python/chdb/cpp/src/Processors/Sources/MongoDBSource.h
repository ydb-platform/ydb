#pragma once

#error #include <CHDBPoco/MongoDB/Element.h>
#error #include <CHDBPoco/MongoDB/Array.h>

#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Core/ExternalResultDescription.h>

#include <Core/Field.h>


namespace CHDBPoco
{
namespace MongoDB
{
    class Connection;
    class Document;
    class Cursor;
    class OpMsgCursor;
}
}

namespace DB_CHDB
{

struct MongoDBArrayInfo
{
    size_t num_dimensions;
    Field default_value;
    std::function<Field(const CHDBPoco::MongoDB::Element & value, const std::string & name)> parser;
};

void authenticate(CHDBPoco::MongoDB::Connection & connection, const std::string & database, const std::string & user, const std::string & password);

bool isMongoDBWireProtocolOld(CHDBPoco::MongoDB::Connection & connection_, const std::string & database_name_);

class MongoDBCursor
{
public:
    MongoDBCursor(
        const std::string & database,
        const std::string & collection,
        const Block & sample_block_to_select,
        const CHDBPoco::MongoDB::Document & query,
        CHDBPoco::MongoDB::Connection & connection);

    CHDBPoco::MongoDB::Document::Vector nextDocuments(CHDBPoco::MongoDB::Connection & connection);

    Int64 cursorID() const;

private:
    const bool is_wire_protocol_old;
    std::unique_ptr<CHDBPoco::MongoDB::Cursor> old_cursor;
    std::unique_ptr<CHDBPoco::MongoDB::OpMsgCursor> new_cursor;
    Int64 cursor_id = 0;
};

/// Converts MongoDB Cursor to a stream of Blocks
class MongoDBSource final : public ISource
{
public:
    MongoDBSource(
        std::shared_ptr<CHDBPoco::MongoDB::Connection> & connection_,
        const String & database_name_,
        const String & collection_name_,
        const CHDBPoco::MongoDB::Document & query_,
        const Block & sample_block,
        UInt64 max_block_size_);

    ~MongoDBSource() override;

    String getName() const override { return "MongoDB"; }

private:
    Chunk generate() override;

    std::shared_ptr<CHDBPoco::MongoDB::Connection> connection;
    MongoDBCursor cursor;
    const UInt64 max_block_size;
    ExternalResultDescription description;
    bool all_read = false;

    std::unordered_map<size_t, MongoDBArrayInfo> array_info;
};

}
