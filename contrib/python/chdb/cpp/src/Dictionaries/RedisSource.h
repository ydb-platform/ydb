#pragma once

#include <Core/Block.h>

#include <Core/ExternalResultDescription.h>
#include <Processors/ISource.h>
#include <CHDBPoco/Redis/Array.h>
#include <CHDBPoco/Redis/Type.h>
#include <Storages/RedisCommon.h>


namespace DB_CHDB
{
    class RedisSource final : public ISource
    {
    public:
        RedisSource(
            RedisConnectionPtr connection_,
            const RedisArray & keys_,
            const RedisStorageType & storage_type_,
            const DB_CHDB::Block & sample_block,
            size_t max_block_size);

        ~RedisSource() override;

        String getName() const override { return "Redis"; }

    private:
        Chunk generate() override;

        RedisConnectionPtr connection;
        CHDBPoco::Redis::Array keys;
        RedisStorageType storage_type;
        const size_t max_block_size;
        ExternalResultDescription description;
        size_t cursor = 0;
        bool all_read = false;
    };

}

