#pragma once

#include <Core/Block.h>

#include <Core/ExternalResultDescription.h>
#include <Processors/ISource.h>
#include <DBPoco/Redis/Array.h>
#include <DBPoco/Redis/Type.h>
#include <Storages/RedisCommon.h>


namespace DB
{
    class RedisSource final : public ISource
    {
    public:
        RedisSource(
            RedisConnectionPtr connection_,
            const RedisArray & keys_,
            const RedisStorageType & storage_type_,
            const DB::Block & sample_block,
            size_t max_block_size);

        ~RedisSource() override;

        String getName() const override { return "Redis"; }

    private:
        Chunk generate() override;

        RedisConnectionPtr connection;
        DBPoco::Redis::Array keys;
        RedisStorageType storage_type;
        const size_t max_block_size;
        ExternalResultDescription description;
        size_t cursor = 0;
        bool all_read = false;
    };

}

