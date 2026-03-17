#pragma once

#include <CHDBPoco/Redis/Client.h>
#include <CHDBPoco/Redis/Command.h>
#include <CHDBPoco/Redis/Array.h>
#include <CHDBPoco/Types.h>

#include <Core/Defines.h>
#include <base/BorrowedObjectPool.h>
#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageFactory.h>

namespace DB_CHDB
{
static constexpr size_t REDIS_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
static constexpr size_t REDIS_LOCK_ACQUIRE_TIMEOUT_MS = 5000;

enum class RedisStorageType : uint8_t
{
    SIMPLE,
    HASH_MAP,
    UNKNOWN
};


/// storage type to Redis key type
String storageTypeToKeyType(RedisStorageType type);

RedisStorageType parseStorageType(const String & storage_type_str);
String serializeStorageType(RedisStorageType storage_type);

struct RedisConfiguration
{
    String host;
    uint32_t port;
    uint32_t db_index;
    String password;
    RedisStorageType storage_type;
    uint32_t pool_size;
};

static uint32_t DEFAULT_REDIS_DB_INDEX = 0;
static uint32_t DEFAULT_REDIS_POOL_SIZE = 16;
static String DEFAULT_REDIS_PASSWORD;

using RedisCommand = CHDBPoco::Redis::Command;
using RedisArray = CHDBPoco::Redis::Array;
using RedisArrayPtr = std::shared_ptr<RedisArray>;
using RedisBulkString = CHDBPoco::Redis::BulkString;
using RedisSimpleString = String;
using RedisInteger = CHDBPoco::Int64;

using RedisClientPtr = std::unique_ptr<CHDBPoco::Redis::Client>;
using RedisPool = BorrowedObjectPool<RedisClientPtr>;
using RedisPoolPtr = std::shared_ptr<RedisPool>;

/// Redis scan iterator
using RedisIterator = int64_t;

struct RedisConnection
{
    RedisConnection(RedisPoolPtr pool_, RedisClientPtr client_);
    ~RedisConnection();

    RedisPoolPtr pool;
    RedisClientPtr client;
};

using RedisConnectionPtr = std::unique_ptr<RedisConnection>;

RedisConnectionPtr getRedisConnection(RedisPoolPtr pool, const RedisConfiguration & configuration);

///get all redis hash key array
///    eg: keys -> [key1, key2] and get [[key1, field1, field2], [key2, field1, field2]]
RedisArrayPtr getRedisHashMapKeys(const RedisConnectionPtr & connection, RedisArray & keys);

}
