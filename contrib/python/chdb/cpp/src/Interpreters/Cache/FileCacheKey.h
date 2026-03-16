#pragma once
#include <Core/Types.h>
#include <fmt/format.h>

namespace DB_CHDB
{

struct FileCacheKey
{
    using KeyHash = UInt128;
    KeyHash key;

    std::string toString() const;

    FileCacheKey() = default;

    explicit FileCacheKey(const std::string & path);

    explicit FileCacheKey(const UInt128 & key_);

    static FileCacheKey random();

    bool operator==(const FileCacheKey & other) const { return key == other.key; }
    bool operator<(const FileCacheKey & other) const { return key < other.key; }

    static FileCacheKey fromKeyString(const std::string & key_str);
};

using FileCacheKeyAndOffset = std::pair<FileCacheKey, size_t>;
struct FileCacheKeyAndOffsetHash
{
    std::size_t operator()(const FileCacheKeyAndOffset & key) const
    {
        return std::hash<UInt128>()(key.first.key) ^ std::hash<UInt64>()(key.second);
    }
};

}

namespace std
{
template <>
struct hash<DB_CHDB::FileCacheKey>
{
    std::size_t operator()(const DB_CHDB::FileCacheKey & k) const { return hash<UInt128>()(k.key); }
};

}

template <>
struct fmt::formatter<DB_CHDB::FileCacheKey> : fmt::formatter<std::string>
{
    template <typename FormatCtx>
    auto format(const DB_CHDB::FileCacheKey & key, FormatCtx & ctx) const
    {
        return fmt::formatter<std::string>::format(key.toString(), ctx);
    }
};
