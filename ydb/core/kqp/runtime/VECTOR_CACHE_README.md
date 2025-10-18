# Vector Index Level Table Cache

This implementation adds caching support for vector index level tables in YDB's KQP (Kikimr Query Processor) to improve performance of vector similarity queries.

## Overview

Vector indexes in YDB use a hierarchical k-means tree structure stored in level tables. Previously, each vector query would read cluster data from these level tables repeatedly, causing unnecessary I/O operations. This cache implementation stores the cluster hierarchy data in memory to reduce database reads.

## Components

### TKqpVectorLevelCache
- Main cache class that stores cluster data by table path, schema version, and parent cluster ID
- Uses TTL-based expiration (default: 1 hour)
- Thread-safe with adaptive locks
- Supports cache statistics (hits/misses)
- Can be enabled/disabled for testing and debugging

### TKqpVectorLevelCacheManager
- Singleton cache manager providing global access to the cache
- Handles cache initialization and lifecycle

### TCachedClusterLevel
- Represents cached cluster data for a specific level
- Stores cluster IDs, centroid data, and index settings
- Can recreate IClusters objects from cached data

## Integration

The cache is integrated into `TKqpVectorResolveActor`:

1. **ReadChildClusters()** - Modified to check cache before reading from level table
2. **ParseFetchedClusters()** - Modified to store data in cache after reading from level table
3. **TryGetFromCache()** - New method to retrieve and use cached data
4. **StoreInCache()** - New method to save cluster data to cache

## Cache Key Structure

Cache keys are composed of:
- Table path (e.g., "/Root/TestTable/index/indexImplLevelTable")
- Schema version (to handle index rebuilds)
- Parent cluster ID (0 for root level)

Example key: `/Root/TestTable/index/indexImplLevelTable:1:0`

## Cache Invalidation

The cache supports several invalidation strategies:

1. **TTL-based expiration** (automatic) - Default 1 hour
2. **Manual table invalidation** - `InvalidateTable(tablePath)`
3. **Schema version invalidation** - `InvalidateTableVersion(tablePath, version)`
4. **Cache disable/clear** - `SetEnabled(false)`

## Configuration

Default cache settings:
- Max size: 1000 entries
- TTL: 1 hour
- Enabled by default

## Performance Benefits

The cache reduces:
- Database I/O operations for level table reads
- Query latency for vector similarity searches
- Resource usage on frequently accessed vector indexes

## Testing

Unit tests are provided for:
- Basic cache operations (put/get)
- Cache invalidation
- Enable/disable functionality
- Integration with vector queries

Integration tests verify that:
- Multiple executions of the same vector query return identical results
- Cache doesn't affect query correctness
- Performance improvements are measurable

## Usage

The cache is automatically used by vector queries. No application changes are required.

For manual cache management:
```cpp
auto& cache = TKqpVectorLevelCacheManager::Instance().GetCache();

// Check stats
auto stats = cache.GetStats();

// Disable caching
cache.SetEnabled(false);

// Invalidate specific table
cache.InvalidateTable("/Root/MyTable/vectorIndex");
```

## Future Improvements

1. **LRU eviction policy** - Currently uses simple first-found eviction
2. **Metrics integration** - Export cache statistics to monitoring
3. **Configuration settings** - Make cache size and TTL configurable
4. **Automatic invalidation** - Hook into schema change events
5. **Memory usage optimization** - Compress cached cluster data