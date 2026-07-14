$p = RandomNumber(1);

SELECT Uuid::newChrono() != Uuid::newChrono() AS chrono_unique;
SELECT Uuid::newSharded() != Uuid::newSharded() AS sharded_unique;

SELECT Uuid::newChronoPrefix($p) != Uuid::newChronoPrefix($p) AS chrono_prefix_unique;
SELECT Uuid::newShardedPrefix($p) != Uuid::newSharded() AS sharded_prefix_differs;

$chronoBase = Uuid::newChronoPrefix($p);
$shardedBase = Uuid::newShardedPrefix($p);
SELECT Uuid::newChronoPrefix($chronoBase) != Uuid::newChronoPrefix($chronoBase) AS chrono_uuid_prefix_unique;
SELECT Uuid::newShardedPrefix($shardedBase) != Uuid::newShardedPrefix($shardedBase) AS sharded_uuid_prefix_unique;
SELECT Uuid::newChronoPrefix($chronoBase, 1) != Uuid::newChronoPrefix($chronoBase, 2) AS chrono_uuid_prefix_dep_unique;
SELECT Uuid::newShardedPrefix($shardedBase, 1) != Uuid::newShardedPrefix($shardedBase, 2) AS sharded_uuid_prefix_dep_unique;

SELECT Uuid::newChronoPrefix(3) != Uuid::newChrono(0) AS chrono_small_prefix_differs;

SELECT Uuid::newChrono(1) != Uuid::newChrono(2) AS chrono_dep_unique;
SELECT Uuid::newSharded(1) != Uuid::newSharded(2) AS sharded_dep_unique;
SELECT Uuid::newChrono(1, 2, 3) != Uuid::newChrono(1, 2, 4) AS chrono_three_dep_unique;
SELECT Uuid::newSharded(1, 2, 3) != Uuid::newSharded(1, 2, 4) AS sharded_three_dep_unique;
SELECT Uuid::newChronoPrefix($p, 1) != Uuid::newChronoPrefix($p, 2) AS chrono_prefix_dep_unique;
SELECT Uuid::newShardedPrefix($p, 1) != Uuid::newShardedPrefix($p, 2) AS sharded_prefix_dep_unique;
SELECT Uuid::newChronoPrefix($p, 1, 2, 3) != Uuid::newChronoPrefix($p, 1, 2, 4) AS chrono_prefix_three_dep_unique;
SELECT Uuid::newShardedPrefix($p, 1, 2, 3) != Uuid::newShardedPrefix($p, 1, 2, 4) AS sharded_prefix_three_dep_unique;

SELECT
    Substring(CAST(Uuid::newChrono() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newChrono() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newChrono() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newChrono() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newChrono() AS String), 14, 1) = '8'
    AS chrono_string_format;
SELECT
    Substring(CAST(Uuid::newChronoPrefix($p) AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newChronoPrefix($p) AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newChronoPrefix($p) AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newChronoPrefix($p) AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newChronoPrefix($p) AS String), 14, 1) = '8'
    AS chrono_prefix_string_format;
SELECT
    Substring(CAST(Uuid::newSharded() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newSharded() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newSharded() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newSharded() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newSharded() AS String), 14, 1) = '8'
    AS sharded_string_format;
SELECT
    Substring(CAST(Uuid::newShardedPrefix($p) AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newShardedPrefix($p) AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newShardedPrefix($p) AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newShardedPrefix($p) AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newShardedPrefix($p) AS String), 14, 1) = '8'
    AS sharded_prefix_string_format;

SELECT $p != 0ul OR $p == 0ul AS prefix_is_uint64;
