#pragma once

namespace DB_CHDB
{

/// Some window functions support adding [IGNORE|RESPECT] NULLS
enum class NullsAction : UInt8
{
    EMPTY,
    RESPECT_NULLS,
    IGNORE_NULLS,
};

}
