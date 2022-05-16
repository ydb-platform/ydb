#pragma once


namespace NDB
{

enum class TTLMode
{
    DELETE,
    MOVE,
    GROUP_BY,
    RECOMPRESS,
};

}
