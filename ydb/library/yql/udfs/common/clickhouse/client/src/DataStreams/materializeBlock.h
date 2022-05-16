#pragma once

#include <Core/Block.h>


namespace NDB
{

/** Converts columns-constants to full columns ("materializes" them).
  */
Block materializeBlock(const Block & block);
void materializeBlockInplace(Block & block);

}
