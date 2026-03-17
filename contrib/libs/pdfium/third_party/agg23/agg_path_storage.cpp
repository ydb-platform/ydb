
//----------------------------------------------------------------------------
// XYQ: 2006-01-22 Copied from AGG project.
// TODO: This file uses intensive floating point operations, so it's NOT suitable
// for platforms like Symbian OS. We need to change to FIX format.
//----------------------------------------------------------------------------
//----------------------------------------------------------------------------
// Anti-Grain Geometry - Version 2.3
// Copyright (C) 2002-2005 Maxim Shemanarev (http://www.antigrain.com)
//
// Permission to copy, use, modify, sell and distribute this software
// is granted provided this copyright notice appears in all copies.
// This software is provided "as is" without express or implied
// warranty, and with no claim as to its suitability for any purpose.
//
//----------------------------------------------------------------------------
// Contact: mcseem@antigrain.com
//          mcseemagg@yahoo.com
//          http://www.antigrain.com
//----------------------------------------------------------------------------
//
// Class path_storage
//
//----------------------------------------------------------------------------

#include "agg_path_storage.h"

#include <string.h>

#include "agg_math.h"
#include "core/fxcrt/fx_memory.h"

namespace pdfium
{
namespace agg
{
path_storage::~path_storage()
{
    if(m_total_blocks) {
        float** coord_blk = m_coord_blocks + m_total_blocks - 1;
        while(m_total_blocks--) {
            FX_Free(*coord_blk);
            --coord_blk;
        }
        FX_Free(m_coord_blocks);
    }
}
path_storage::path_storage() = default;
path_storage::path_storage(path_storage&& other) {
    m_total_vertices = other.m_total_vertices;
    m_total_blocks = other.m_total_blocks;
    m_max_blocks = other.m_max_blocks;
    m_coord_blocks = other.m_coord_blocks;
    m_cmd_blocks = other.m_cmd_blocks;
    m_iterator = other.m_iterator;
    other.m_total_vertices = 0;
    other.m_total_blocks = 0;
    other.m_max_blocks = 0;
    other.m_coord_blocks = nullptr;
    other.m_cmd_blocks = nullptr;
    other.m_iterator = 0;
}
void path_storage::allocate_block(unsigned nb)
{
    if(nb >= m_max_blocks) {
        float** new_coords =
            FX_Alloc2D(float*, m_max_blocks + block_pool, 2);
        unsigned char** new_cmds =
            (unsigned char**)(new_coords + m_max_blocks + block_pool);
        if(m_coord_blocks) {
          memcpy(new_coords, m_coord_blocks, m_max_blocks * sizeof(float*));
          memcpy(new_cmds, m_cmd_blocks, m_max_blocks * sizeof(unsigned char*));
          FX_Free(m_coord_blocks);
        }
        m_coord_blocks = new_coords;
        m_cmd_blocks = new_cmds;
        m_max_blocks += block_pool;
    }
    m_coord_blocks[nb] =
        FX_Alloc( float, block_size * 2 +
                  block_size /
                  (sizeof(float) / sizeof(unsigned char)));
    m_cmd_blocks[nb]  =
        (unsigned char*)(m_coord_blocks[nb] + block_size * 2);
    m_total_blocks++;
}
void path_storage::rewind(unsigned path_id)
{
    m_iterator = path_id;
}
void path_storage::curve4(float x_ctrl1, float y_ctrl1,
                          float x_ctrl2, float y_ctrl2,
                          float x_to,    float y_to)
{
    add_vertex(x_ctrl1, y_ctrl1, path_cmd_curve4);
    add_vertex(x_ctrl2, y_ctrl2, path_cmd_curve4);
    add_vertex(x_to,    y_to,    path_cmd_curve4);
}
void path_storage::end_poly()
{
    if(m_total_vertices) {
        if(is_vertex(command(m_total_vertices - 1))) {
            add_vertex(0, 0, unsigned{path_cmd_end_poly} | path_flags_close);
        }
    }
}
}
}  // namespace pdfium
