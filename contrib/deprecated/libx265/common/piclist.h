/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Gopu Govindaswamy <gopu@multicorewareinc.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#ifndef X265_PICLIST_H
#define X265_PICLIST_H

#include "common.h"

namespace X265_NS {

class Frame;

class PicList
{
protected:

    Frame*   m_start;
    Frame*   m_end;
    int      m_count;

public:

    PicList()
    {
        m_start = NULL;
        m_end   = NULL;
        m_count = 0;
    }

    /** Push picture to end of the list */
    void pushBack(Frame& pic);

    /** Push picture to beginning of the list */
    void pushFront(Frame& pic);

    /** Pop picture from end of the list */
    Frame* popBack();

    /** Pop picture from beginning of the list */
    Frame* popFront();

    /** Find frame with specified POC */
    Frame* getPOC(int poc);

    /** Get the current Frame from the list **/
    Frame* getCurFrame(void);

    /** Remove picture from list */
    void remove(Frame& pic);

    Frame* first()        { return m_start;   }

    Frame* last()         { return m_end;     }

    int size()            { return m_count;   }

    bool empty() const    { return !m_count;  }

    operator bool() const { return !!m_count; }
};
}

#endif // ifndef X265_PICLIST_H
