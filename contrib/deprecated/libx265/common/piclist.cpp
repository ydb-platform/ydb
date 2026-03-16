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

#include "common.h"
#include "piclist.h"
#include "frame.h"

using namespace X265_NS;

void PicList::pushFront(Frame& curFrame)
{
    X265_CHECK(!curFrame.m_next && !curFrame.m_prev, "piclist: picture already in list\n"); // ensure frame is not in a list
    curFrame.m_next = m_start;
    curFrame.m_prev = NULL;

    if (m_count)
    {
        m_start->m_prev = &curFrame;
        m_start = &curFrame;
    }
    else
    {
        m_start = m_end = &curFrame;
    }
    m_count++;
}

void PicList::pushBack(Frame& curFrame)
{
    X265_CHECK(!curFrame.m_next && !curFrame.m_prev, "piclist: picture already in list\n"); // ensure frame is not in a list
    curFrame.m_next = NULL;
    curFrame.m_prev = m_end;

    if (m_count)
    {
        m_end->m_next = &curFrame;
        m_end = &curFrame;
    }
    else
    {
        m_start = m_end = &curFrame;
    }
    m_count++;
}

Frame *PicList::popFront()
{
    if (m_start)
    {
        Frame *temp = m_start;
        m_count--;

        if (m_count)
        {
            m_start = m_start->m_next;
            m_start->m_prev = NULL;
        }
        else
        {
            m_start = m_end = NULL;
        }
        temp->m_next = temp->m_prev = NULL;
        return temp;
    }
    else
        return NULL;
}

Frame* PicList::getPOC(int poc)
{
    Frame *curFrame = m_start;
    while (curFrame && curFrame->m_poc != poc)
        curFrame = curFrame->m_next;
    return curFrame;
}

Frame *PicList::popBack()
{
    if (m_end)
    {
        Frame* temp = m_end;
        m_count--;

        if (m_count)
        {
            m_end = m_end->m_prev;
            m_end->m_next = NULL;
        }
        else
        {
            m_start = m_end = NULL;
        }
        temp->m_next = temp->m_prev = NULL;
        return temp;
    }
    else
        return NULL;
}

Frame* PicList::getCurFrame(void)
{
    Frame *curFrame = m_start;
    if (curFrame != NULL)
        return curFrame;
    else
        return NULL;
}

void PicList::remove(Frame& curFrame)
{
#if _DEBUG
    Frame *tmp = m_start;
    while (tmp && tmp != &curFrame)
    {
        tmp = tmp->m_next;
    }

    X265_CHECK(tmp == &curFrame, "piclist: pic being removed was not in list\n"); // verify pic is in this list
#endif

    m_count--;
    if (m_count)
    {
        if (m_start == &curFrame)
            m_start = curFrame.m_next;
        if (m_end == &curFrame)
            m_end = curFrame.m_prev;

        if (curFrame.m_next)
            curFrame.m_next->m_prev = curFrame.m_prev;
        if (curFrame.m_prev)
            curFrame.m_prev->m_next = curFrame.m_next;
    }
    else
    {
        m_start = m_end = NULL;
    }

    curFrame.m_next = curFrame.m_prev = NULL;
}
