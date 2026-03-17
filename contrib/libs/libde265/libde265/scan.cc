/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * This file is part of libde265.
 *
 * libde265 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * libde265 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with libde265.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "scan.h"

static position scan0 = { 0,0 };
static position scan_h_1[ 2* 2], scan_v_1[ 2* 2], scan_d_1[ 2* 2];
static position scan_h_2[ 4* 4], scan_v_2[ 4* 4], scan_d_2[ 4* 4];
static position scan_h_3[ 8* 8], scan_v_3[ 8* 8], scan_d_3[ 8* 8];
static position scan_h_4[16*16], scan_v_4[16*16], scan_d_4[16*16];
static position scan_h_5[32*32], scan_v_5[32*32], scan_d_5[32*32];

static position* scan_h[7] = { &scan0,scan_h_1,scan_h_2,scan_h_3,scan_h_4,scan_h_5 };
static position* scan_v[7] = { &scan0,scan_v_1,scan_v_2,scan_v_3,scan_v_4,scan_v_5 };
static position* scan_d[7] = { &scan0,scan_d_1,scan_d_2,scan_d_3,scan_d_4,scan_d_5 };

static void init_scan_h(position* scan, int blkSize)
{
  int i=0;
  for (int y=0;y<blkSize;y++)
    for (int x=0;x<blkSize;x++)
      {
        scan[i].x = x;
        scan[i].y = y;
        i++;
      }
}

static void init_scan_v(position* scan, int blkSize)
{
  int i=0;
  for (int x=0;x<blkSize;x++)
    for (int y=0;y<blkSize;y++)
      {
        scan[i].x = x;
        scan[i].y = y;
        i++;
      }
}

static void init_scan_d(position* scan, int blkSize)
{
  int i=0;
  int x=0,y=0;

  do {
    while (y>=0) {
      if (x<blkSize && y<blkSize) {
        scan[i].x = x;
        scan[i].y = y;
        i++;
      }
      y--;
      x++;
    }

    y=x;
    x=0;
  } while (i < blkSize*blkSize);
}


const position* get_scan_order(int log2BlockSize, int scanIdx)
{
  switch (scanIdx) {
  case 0: return scan_d[log2BlockSize];
  case 1: return scan_h[log2BlockSize];
  case 2: return scan_v[log2BlockSize];
  default: return 0; // should never happen
  }
}



static scan_position scanpos_h_2[ 4* 4], scanpos_v_2[ 4* 4], scanpos_d_2[ 4* 4];
static scan_position scanpos_h_3[ 8* 8], scanpos_v_3[ 8* 8], scanpos_d_3[ 8* 8];
static scan_position scanpos_h_4[16*16], scanpos_v_4[16*16], scanpos_d_4[16*16];
static scan_position scanpos_h_5[32*32], scanpos_v_5[32*32], scanpos_d_5[32*32];

static scan_position* scanpos[3][6] =
  { { 0,0,scanpos_d_2,scanpos_d_3,scanpos_d_4,scanpos_d_5 },
    { 0,0,scanpos_h_2,scanpos_h_3,scanpos_h_4,scanpos_h_5 },
    { 0,0,scanpos_v_2,scanpos_v_3,scanpos_v_4,scanpos_v_5 } };
   

scan_position get_scan_position(int x,int y, int scanIdx, int log2BlkSize)
{
  return scanpos[scanIdx][log2BlkSize][ y*(1<<log2BlkSize) + x ];
}

static void fill_scan_pos(scan_position* pos, int x,int y,int scanIdx, int log2TrafoSize)
{
  int lastScanPos = 16;
  int lastSubBlock = (1<<(log2TrafoSize-2)) * (1<<(log2TrafoSize-2)) -1;

  const position* ScanOrderSub = get_scan_order(log2TrafoSize-2, scanIdx);
  const position* ScanOrderPos = get_scan_order(2, scanIdx);

  int xC,yC;
  do {
    if (lastScanPos==0) {
      lastScanPos=16;
      lastSubBlock--;
    }
    lastScanPos--;

    position S = ScanOrderSub[lastSubBlock];
    xC = (S.x<<2) + ScanOrderPos[lastScanPos].x;
    yC = (S.y<<2) + ScanOrderPos[lastScanPos].y;

  } while ( (xC != x) || (yC != y));

  pos->subBlock = lastSubBlock;
  pos->scanPos  = lastScanPos;
}


void init_scan_orders()
{
  for (int log2size=1;log2size<=5;log2size++)
    {
      init_scan_h(scan_h[log2size], 1<<log2size);
      init_scan_v(scan_v[log2size], 1<<log2size);
      init_scan_d(scan_d[log2size], 1<<log2size);
    }


  for (int log2size=2;log2size<=5;log2size++)
    for (int scanIdx=0;scanIdx<3;scanIdx++)
      for (int y=0;y<(1<<log2size);y++)
        for (int x=0;x<(1<<log2size);x++)
          {
            fill_scan_pos(&scanpos[scanIdx][log2size][ y*(1<<log2size) + x ],x,y,scanIdx,log2size);
          }
}
