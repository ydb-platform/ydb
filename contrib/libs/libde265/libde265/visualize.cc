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

#include "visualize.h"
#include "decctx.h"

#include <math.h>

#if 0
void writeFrame_Y(de265_image* img,const char* filename)
{
  int w = ctx->img->get_width();
  int h = ctx->img->get_height();
  //int c_idx=0;
  int ctb_size = 64; // HACK

  int stride = ctx->img->get_luma_stride();

  for (int ctbY=0;ctbY<ctx->current_sps->PicHeightInCtbsY;ctbY++)
    for (int ctbX=0;ctbX<ctx->current_sps->PicWidthInCtbsY;ctbX++)
      {
        int x0 = ctbX*ctb_size;
        int y0 = ctbY*ctb_size;


        uint8_t *src = ctx->img->get_image_plane_at_pos(0,x0,y0);

        printf("%s %d %d\n",filename,x0,y0);
        int dx,dy;
        for (dy=0;dy<ctb_size;dy++)
          if (y0+dy < h)
            {
              printf("%s %d %d ",filename,y0+dy,x0);

              for (dx=0;dx<ctb_size;dx++)
                if (x0+dx < w)
                  {
                    printf("%02x ",*(src+dx+dy*stride));
                  }

              printf("\n");
            }
      }
}
#endif


void write_picture_to_file(const de265_image* img, const char* filename)
{
  FILE* fh = fopen(filename, "wb");

  for (int c=0;c<3;c++)
    for (int y=0;y<de265_get_image_height(img,c);y++)
      fwrite(img->get_image_plane_at_pos(c, 0,y), de265_get_image_width(img,c), 1, fh);

  fflush(fh);
  fclose(fh);
}


void set_pixel(uint8_t* img, int x,int y, int stride, uint32_t color, int pixelSize)
{
  for (int i=0;i<pixelSize;i++) {
    uint8_t col = (color>>(i*8)) & 0xFF;
    img[y*stride + x*pixelSize + i] = col;
  }
}


void draw_block_boundary(const de265_image* srcimg,
                         uint8_t* img,int stride,
                         int x,int y,int hBlkSize, int vBlkSize, uint32_t color, int pixelSize)
{
  for (int i=0;i<vBlkSize;i++)
    {
      int yi = y + i;

      if (yi < srcimg->get_sps().pic_height_in_luma_samples) {
        set_pixel(img,x,yi,stride,color,pixelSize);
      }
    }

  for (int i=0;i<hBlkSize;i++)
    {
      int xi = x + i;

      if (xi < srcimg->get_sps().pic_width_in_luma_samples) {
        set_pixel(img,xi,y,stride,color,pixelSize);
      }
    }
}


#include "intrapred.h"

void draw_intra_pred_mode(const de265_image* srcimg,
                          uint8_t* img,int stride,
                          int x0,int y0,int log2BlkSize,
                          enum IntraPredMode mode, uint32_t color,int pixelSize)
{
  int w = 1<<log2BlkSize;

  if (mode==0) {
    // Planar -> draw square

    for (int i=-w*1/4;i<=w*1/4;i++)
      {
        set_pixel(img, x0+w*1/4, y0+w/2+i,stride, color, pixelSize);
        set_pixel(img, x0+w*3/4, y0+w/2+i,stride, color, pixelSize);
        set_pixel(img, x0+w/2+i, y0+w*1/4,stride, color, pixelSize);
        set_pixel(img, x0+w/2+i, y0+w*3/4,stride, color, pixelSize);
      }
  }
  else if (mode==1) {
    // DC -> draw circle

    for (int i=-w/4;i<w/4;i++)
      {
        int k = (sqrt((double)(w*w - i*i*16))+2)/4;

        set_pixel(img, x0+w/2+i, y0+w/2+k, stride, color, pixelSize);
        set_pixel(img, x0+w/2+i, y0+w/2-k, stride, color, pixelSize);
        set_pixel(img, x0+w/2+k, y0+w/2+i, stride, color, pixelSize);
        set_pixel(img, x0+w/2-k, y0+w/2+i, stride, color, pixelSize);
      }
  }
  else {
    // angular -> draw line in prediction direction

    int slope = intraPredAngle_table[mode];
    bool horiz = (mode<18);

    if (horiz) {
      for (int i=-w/2;i<w/2;i++)
        {
          int dy = (slope*i+Sign(slope*i)*16)/32;
          int y = y0+w/2-dy;
          if (y>=0 && y<srcimg->get_sps().pic_height_in_luma_samples) {
            set_pixel(img, x0+i+w/2, y, stride, color, pixelSize);
          }
        }
    }
    else {
      for (int i=-w/2;i<w/2;i++)
        {
          int dx = (slope*i+Sign(slope*i)*16)/32;
          int x = x0+w/2-dx;
          if (x>=0 && x<srcimg->get_sps().pic_width_in_luma_samples) {
            set_pixel(img, x, y0+i+w/2, stride, color, pixelSize);
          }
        }
    }
  }
}


void drawTBgrid(const de265_image* srcimg, uint8_t* img, int stride,
                int x0,int y0, uint32_t color, int pixelSize, int log2CbSize, int trafoDepth)
{
  int split_transform_flag = srcimg->get_split_transform_flag(x0,y0,trafoDepth);
  if (split_transform_flag) {
    int x1 = x0 + ((1<<(log2CbSize-trafoDepth))>>1);
    int y1 = y0 + ((1<<(log2CbSize-trafoDepth))>>1);
    drawTBgrid(srcimg,img,stride,x0,y0,color,pixelSize,log2CbSize,trafoDepth+1);
    drawTBgrid(srcimg,img,stride,x1,y0,color,pixelSize,log2CbSize,trafoDepth+1);
    drawTBgrid(srcimg,img,stride,x0,y1,color,pixelSize,log2CbSize,trafoDepth+1);
    drawTBgrid(srcimg,img,stride,x1,y1,color,pixelSize,log2CbSize,trafoDepth+1);
  }
  else {
    draw_block_boundary(srcimg,img,stride,x0,y0,1<<(log2CbSize-trafoDepth),1<<(log2CbSize-trafoDepth), color, pixelSize);
  }
}


enum DrawMode {
  Partitioning_CB,
  Partitioning_TB,
  Partitioning_PB,
  IntraPredMode,
  PBPredMode,
  PBMotionVectors,
  QuantP_Y
};


void tint_rect(uint8_t* img, int stride, int x0,int y0,int w,int h, uint32_t color, int pixelSize)
{
  for (int y=0;y<h;y++)
    for (int x=0;x<w;x++)
      {
        int xp = x0+x;
        int yp = y0+y;

        for (int i=0;i<pixelSize;i++) {
          uint8_t col = (color>>(i*8)) & 0xFF;
          img[yp*stride+xp*pixelSize + i] = (img[yp*stride+xp*pixelSize + i] + col)/2;
        }
      }
}

void fill_rect(uint8_t* img, int stride, int x0,int y0,int w,int h, uint32_t color, int pixelSize)
{
  for (int y=0;y<h;y++)
    for (int x=0;x<w;x++)
      {
        int xp = x0+x;
        int yp = y0+y;

        for (int i=0;i<pixelSize;i++) {
          uint8_t col = (color>>(i*8)) & 0xFF;
          img[yp*stride+xp*pixelSize + i] = col;
        }
      }
}


void draw_QuantPY_block(const de265_image* srcimg,uint8_t* img,int stride,
                        int x0,int y0, int w,int h, int pixelSize)
{
  int q = srcimg->get_QPY(x0,y0);

  const int MIN_DRAW_Q = 20;
  const int MAX_DRAW_Q = 40;

  if (q<MIN_DRAW_Q) q=MIN_DRAW_Q;
  if (q>MAX_DRAW_Q) q=MAX_DRAW_Q;

  float f = ((float)q-MIN_DRAW_Q)/(MAX_DRAW_Q-MIN_DRAW_Q);
  uint32_t col = 0xFF * f;
  col = col | (col<<8) | (col<<16);

  fill_rect(img,stride, x0,y0,w,h, col, pixelSize);
}


void draw_line(uint8_t* img,int stride,uint32_t color,int pixelSize,
               int width,int height,
               int x0,int y0,int x1,int y1)
{
  if (x1==x0 && y1==y0) {
    set_pixel(img,x0,y0,stride,color,pixelSize);
  }
  else if (abs(x1-x0) < abs(y1-y0)) {
    for (int y=y0;y<=y1;y += Sign(y1-y0))
      {
        int x = (y-y0)*(x1-x0)/(y1-y0) + x0;

        if (x>=0 && x<width && y>=0 && y<height)
         set_pixel(img,x,y,stride,color,pixelSize);
      }
  }
  else {
    for (int x=x0;x<=x1;x += Sign(x1-x0))
      {
        int y = (x-x0)*(y1-y0)/(x1-x0) + y0;

        if (x>=0 && x<width && y>=0 && y<height)
          set_pixel(img,x,y,stride,color,pixelSize);
      }
  }
}


void draw_PB_block(const de265_image* srcimg,uint8_t* img,int stride,
                   int x0,int y0, int w,int h, enum DrawMode what, uint32_t color, int pixelSize)
{
  if (what == Partitioning_PB) {
    draw_block_boundary(srcimg,img,stride,x0,y0,w,h, color,pixelSize);
  }
  else if (what == PBPredMode) {
    enum PredMode predMode = srcimg->get_pred_mode(x0,y0);

    uint32_t cols[3] = { 0xff0000, 0x0000ff, 0x00ff00 };

    tint_rect(img,stride, x0,y0,w,h, cols[predMode], pixelSize);
  }
  else if (what == PBMotionVectors) {
    const PBMotion& mvi = srcimg->get_mv_info(x0,y0);
    int x = x0+w/2;
    int y = y0+h/2;
    if (mvi.predFlag[0]) {
      draw_line(img,stride,0xFF0000,pixelSize,
                srcimg->get_width(),
                srcimg->get_height(),
                x,y,x+mvi.mv[0].x,y+mvi.mv[0].y);
    }
    if (mvi.predFlag[1]) {
      draw_line(img,stride,0x00FF00,pixelSize,
                srcimg->get_width(),
                srcimg->get_height(),
                x,y,x+mvi.mv[1].x,y+mvi.mv[1].y);
    }
  }
}


void draw_tree_grid(const de265_image* srcimg, uint8_t* img, int stride,
                    uint32_t color, int pixelSize, enum DrawMode what)
{
  const seq_parameter_set& sps = srcimg->get_sps();
  int minCbSize = sps.MinCbSizeY;

  for (int y0=0;y0<sps.PicHeightInMinCbsY;y0++)
    for (int x0=0;x0<sps.PicWidthInMinCbsY;x0++)
      {
        int log2CbSize = srcimg->get_log2CbSize_cbUnits(x0,y0);
        if (log2CbSize==0) {
          continue;
        }

        int xb = x0*minCbSize;
        int yb = y0*minCbSize;

        int CbSize = 1<<log2CbSize;

        if (what == Partitioning_TB) {
          drawTBgrid(srcimg,img,stride,x0*minCbSize,y0*minCbSize, color,pixelSize, log2CbSize, 0);
        }
        else if (what == Partitioning_CB) {
          draw_block_boundary(srcimg,img,stride,xb,yb, 1<<log2CbSize,1<<log2CbSize, color,pixelSize);
        }
        else if (what == PBPredMode) {
          draw_PB_block(srcimg,img,stride,xb,yb,CbSize,CbSize, what,color,pixelSize);
        }
        else if (what == QuantP_Y) {
          draw_QuantPY_block(srcimg,img,stride,xb,yb,CbSize,CbSize,pixelSize);
        }
        else if (what == Partitioning_PB ||
                 what == PBMotionVectors) {
          enum PartMode partMode = srcimg->get_PartMode(xb,yb);

          int HalfCbSize = (1<<(log2CbSize-1));

          switch (partMode) {
          case PART_2Nx2N:
            draw_PB_block(srcimg,img,stride,xb,yb,CbSize,CbSize, what,color,pixelSize);
            break;
          case PART_NxN:
            draw_PB_block(srcimg,img,stride,xb,           yb,           CbSize/2,CbSize/2, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb+HalfCbSize,yb,           CbSize/2,CbSize/2, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb           ,yb+HalfCbSize,CbSize/2,CbSize/2, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb+HalfCbSize,yb+HalfCbSize,CbSize/2,CbSize/2, what,color,pixelSize);
            break;
          case PART_2NxN:
            draw_PB_block(srcimg,img,stride,xb,           yb,           CbSize  ,CbSize/2, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb,           yb+HalfCbSize,CbSize  ,CbSize/2, what,color,pixelSize);
            break;
          case PART_Nx2N:
            draw_PB_block(srcimg,img,stride,xb,           yb,           CbSize/2,CbSize, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb+HalfCbSize,yb,           CbSize/2,CbSize, what,color,pixelSize);
            break;
          case PART_2NxnU:
            draw_PB_block(srcimg,img,stride,xb,           yb,           CbSize  ,CbSize/4,   what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb,           yb+CbSize/4  ,CbSize  ,CbSize*3/4, what,color,pixelSize);
            break;
          case PART_2NxnD:
            draw_PB_block(srcimg,img,stride,xb,           yb,           CbSize  ,CbSize*3/4, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb,           yb+CbSize*3/4,CbSize  ,CbSize/4,   what,color,pixelSize);
            break;
          case PART_nLx2N:
            draw_PB_block(srcimg,img,stride,xb,           yb,           CbSize/4  ,CbSize, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb+CbSize/4  ,yb,           CbSize*3/4,CbSize, what,color,pixelSize);
            break;
          case PART_nRx2N:
            draw_PB_block(srcimg,img,stride,xb,           yb,           CbSize*3/4,CbSize, what,color,pixelSize);
            draw_PB_block(srcimg,img,stride,xb+CbSize*3/4,yb,           CbSize/4  ,CbSize, what,color,pixelSize);
            break;
          default:
            assert(false);
            break;
          }
        }
        else if (what==IntraPredMode) {
          enum PredMode predMode = srcimg->get_pred_mode(xb,yb);
          if (predMode == MODE_INTRA) {
            enum PartMode partMode = srcimg->get_PartMode(xb,yb);

            int HalfCbSize = (1<<(log2CbSize-1));

            switch (partMode) {
            case PART_2Nx2N:
              draw_intra_pred_mode(srcimg,img,stride,xb,yb,log2CbSize,
                                   srcimg->get_IntraPredMode(xb,yb), color,pixelSize);
              break;
            case PART_NxN:
              draw_intra_pred_mode(srcimg,img,stride,xb,           yb,           log2CbSize-1,
                                   srcimg->get_IntraPredMode(xb,yb), color,pixelSize);
              draw_intra_pred_mode(srcimg,img,stride,xb+HalfCbSize,yb,           log2CbSize-1,
                                   srcimg->get_IntraPredMode(xb+HalfCbSize,yb), color,pixelSize);
              draw_intra_pred_mode(srcimg,img,stride,xb           ,yb+HalfCbSize,log2CbSize-1,
                                   srcimg->get_IntraPredMode(xb,yb+HalfCbSize), color,pixelSize);
              draw_intra_pred_mode(srcimg,img,stride,xb+HalfCbSize,yb+HalfCbSize,log2CbSize-1,
                                   srcimg->get_IntraPredMode(xb+HalfCbSize,yb+HalfCbSize), color,pixelSize);
              break;
            default:
              assert(false);
              break;
            }
          }
        }
      }
}


LIBDE265_API void draw_CB_grid(const de265_image* img, uint8_t* dst, int stride, uint32_t color,int pixelSize)
{
  draw_tree_grid(img,dst,stride,color,pixelSize, Partitioning_CB);
}

LIBDE265_API void draw_TB_grid(const de265_image* img, uint8_t* dst, int stride, uint32_t color,int pixelSize)
{
  draw_tree_grid(img,dst,stride,color,pixelSize, Partitioning_TB);
}

LIBDE265_API void draw_PB_grid(const de265_image* img, uint8_t* dst, int stride, uint32_t color,int pixelSize)
{
  draw_tree_grid(img,dst,stride,color,pixelSize, Partitioning_PB);
}

LIBDE265_API void draw_intra_pred_modes(const de265_image* img, uint8_t* dst, int stride, uint32_t color,int pixelSize)
{
  draw_tree_grid(img,dst,stride,color,pixelSize, IntraPredMode);
}

LIBDE265_API void draw_PB_pred_modes(const de265_image* img, uint8_t* dst, int stride, int pixelSize)
{
  draw_tree_grid(img,dst,stride,0,pixelSize, PBPredMode);
}

LIBDE265_API void draw_QuantPY(const de265_image* img, uint8_t* dst, int stride, int pixelSize)
{
  draw_tree_grid(img,dst,stride,0,pixelSize, QuantP_Y);
}

LIBDE265_API void draw_Motion(const de265_image* img, uint8_t* dst, int stride, int pixelSize)
{
  draw_tree_grid(img,dst,stride,0,pixelSize, PBMotionVectors);
}

LIBDE265_API void draw_Slices(const de265_image* img, uint8_t* dst, int stride, int pixelSize)
{
  const seq_parameter_set& sps = img->get_sps();

  // --- mark first CTB in slice (red - independent / green - dependent) ---

  for (int ctby=0;ctby<sps.PicHeightInCtbsY;ctby++)
    for (int ctbx=0;ctbx<sps.PicWidthInCtbsY;ctbx++)
      {
        const int blkw = sps.Log2CtbSizeY;

        int ctbAddrRS = ctby*sps.PicWidthInCtbsY + ctbx;
        int prevCtbRS = -1;
        if (ctbx>0 || ctby>0) { prevCtbRS = img->get_pps().CtbAddrTStoRS[ img->get_pps().CtbAddrRStoTS[ctbAddrRS] -1 ]; }

        if (prevCtbRS<0 ||
            img->get_SliceHeaderIndex_atIndex(ctbAddrRS) !=
            img->get_SliceHeaderIndex_atIndex(prevCtbRS)) {
          int step=2;
          int fillcolor = 0xFF0000;

          if (img->get_SliceHeaderCtb(ctbx,ctby)->dependent_slice_segment_flag) {
            step=2;
            fillcolor = 0x00FF00;
          }

          for (int x=0;x<1<<blkw;x+=step)
            for (int y=0;y<1<<blkw;y+=step) {
              int x1 = x + (ctbx<<blkw);
              int y1 = y + (ctby<<blkw);

              if (x1<sps.pic_width_in_luma_samples &&
                  y1<sps.pic_height_in_luma_samples)
                {
                  set_pixel(dst,x1,y1,stride,fillcolor,pixelSize);
                }
            }
        }
      }



  // --- draw slice boundaries ---

  const uint32_t color = 0xff0000;

  for (int ctby=0;ctby<sps.PicHeightInCtbsY;ctby++)
    for (int ctbx=0;ctbx<sps.PicWidthInCtbsY;ctbx++) {
      if (ctbx>0 && (img->get_SliceHeaderIndexCtb(ctbx  ,ctby) !=
                     img->get_SliceHeaderIndexCtb(ctbx-1,ctby))) {
        int x  = ctbx << sps.Log2CtbSizeY;
        int y0 = ctby << sps.Log2CtbSizeY;

        for (int y=y0;
             (y<y0+(1<<sps.Log2CtbSizeY) &&
              y<sps.pic_height_in_luma_samples) ;
             y++) {
          set_pixel(dst,x,y,stride,color,pixelSize);
        }
      }
    }


  for (int ctby=0;ctby<sps.PicHeightInCtbsY;ctby++)
    for (int ctbx=0;ctbx<sps.PicWidthInCtbsY;ctbx++) {
      if (ctby>0 && (img->get_SliceHeaderIndexCtb(ctbx,ctby  ) !=
                     img->get_SliceHeaderIndexCtb(ctbx,ctby-1))) {
        int x0 = ctbx << sps.Log2CtbSizeY;
        int y  = ctby << sps.Log2CtbSizeY;

        for (int x=x0 ;
             (x<x0+(1<<sps.Log2CtbSizeY) &&
              x<sps.pic_width_in_luma_samples) ;
             x++) {
          set_pixel(dst,x,y,stride,color,pixelSize);
        }
      }
    }


}

LIBDE265_API void draw_Tiles(const de265_image* img, uint8_t* dst, int stride, int pixelSize)
{
  const uint32_t color = 0xffff00;

  const seq_parameter_set& sps = img->get_sps();
  const pic_parameter_set& pps = img->get_pps();


  for (int tx=1;tx<pps.num_tile_columns;tx++) {
    int x = pps.colBd[tx] << sps.Log2CtbSizeY;

    for (int y=0;y<sps.pic_height_in_luma_samples;y++) {
      set_pixel(dst,x,y,stride,color,pixelSize);
    }
  }

  for (int ty=1;ty<pps.num_tile_rows;ty++) {
    int y = pps.rowBd[ty] << sps.Log2CtbSizeY;

    for (int x=0;x<sps.pic_width_in_luma_samples;x++) {
      set_pixel(dst,x,y,stride,color,pixelSize);
    }
  }
}
