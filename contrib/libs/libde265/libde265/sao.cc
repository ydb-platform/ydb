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

#include "sao.h"
#include "util.h"

#include <stdlib.h>
#include <string.h>


template <class pixel_t>
void apply_sao_internal(de265_image* img, int xCtb,int yCtb,
                        const slice_segment_header* shdr, int cIdx, int nSW,int nSH,
                        const pixel_t* in_img,  int in_stride,
                        /* */ pixel_t* out_img, int out_stride)
{
  const sao_info* saoinfo = img->get_sao_info(xCtb,yCtb);

  int SaoTypeIdx = (saoinfo->SaoTypeIdx >> (2*cIdx)) & 0x3;

  logtrace(LogSAO,"apply_sao CTB %d;%d cIdx:%d type=%d (%dx%d)\n",xCtb,yCtb,cIdx, SaoTypeIdx, nSW,nSH);

  if (SaoTypeIdx==0) {
    return;
  }

  const seq_parameter_set* sps = &img->get_sps();
  const pic_parameter_set* pps = &img->get_pps();
  const int bitDepth = (cIdx==0 ? sps->BitDepth_Y : sps->BitDepth_C);
  const int maxPixelValue = (1<<bitDepth)-1;

  // top left position of CTB in pixels
  const int xC = xCtb*nSW;
  const int yC = yCtb*nSH;

  const int width  = img->get_width(cIdx);
  const int height = img->get_height(cIdx);

  const int ctbSliceAddrRS = img->get_SliceHeader(xC,yC)->SliceAddrRS;

  const int picWidthInCtbs = sps->PicWidthInCtbsY;
  const int chromashiftW = sps->get_chroma_shift_W(cIdx);
  const int chromashiftH = sps->get_chroma_shift_H(cIdx);
  const int ctbshiftW = sps->Log2CtbSizeY - chromashiftW;
  const int ctbshiftH = sps->Log2CtbSizeY - chromashiftH;


  for (int i=0;i<5;i++)
    {
      logtrace(LogSAO,"offset[%d] = %d\n", i, i==0 ? 0 : saoinfo->saoOffsetVal[cIdx][i-1]);
    }


  // actual size of CTB to be processed (can be smaller when partially outside of image)
  const int ctbW = (xC+nSW>width)  ? width -xC : nSW;
  const int ctbH = (yC+nSH>height) ? height-yC : nSH;


  const bool extendedTests = img->get_CTB_has_pcm_or_cu_transquant_bypass(xCtb,yCtb);

  if (SaoTypeIdx==2) {
    int hPos[2], vPos[2];
    int vPosStride[2]; // vPos[] multiplied by image stride
    int SaoEoClass = (saoinfo->SaoEoClass >> (2*cIdx)) & 0x3;

    switch (SaoEoClass) {
    case 0: hPos[0]=-1; hPos[1]= 1; vPos[0]= 0; vPos[1]=0; break;
    case 1: hPos[0]= 0; hPos[1]= 0; vPos[0]=-1; vPos[1]=1; break;
    case 2: hPos[0]=-1; hPos[1]= 1; vPos[0]=-1; vPos[1]=1; break;
    case 3: hPos[0]= 1; hPos[1]=-1; vPos[0]=-1; vPos[1]=1; break;
    }

    vPosStride[0] = vPos[0] * in_stride;
    vPosStride[1] = vPos[1] * in_stride;

    /* Reorder sao_info.saoOffsetVal[] array, so that we can index it
       directly with the sum of the two pixel-difference signs. */
    int8_t  saoOffsetVal[5]; // [2] unused
    saoOffsetVal[0] = saoinfo->saoOffsetVal[cIdx][1-1];
    saoOffsetVal[1] = saoinfo->saoOffsetVal[cIdx][2-1];
    saoOffsetVal[2] = 0;
    saoOffsetVal[3] = saoinfo->saoOffsetVal[cIdx][3-1];
    saoOffsetVal[4] = saoinfo->saoOffsetVal[cIdx][4-1];


    for (int j=0;j<ctbH;j++) {
      const pixel_t* in_ptr  = &in_img [xC+(yC+j)*in_stride];
      /* */ pixel_t* out_ptr = &out_img[xC+(yC+j)*out_stride];

      for (int i=0;i<ctbW;i++) {
        int edgeIdx = -1;

        logtrace(LogSAO, "pos %d,%d\n",xC+i,yC+j);

        if ((extendedTests &&
             (sps->pcm_loop_filter_disable_flag &&
              img->get_pcm_flag((xC+i)<<chromashiftW,(yC+j)<<chromashiftH))) ||
            img->get_cu_transquant_bypass((xC+i)<<chromashiftW,(yC+j)<<chromashiftH)) {
          continue;
        }

        // do the expensive test for boundaries only at the boundaries
        bool testBoundary = (i==0 || j==0 || i==ctbW-1 || j==ctbH-1);

        if (testBoundary)
          for (int k=0;k<2;k++) {
            int xS = xC+i+hPos[k];
            int yS = yC+j+vPos[k];

            if (xS<0 || yS<0 || xS>=width || yS>=height) {
              edgeIdx=0;
              break;
            }


            // This part seems inefficient with all the get_SliceHeaderIndex() calls,
            // but removing this part (because the input was known to have only a single
            // slice anyway) reduced computation time only by 1.3%.
            // TODO: however, this may still be a big part of SAO itself.

            slice_segment_header* sliceHeader = img->get_SliceHeader(xS<<chromashiftW,
                                                                     yS<<chromashiftH);
            if (sliceHeader==NULL) { return; }

            int sliceAddrRS = sliceHeader->SliceAddrRS;
            if (sliceAddrRS <  ctbSliceAddrRS &&
                img->get_SliceHeader((xC+i)<<chromashiftW,
                                     (yC+j)<<chromashiftH)->slice_loop_filter_across_slices_enabled_flag==0) {
              edgeIdx=0;
              break;
            }

            if (sliceAddrRS >  ctbSliceAddrRS &&
                img->get_SliceHeader(xS<<chromashiftW,
                                     yS<<chromashiftH)->slice_loop_filter_across_slices_enabled_flag==0) {
              edgeIdx=0;
              break;
            }


            if (pps->loop_filter_across_tiles_enabled_flag==0 &&
                pps->TileIdRS[(xS>>ctbshiftW) + (yS>>ctbshiftH)*picWidthInCtbs] !=
                pps->TileIdRS[(xC>>ctbshiftW) + (yC>>ctbshiftH)*picWidthInCtbs]) {
              edgeIdx=0;
              break;
            }
          }

        if (edgeIdx != 0) {

          edgeIdx = ( Sign(in_ptr[i] - in_ptr[i+hPos[0]+vPosStride[0]]) +
                      Sign(in_ptr[i] - in_ptr[i+hPos[1]+vPosStride[1]])   );

          if (1) { // edgeIdx != 0) {   // seems to be faster without this check (zero in offset table)
            int offset = saoOffsetVal[edgeIdx+2];

            out_ptr[i] = Clip3(0,maxPixelValue,
                               in_ptr[i] + offset);
          }
        }
      }
    }
  }
  else {
    int bandShift = bitDepth-5;
    int saoLeftClass = saoinfo->sao_band_position[cIdx];
    logtrace(LogSAO,"saoLeftClass: %d\n",saoLeftClass);

    int bandTable[32];
    memset(bandTable, 0, sizeof(int)*32);

    for (int k=0;k<4;k++) {
      bandTable[ (k+saoLeftClass)&31 ] = k+1;
    }


    /* If PCM or transquant_bypass is used in this CTB, we have to
       run all checks (A).
       Otherwise, we run a simplified version of the code (B).

       NOTE: this whole part of SAO does not seem to be a significant part of the time spent
    */

    if (extendedTests) {

      // (A) full version with all checks

      for (int j=0;j<ctbH;j++)
        for (int i=0;i<ctbW;i++) {

          if ((sps->pcm_loop_filter_disable_flag &&
               img->get_pcm_flag((xC+i)<<chromashiftW,(yC+j)<<chromashiftH)) ||
              img->get_cu_transquant_bypass((xC+i)<<chromashiftW,(yC+j)<<chromashiftH)) {
            continue;
          }

          // Shifts are a strange thing. On x86, >>x actually computes >>(x%64).
          // But this should never happen, because the maximum bit-depth is 16.
          int pixel = in_img[xC + i + (yC + j) * in_stride];

          // Note: the input pixel value should never exceed the valid range, but it seems that it still does,
          // maybe when there was a decoding error and the pixels have not been filled in correctly.
          // Thus, we have to limit the pixel range to ensure that we have no illegal table access.
          pixel = Clip3(0, maxPixelValue, pixel);

          int bandIdx = bandTable[pixel >> bandShift];

          if (bandIdx>0) {
            int offset = saoinfo->saoOffsetVal[cIdx][bandIdx-1];

            logtrace(LogSAO,"%d %d (%d) offset %d  %x -> %x\n",xC+i,yC+j,bandIdx,
                     offset,
                     in_img[xC+i+(yC+j)*in_stride],
                     in_img[xC+i+(yC+j)*in_stride]+offset);

            out_img[xC+i+(yC+j)*out_stride] = Clip3(0,maxPixelValue,
                                                    in_img[xC+i+(yC+j)*in_stride] + offset);
          }
        }
    }
    else
      {
        // (B) simplified version (only works if no PCM and transquant_bypass is active)

        for (int j=0;j<ctbH;j++)
          for (int i=0;i<ctbW;i++) {

            int pixel = in_img[xC + i + (yC + j) * in_stride];

            // Note: the input pixel value should never exceed the valid range, but it seems that it still does,
            // maybe when there was a decoding error and the pixels have not been filled in correctly.
            // Thus, we have to limit the pixel range to ensure that we have no illegal table access.
            pixel = Clip3(0, maxPixelValue, pixel);

            int bandIdx = bandTable[pixel >> bandShift];

            if (bandIdx>0) {
              int offset = saoinfo->saoOffsetVal[cIdx][bandIdx-1];

              out_img[xC+i+(yC+j)*out_stride] = Clip3(0,maxPixelValue,
                                                      in_img[xC+i+(yC+j)*in_stride] + offset);
            }
          }
      }
  }
}


template <class pixel_t>
void apply_sao(de265_image* img, int xCtb,int yCtb,
               const slice_segment_header* shdr, int cIdx, int nSW,int nSH,
               const pixel_t* in_img,  int in_stride,
               /* */ pixel_t* out_img, int out_stride)
{
  if (img->high_bit_depth(cIdx)) {
    apply_sao_internal<uint16_t>(img,xCtb,yCtb, shdr,cIdx,nSW,nSH,
                                 (uint16_t*)in_img, in_stride,
                                 (uint16_t*)out_img,out_stride);
  }
  else {
    apply_sao_internal<uint8_t>(img,xCtb,yCtb, shdr,cIdx,nSW,nSH,
                                in_img, in_stride,
                                out_img,out_stride);
  }
}


void apply_sample_adaptive_offset(de265_image* img)
{
  const seq_parameter_set& sps = img->get_sps();

  if (sps.sample_adaptive_offset_enabled_flag==0) {
    return;
  }

  de265_image inputCopy;
  de265_error err = inputCopy.copy_image(img);
  if (err != DE265_OK) {
    img->decctx->add_warning(DE265_WARNING_CANNOT_APPLY_SAO_OUT_OF_MEMORY,false);
    return;
  }

  for (int yCtb=0; yCtb<sps.PicHeightInCtbsY; yCtb++)
    for (int xCtb=0; xCtb<sps.PicWidthInCtbsY; xCtb++)
      {
        const slice_segment_header* shdr = img->get_SliceHeaderCtb(xCtb,yCtb);

        if (shdr->slice_sao_luma_flag) {
          apply_sao(img, xCtb,yCtb, shdr, 0, 1<<sps.Log2CtbSizeY, 1<<sps.Log2CtbSizeY,
                    inputCopy.get_image_plane(0), inputCopy.get_image_stride(0),
                    img->get_image_plane(0), img->get_image_stride(0));
        }

        if (shdr->slice_sao_chroma_flag) {
          int nSW = (1<<sps.Log2CtbSizeY) / sps.SubWidthC;
          int nSH = (1<<sps.Log2CtbSizeY) / sps.SubHeightC;

          apply_sao(img, xCtb,yCtb, shdr, 1, nSW,nSH,
                    inputCopy.get_image_plane(1), inputCopy.get_image_stride(1),
                    img->get_image_plane(1), img->get_image_stride(1));

          apply_sao(img, xCtb,yCtb, shdr, 2, nSW,nSH,
                    inputCopy.get_image_plane(2), inputCopy.get_image_stride(2),
                    img->get_image_plane(2), img->get_image_stride(2));
        }
      }
}


void apply_sample_adaptive_offset_sequential(de265_image* img)
{
  const seq_parameter_set& sps = img->get_sps();

  if (sps.sample_adaptive_offset_enabled_flag==0) {
    return;
  }

  int lumaImageSize   = img->get_image_stride(0) * img->get_height(0) * img->get_bytes_per_pixel(0);
  int chromaImageSize = img->get_image_stride(1) * img->get_height(1) * img->get_bytes_per_pixel(1);

  uint8_t* inputCopy = new uint8_t[ libde265_max(lumaImageSize, chromaImageSize) ];
  if (inputCopy == NULL) {
    img->decctx->add_warning(DE265_WARNING_CANNOT_APPLY_SAO_OUT_OF_MEMORY,false);
    return;
  }


  int nChannels = 3;
  if (sps.ChromaArrayType == CHROMA_MONO) { nChannels=1; }

  for (int cIdx=0;cIdx<nChannels;cIdx++) {

    int stride = img->get_image_stride(cIdx);
    int height = img->get_height(cIdx);

    memcpy(inputCopy, img->get_image_plane(cIdx), stride * height * img->get_bytes_per_pixel(cIdx));

    for (int yCtb=0; yCtb<sps.PicHeightInCtbsY; yCtb++)
      for (int xCtb=0; xCtb<sps.PicWidthInCtbsY; xCtb++)
        {
          const slice_segment_header* shdr = img->get_SliceHeaderCtb(xCtb,yCtb);
          if (shdr==NULL) {
	    delete[] inputCopy;
	    return;
	  }

          if (cIdx==0 && shdr->slice_sao_luma_flag) {
            apply_sao(img, xCtb,yCtb, shdr, 0, 1<<sps.Log2CtbSizeY, 1<<sps.Log2CtbSizeY,
                      inputCopy, stride,
                      img->get_image_plane(0), img->get_image_stride(0));
          }

          if (cIdx!=0 && shdr->slice_sao_chroma_flag) {
            int nSW = (1<<sps.Log2CtbSizeY) / sps.SubWidthC;
            int nSH = (1<<sps.Log2CtbSizeY) / sps.SubHeightC;

            apply_sao(img, xCtb,yCtb, shdr, cIdx, nSW,nSH,
                      inputCopy, stride,
                      img->get_image_plane(cIdx), img->get_image_stride(cIdx));
          }
        }
  }

  delete[] inputCopy;
}




class thread_task_sao : public thread_task
{
public:
  int  ctb_y;
  de265_image* img; /* this is where we get the SPS from
                       (either inputImg or outputImg can be a dummy image)
                    */

  de265_image* inputImg;
  de265_image* outputImg;
  int inputProgress;

  virtual void work();
  virtual std::string name() const {
    char buf[100];
    sprintf(buf,"sao-%d",ctb_y);
    return buf;
  }
};


void thread_task_sao::work()
{
  state = Running;
  img->thread_run(this);

  const seq_parameter_set& sps = img->get_sps();

  const int rightCtb = sps.PicWidthInCtbsY-1;
  const int ctbSize  = (1<<sps.Log2CtbSizeY);


  // wait until also the CTB-rows below and above are ready

  img->wait_for_progress(this, rightCtb,ctb_y,  inputProgress);

  if (ctb_y>0) {
    img->wait_for_progress(this, rightCtb,ctb_y-1, inputProgress);
  }

  if (ctb_y+1<sps.PicHeightInCtbsY) {
    img->wait_for_progress(this, rightCtb,ctb_y+1, inputProgress);
  }


  // copy input image to output for this CTB-row

  outputImg->copy_lines_from(inputImg, ctb_y * ctbSize, (ctb_y+1) * ctbSize);


  // process SAO in the CTB-row

  for (int xCtb=0; xCtb<sps.PicWidthInCtbsY; xCtb++)
    {
      const slice_segment_header* shdr = img->get_SliceHeaderCtb(xCtb,ctb_y);
      if (shdr==NULL) {
        break;
      }

      if (shdr->slice_sao_luma_flag) {
        apply_sao(img, xCtb,ctb_y, shdr, 0, ctbSize, ctbSize,
                  inputImg ->get_image_plane(0), inputImg ->get_image_stride(0),
                  outputImg->get_image_plane(0), outputImg->get_image_stride(0));
      }

      if (shdr->slice_sao_chroma_flag) {
        int nSW = ctbSize / sps.SubWidthC;
        int nSH = ctbSize / sps.SubHeightC;

        apply_sao(img, xCtb,ctb_y, shdr, 1, nSW,nSH,
                  inputImg ->get_image_plane(1), inputImg ->get_image_stride(1),
                  outputImg->get_image_plane(1), outputImg->get_image_stride(1));

        apply_sao(img, xCtb,ctb_y, shdr, 2, nSW,nSH,
                  inputImg ->get_image_plane(2), inputImg ->get_image_stride(2),
                  outputImg->get_image_plane(2), outputImg->get_image_stride(2));
      }
    }


  // mark SAO progress

  for (int x=0;x<=rightCtb;x++) {
    const int CtbWidth = sps.PicWidthInCtbsY;
    img->ctb_progress[x+ctb_y*CtbWidth].set_progress(CTB_PROGRESS_SAO);
  }


  state = Finished;
  img->thread_finishes(this);
}


bool add_sao_tasks(image_unit* imgunit, int saoInputProgress)
{
  de265_image* img = imgunit->img;
  const seq_parameter_set& sps = img->get_sps();

  if (sps.sample_adaptive_offset_enabled_flag==0) {
    return false;
  }


  decoder_context* ctx = img->decctx;

  de265_error err = imgunit->sao_output.alloc_image(img->get_width(), img->get_height(),
                                                    img->get_chroma_format(),
                                                    img->get_shared_sps(),
                                                    false,
                                                    img->decctx, //img->encctx,
                                                    img->pts, img->user_data, true);
  if (err != DE265_OK) {
    img->decctx->add_warning(DE265_WARNING_CANNOT_APPLY_SAO_OUT_OF_MEMORY,false);
    return false;
  }

  int nRows = sps.PicHeightInCtbsY;

  int n=0;
  img->thread_start(nRows);

  for (int y=0;y<nRows;y++)
    {
      thread_task_sao* task = new thread_task_sao;

      task->inputImg  = img;
      task->outputImg = &imgunit->sao_output;
      task->img = img;
      task->ctb_y = y;
      task->inputProgress = saoInputProgress;

      imgunit->tasks.push_back(task);
      add_task(&ctx->thread_pool_, task);
      n++;
    }

  /* Currently need barrier here because when are finished, we have to swap the pixel
     data back into the main image. */
  img->wait_for_completion();

  img->exchange_pixel_data_with(imgunit->sao_output);

  return true;
}
