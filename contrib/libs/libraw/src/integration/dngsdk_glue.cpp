/* -*- C++ -*-
 * Copyright 2019-2025 LibRaw LLC (info@libraw.org)
 *
 LibRaw is free software; you can redistribute it and/or modify
 it under the terms of the one of two licenses as you choose:

1. GNU LESSER GENERAL PUBLIC LICENSE version 2.1
   (See file LICENSE.LGPL provided in LibRaw distribution archive for details).

2. COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0
   (See file LICENSE.CDDL provided in LibRaw distribution archive for details).

 */

#define LIBRAW_DNGSDK_CONFLICT 1

#include "../../internal/libraw_cxx_defs.h"

#ifdef USE_DNGSDK
#error #include "dng_host.h"
#error #include "dng_negative.h"
#error #include "dng_simple_image.h"
#error #include "dng_info.h"
#endif


#if defined (USE_GPRSDK) && !defined(USE_DNGSDK)
#error  GPR (GoPro) SDK should be used with Adobe DNG SDK
#endif
#ifdef USE_DNGSDK
#error #include "dng_read_image.h"
#endif
#ifdef USE_GPRSDK
#error #include "gpr_read_image.h"
#endif

#ifdef USE_DNGSDK

void clear_dng_negative(void *p)
{
  if (!p)
    return;
  dng_negative *dn = (dng_negative *)p;
  delete dn;
}

void clear_dng_image(void *p)
{
	if (!p) return;
	dng_image *dimage = (dng_image *)p;
	delete dimage;
}

static dng_ifd* search_single_ifd(const std::vector <dng_ifd *>& v, uint64 offset, int& idx, dng_stream& stream)
{
    idx = -1;
    for (int i = 0; i < v.size(); i++)
    {
        if (!v[i]) continue;
        if (v[i]->fTileOffsetsOffset == offset)
        {
            idx = i;
            return v[i];
        }
        else if (v[i]->fTileOffsetsCount == 1 && v[i]->fTileOffset[0] == offset)
        {
            idx = i;
            return v[i];
        }
		else if (v[i]->fTileOffsetsCount > dng_ifd::kMaxTileInfo)
		{
			uint64 p  = stream.Position();
			stream.SetReadPosition(v[i]->fTileOffsetsOffset);
			int32 oo = stream.TagValue_uint32(v[i]->fTileOffsetsType);
			stream.SetReadPosition(p);
			if (oo == offset)
			{
				idx = i;
				return v[i];
			}
		}
    }
    return NULL;
}

static dng_ifd* search_for_ifd(const dng_info& info, uint64 offset, ushort w, ushort h, int& ifdIndex, dng_stream& stream)
{
    dng_ifd *ret = 0;
    ret = search_single_ifd(info.fIFD, offset, ifdIndex, stream);
    int dummy;
    if (!ret) ret = search_single_ifd(info.fChainedIFD, offset, dummy, stream);
    if (!ret)
    {
        for (int c = 0; !ret && c < info.fChainedSubIFD.size(); c++)
            ret = search_single_ifd(info.fChainedSubIFD[c], offset, dummy, stream);
    }
    if (ret && (ret->fImageLength == h) && ret->fImageWidth == w)
        return ret;
    ifdIndex = -1;
    return 0;
}
#endif

int LibRaw::valid_for_dngsdk()
{
#ifndef USE_DNGSDK
  return 0;
#else
  if (!imgdata.idata.dng_version)
    return 0;

  if (libraw_internal_data.unpacker_data.tiff_compress == 52546) // regardless of flags or use_dngsdk value!
  {
#ifdef qDNGSupportJXL
	  if (dngVersion_Current >= dngVersion_1_7_0_0)
		  return 1;
	  else
#endif
		  return 0; // Old DNG SDK
  }


  // All DNG larger than 2GB - to DNG SDK
  if (libraw_internal_data.internal_data.input->size() > 2147483647ULL)
      return 1;

  if (!strcasecmp(imgdata.idata.make, "Blackmagic") 
      && (libraw_internal_data.unpacker_data.tiff_compress == 7)
      && (libraw_internal_data.unpacker_data.tiff_bps > 8)
      )
      return 0;

  if (libraw_internal_data.unpacker_data.tiff_compress == 34892
	  && libraw_internal_data.unpacker_data.tiff_bps == 8
	  && (libraw_internal_data.unpacker_data.tiff_samples == 3
		  || libraw_internal_data.unpacker_data.tiff_samples == 1
		  || libraw_internal_data.unpacker_data.tiff_samples == 4 )
	  && load_raw == &LibRaw::lossy_dng_load_raw
	  )
  {
      if (!dnghost)
          return 0;
	  try
	  {
        dng_host *host = static_cast<dng_host *>(dnghost);
        libraw_dng_stream stream(libraw_internal_data.internal_data.input);
        AutoPtr<dng_negative> negative;
        negative.Reset(host->Make_dng_negative());
        dng_info info;
        info.Parse(*host, stream);
        info.PostParse(*host);
		if (!info.IsValidDNG())
		{
			imgdata.process_warnings |= LIBRAW_WARN_DNG_NOT_PARSED;
			return 0;
		}
        negative->Parse(*host, stream, info);
        negative->PostParse(*host, stream, info);
        int ifdindex = -1;
        dng_ifd *rawIFD = search_for_ifd(info, libraw_internal_data.unpacker_data.data_offset, imgdata.sizes.raw_width,
                                         imgdata.sizes.raw_height, ifdindex, stream);
        if (rawIFD && ifdindex >= 0 && ifdindex == info.fMainIndex)
          return 1;
        if (rawIFD && ifdindex >= 0 && (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_ADD_PREVIEWS))
          return 1;
        return 0;
	  }
	  catch (...)
	  {
		  imgdata.process_warnings |= LIBRAW_WARN_DNG_NOT_PARSED;
		  return 0;
	  }
  }

#ifdef USE_GPRSDK
  if (libraw_internal_data.unpacker_data.tiff_compress == 9) // regardless of flags or use_dngsdk value!
      return 1;
#endif
  if (!imgdata.rawparams.use_dngsdk)
    return 0;
  if (load_raw == &LibRaw::lossy_dng_load_raw) // WHY??
    return 0;
  if (is_floating_point() && (imgdata.rawparams.use_dngsdk & LIBRAW_DNG_FLOAT))
    return 1;
  if (!imgdata.idata.filters && (imgdata.rawparams.use_dngsdk & LIBRAW_DNG_LINEAR))
    return 1;
  if (libraw_internal_data.unpacker_data.tiff_bps == 8 &&
      (imgdata.rawparams.use_dngsdk & LIBRAW_DNG_8BIT))
    return 1;
  if (libraw_internal_data.unpacker_data.tiff_compress == 8 &&
      (imgdata.rawparams.use_dngsdk & LIBRAW_DNG_DEFLATE))
    return 1;
  if (libraw_internal_data.unpacker_data.tiff_samples == 2)
    return 0; // Always deny 2-samples (old fuji superccd)
  if (imgdata.idata.filters == 9 &&
      (imgdata.rawparams.use_dngsdk & LIBRAW_DNG_XTRANS))
    return 1;
  if (is_fuji_rotated())
    return 0; // refuse
  if (imgdata.rawparams.use_dngsdk & LIBRAW_DNG_OTHER)
    return 1;
  return 0;
#endif
}




int LibRaw::try_dngsdk()
{
#ifdef USE_DNGSDK
  if (!dnghost)
    return LIBRAW_UNSPECIFIED_ERROR;

  dng_host *host = static_cast<dng_host *>(dnghost);

  try
  {
    libraw_dng_stream stream(libraw_internal_data.internal_data.input);

    AutoPtr<dng_negative> negative;
    negative.Reset(host->Make_dng_negative());

    dng_info info;
    info.Parse(*host, stream);
    info.PostParse(*host);

    if (!info.IsValidDNG())
    {
      return LIBRAW_DATA_ERROR;
    }
    negative->Parse(*host, stream, info);
    negative->PostParse(*host, stream, info);
    int ifdindex;
    dng_ifd *rawIFD = search_for_ifd(info,libraw_internal_data.unpacker_data.data_offset,imgdata.sizes.raw_width,imgdata.sizes.raw_height,ifdindex,stream);
	if (!rawIFD)
	{
		imgdata.process_warnings |= LIBRAW_WARN_DNG_NOT_PROCESSED;
		return LIBRAW_DATA_ERROR;
	}

    AutoPtr<dng_simple_image> stage2;
	unsigned stageBits = 0; // 1=> release Stage2, 2=> change Black/Max
	bool zerocopy = false;
	bool isLossy = (rawIFD->fCompression == 34892) || (rawIFD->fCompression == 52546);

    if (
#ifdef USE_GPRSDK
		libraw_internal_data.unpacker_data.tiff_compress != 9 &&
#endif
		(
		((libraw_internal_data.unpacker_data.tiff_compress == 34892 
        && libraw_internal_data.unpacker_data.tiff_bps == 8
        && libraw_internal_data.unpacker_data.tiff_samples == 3
        && load_raw == &LibRaw::lossy_dng_load_raw) // JPEG DNG or JPEG DNG RAW Preview
        || (imgdata.rawparams.options & (LIBRAW_RAWOPTIONS_DNG_STAGE2| LIBRAW_RAWOPTIONS_DNG_STAGE3))
        || ((tiff_ifd[ifdindex].dng_levels.parsedfields & (LIBRAW_DNGFM_OPCODE2| LIBRAW_DNGFM_OPCODE3))
            && (imgdata.rawparams.options & (LIBRAW_RAWOPTIONS_DNG_STAGE2_IFPRESENT | LIBRAW_RAWOPTIONS_DNG_STAGE3_IFPRESENT)))

			|| ((tiff_ifd[ifdindex].dng_levels.parsedfields & (LIBRAW_DNGFM_OPCODE2| LIBRAW_DNGFM_OPCODE3))
            && isLossy && (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_STAGE23_IFPRESENT_JPGJXL))
        )
        && ifdindex >= 0)
		)
    {
        if (info.fMainIndex != ifdindex)
            info.fMainIndex = ifdindex;

		if (rawIFD->fNewSubFileType == 1) // Preview
		{
          dng_read_image reader;
          AutoPtr<dng_image> copy2;
          negative->ReadStage1Image(*host, stream, info); // Read image AND opcodes lists
		  copy2.Reset((dng_simple_image*) negative->Stage1Image());
          host->ApplyOpcodeList(negative->OpcodeList1(), *negative,copy2);
		  stageBits = 1;
		  if ((imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_STAGE2)
			  || ((tiff_ifd[ifdindex].dng_levels.parsedfields & LIBRAW_DNGFM_OPCODE2) && (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_STAGE2_IFPRESENT))
			  || ((tiff_ifd[ifdindex].dng_levels.parsedfields & LIBRAW_DNGFM_OPCODE2) && isLossy && (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_STAGE23_IFPRESENT_JPGJXL))
			  )
		  {
			  host->ApplyOpcodeList(negative->OpcodeList2(), *negative, copy2);
			  stageBits |= 2;
		  }
          stage2.Reset((dng_simple_image *)copy2.Get());
		  copy2.Release();
		}
		else
		{
			negative->ReadStage1Image(*host, stream, info);
			negative->BuildStage2Image(*host);
			imgdata.process_warnings |= LIBRAW_WARN_DNG_STAGE2_APPLIED;
			if ((imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_STAGE3) 
				||
				((tiff_ifd[ifdindex].dng_levels.parsedfields & LIBRAW_DNGFM_OPCODE3) &&
				(imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_STAGE3_IFPRESENT))
				||
                ((tiff_ifd[ifdindex].dng_levels.parsedfields & LIBRAW_DNGFM_OPCODE3) && isLossy &&
                 (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_STAGE23_IFPRESENT_JPGJXL))
				)
			{
				negative->BuildStage3Image(*host);
				stage2.Reset((dng_simple_image*)negative->Stage3Image());
				imgdata.process_warnings |= LIBRAW_WARN_DNG_STAGE3_APPLIED;
			}
			else
				stage2.Reset((dng_simple_image*)negative->Stage2Image());
			stageBits = 3;
		}
    }
    else
    {
        stage2.Reset(new dng_simple_image(rawIFD->Bounds(), rawIFD->fSamplesPerPixel, rawIFD->PixelType(), host->Allocator()));
#ifdef USE_GPRSDK
        if (libraw_internal_data.unpacker_data.tiff_compress == 9)
        {
            gpr_allocator allocator;
            allocator.Alloc = ::malloc;
            allocator.Free = ::free;
            gpr_buffer_auto vc5_image_obj(allocator.Alloc, allocator.Free);

            gpr_read_image reader(&vc5_image_obj);
            reader.Read(*host, *rawIFD, stream, *stage2.Get(), NULL, NULL);
        }
        else
#endif
        {
            dng_read_image reader;
            reader.Read(*host, *rawIFD, stream, *stage2.Get(), NULL, NULL);
        }
    }

    if (stage2->Bounds().W() != S.raw_width ||
        stage2->Bounds().H() != S.raw_height)
    {
		if (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNG_ALLOWSIZECHANGE)
		{
			S.raw_width = S.width = stage2->Bounds().W();
			S.left_margin = 0;
			S.raw_height = S.height = stage2->Bounds().H();
			S.top_margin = 0;
		}
		else
		{
			stage2.Release(); // It holds copy to internal dngnegative
			imgdata.process_warnings |= LIBRAW_WARN_DNG_NOT_PROCESSED;
			return LIBRAW_DATA_ERROR;
		}
    }
	if (stageBits & 2)
	{
		if (stage2->Planes() > 1)
		{
			imgdata.idata.filters = 0;
			imgdata.idata.colors = stage2->Planes();
		}
		// reset BL and whitepoint
		imgdata.color.black = 0;
		memset(imgdata.color.cblack, 0, sizeof(imgdata.color.cblack));
		memset(imgdata.color.linear_max, 0, sizeof(imgdata.color.linear_max));
		imgdata.color.maximum = 0xffff;
	}

    int pplanes = stage2->Planes();
    int ptype = stage2->PixelType();

    dng_pixel_buffer buffer;
    stage2->GetPixelBuffer(buffer);

    int pixels = stage2->Bounds().H() * stage2->Bounds().W() * pplanes;

    if (ptype == ttShort && !(stageBits & 1) &&  !is_curve_linear())
    {
      imgdata.rawdata.raw_alloc = malloc(pixels * TagTypeSize(ptype));
      ushort *src = (ushort *)buffer.fData;
      ushort *dst = (ushort *)imgdata.rawdata.raw_alloc;
      for (int i = 0; i < pixels; i++)
        dst[i] = imgdata.color.curve[src[i]];
      S.raw_pitch = S.raw_width * pplanes * TagTypeSize(ptype);

    }
    else if (ptype == ttByte)
    {
      imgdata.rawdata.raw_alloc = malloc(pixels * TagTypeSize(ttShort));
      unsigned char *src = (unsigned char *)buffer.fData;
      ushort *dst = (ushort *)imgdata.rawdata.raw_alloc;
      if (is_curve_linear())
      {
        memmove(dst, src, pixels * TagTypeSize(ptype));
      }
      else
      {
        for (int i = 0; i < pixels; i++)
          dst[i] = imgdata.color.curve[src[i]];
      }
      S.raw_pitch = S.raw_width * pplanes * TagTypeSize(ttShort);
    }
    else
    {
      // Alloc
      if ((imgdata.rawparams.options & LIBRAW_RAWOPTIONS_DNGSDK_ZEROCOPY) && !(stageBits & 1))
      {
        zerocopy = true;
      }
      else
      {
        imgdata.rawdata.raw_alloc = malloc(pixels * TagTypeSize(ptype));
        memmove(imgdata.rawdata.raw_alloc, buffer.fData,
                pixels * TagTypeSize(ptype));
      }
      S.raw_pitch = S.raw_width * pplanes * TagTypeSize(ptype);
    }

    if (stageBits & 1)
        stage2.Release();

    if ((ptype == ttFloat) && (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_CONVERTFLOAT_TO_INT))
        zerocopy = true;

    if (zerocopy)
    {
      switch (ptype)
      {
      case ttFloat:
        if (pplanes == 1)
          imgdata.rawdata.float_image = (float *)buffer.fData;
        else if (pplanes == 3)
          imgdata.rawdata.float3_image = (float(*)[3])buffer.fData;
        else if (pplanes == 4)
          imgdata.rawdata.float4_image = (float(*)[4])buffer.fData;
        break;

      case ttShort:
        if (pplanes == 1)
          imgdata.rawdata.raw_image = (ushort *)buffer.fData;
        else if (pplanes == 3)
          imgdata.rawdata.color3_image = (ushort(*)[3])buffer.fData;
        else if (pplanes == 4)
          imgdata.rawdata.color4_image = (ushort(*)[4])buffer.fData;
        break;
      default:
        /* do nothing */
        break;
      }
    }
    else
    {
      switch (ptype)
      {
      case ttFloat:
        if (pplanes == 1)
          imgdata.rawdata.float_image = (float *)imgdata.rawdata.raw_alloc;
        else if (pplanes == 3)
          imgdata.rawdata.float3_image = (float(*)[3])imgdata.rawdata.raw_alloc;
        else if (pplanes == 4)
          imgdata.rawdata.float4_image = (float(*)[4])imgdata.rawdata.raw_alloc;
        break;

      case ttByte:
      case ttShort:
        if (pplanes == 1)
          imgdata.rawdata.raw_image = (ushort *)imgdata.rawdata.raw_alloc;
        else if (pplanes == 3)
          imgdata.rawdata.color3_image =
              (ushort(*)[3])imgdata.rawdata.raw_alloc;
        else if (pplanes == 4)
          imgdata.rawdata.color4_image =
              (ushort(*)[4])imgdata.rawdata.raw_alloc;
        break;
      default:
        /* do nothing */
        break;
      }
    }

    if ((ptype == ttFloat) && (imgdata.rawparams.options & LIBRAW_RAWOPTIONS_CONVERTFLOAT_TO_INT))
    {
        convertFloatToInt();
        zerocopy = false;
    }

    if (zerocopy)
    {
      dng_negative *stolen = negative.Release();
      dngnegative = stolen;
      dng_simple_image *simage = stage2.Release();
      dngimage = simage;
    }
  }
  catch (...)
  {
    imgdata.process_warnings |= LIBRAW_WARN_DNG_NOT_PROCESSED;
    return LIBRAW_UNSPECIFIED_ERROR;
  }
  
  int ret = (dngnegative || imgdata.rawdata.raw_alloc) ? LIBRAW_SUCCESS : LIBRAW_UNSPECIFIED_ERROR;
  if(ret != LIBRAW_SUCCESS)
	imgdata.process_warnings |= LIBRAW_WARN_DNG_NOT_PROCESSED;
  return ret;
#else
  return LIBRAW_UNSPECIFIED_ERROR;
#endif
}
void LibRaw::set_dng_host(void *p)
{
#ifdef USE_DNGSDK
  dnghost = p;
#endif
}
