/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%                            DDDD   N   N   GGGG                              %
%                            D   D  NN  N  GS                                 %
%                            D   D  N N N  G  GG                              %
%                            D   D  N  NN  G   G                              %
%                            DDDD   N   N   GGGG                              %
%                                                                             %
%                                                                             %
%                  Read the Digital Negative Image Format                     %
%                                                                             %
%                              Software Design                                %
%                                   Cristy                                    %
%                                 July 1999                                   %
%                                                                             %
%                                                                             %
%  Copyright 1999 ImageMagick Studio LLC, a non-profit organization           %
%  dedicated to making software imaging solutions freely available.           %
%                                                                             %
%  You may not use this file except in compliance with the License.  You may  %
%  obtain a copy of the License at                                            %
%                                                                             %
%    https://imagemagick.org/license/                                         %
%                                                                             %
%  Unless required by applicable law or agreed to in writing, software        %
%  distributed under the License is distributed on an "AS IS" BASIS,          %
%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   %
%  See the License for the specific language governing permissions and        %
%  limitations under the License.                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%
*/
/*
  Include declarations.
*/
#include "magick/studio.h"
#include "magick/blob.h"
#include "magick/blob-private.h"
#include "magick/constitute.h"
#include "magick/delegate.h"
#include "magick/exception.h"
#include "magick/exception-private.h"
#include "magick/geometry.h"
#include "magick/image.h"
#include "magick/image-private.h"
#include "magick/layer.h"
#include "magick/list.h"
#include "magick/log.h"
#include "magick/magick.h"
#include "magick/memory_.h"
#include "magick/monitor.h"
#include "magick/monitor-private.h"
#include "magick/opencl.h"
#include "magick/option.h"
#include "magick/pixel-accessor.h"
#include "magick/profile.h"
#include "magick/property.h"
#include "magick/quantum-private.h"
#include "magick/resource_.h"
#include "magick/static.h"
#include "magick/string_.h"
#include "magick/string-private.h"
#include "magick/module.h"
#include "magick/transform.h"
#include "magick/utility.h"
#include "magick/utility-private.h"
#include "magick/xml-tree.h"
#if defined(MAGICKCORE_RAW_R_DELEGATE)
#ifdef _MSC_VER
#  define _XKEYCHECK_H
#endif
#include <libraw.h>
#endif

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   R e a d D N G I m a g e                                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ReadDNGImage() reads an binary file in the Digital Negative format and
%  returns it.  It allocates the memory necessary for the new Image structure
%  and returns a pointer to the new image.
%
%  The format of the ReadDNGImage method is:
%
%      Image *ReadDNGImage(const ImageInfo *image_info,
%        ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image_info: the image info.
%
%    o exception: return any errors or warnings in this structure.
%
*/

#if defined(MAGICKCORE_WINDOWS_SUPPORT) && defined(MAGICKCORE_OPENCL_SUPPORT)
static void InitializeDcrawOpenCL(ExceptionInfo *exception)
{
  MagickCLEnv
    clEnv;

  (void) SetEnvironmentVariable("DCR_CL_PLATFORM",NULL);
  (void) SetEnvironmentVariable("DCR_CL_DEVICE",NULL);
  (void) SetEnvironmentVariable("DCR_CL_DISABLED",NULL);
  clEnv=GetDefaultOpenCLEnv();
  if (InitOpenCLEnv(clEnv,exception) != MagickFalse)
    {
      char
        *name;

      MagickBooleanType
        opencl_disabled;

      GetMagickOpenCLEnvParam(clEnv,MAGICK_OPENCL_ENV_PARAM_OPENCL_DISABLED,
        sizeof(MagickBooleanType),&opencl_disabled,exception);
      if (opencl_disabled != MagickFalse)
        {
          (void)SetEnvironmentVariable("DCR_CL_DISABLED","1");
          return;
        }
      GetMagickOpenCLEnvParam(clEnv,MAGICK_OPENCL_ENV_PARAM_PLATFORM_VENDOR,
        sizeof(char *),&name,exception);
      if (name != (char *) NULL)
      {
        (void) SetEnvironmentVariable("DCR_CL_PLATFORM",name);
        name=(char *) RelinquishMagickMemory(name);
      }
      GetMagickOpenCLEnvParam(clEnv,MAGICK_OPENCL_ENV_PARAM_DEVICE_NAME,
        sizeof(char *),&name,exception);
      if (name != (char *) NULL)
      {
        (void) SetEnvironmentVariable("DCR_CL_DEVICE",name);
        name=(char *) RelinquishMagickMemory(name);
      }
    }
}
#else
static void InitializeDcrawOpenCL(ExceptionInfo *magick_unused(exception))
{
  magick_unreferenced(exception);
#if defined(MAGICKCORE_WINDOWS_SUPPORT)
  (void) SetEnvironmentVariable("DCR_CL_DISABLED","1");
#endif
}
#endif

#if defined(MAGICKCORE_RAW_R_DELEGATE)
static void SetDNGProperties(Image *image,const libraw_data_t *raw_info,
  ExceptionInfo *exception)
{
  char
    timestamp[MagickTimeExtent];

  (void) SetImageProperty(image,"dng:make",raw_info->idata.make);
  (void) SetImageProperty(image,"dng:camera.model.name",raw_info->idata.model);
  (void) FormatMagickTime(raw_info->other.timestamp,sizeof(timestamp),
    timestamp);
  (void) SetImageProperty(image,"dng:create.date",timestamp);
  (void) FormatImageProperty(image,"dng:iso.setting","%.0g",
    (double) raw_info->other.iso_speed);
#if LIBRAW_COMPILE_CHECK_VERSION_NOTLESS(0,18)
  (void) SetImageProperty(image,"dng:software",raw_info->idata.software);
  if (*raw_info->shootinginfo.BodySerial != '\0')
    (void) SetImageProperty(image,"dng:serial.number",
      raw_info->shootinginfo.BodySerial);
  (void) FormatImageProperty(image,"dng:exposure.time","1/%.0f",
    MagickSafeReciprocal(raw_info->other.shutter));
  (void) FormatImageProperty(image,"dng:f.number","%0.1g",
    (double) raw_info->other.aperture);
  (void) FormatImageProperty(image,"dng:max.aperture.value","%0.1g",
    (double) raw_info->lens.EXIF_MaxAp);
  (void) FormatImageProperty(image,"dng:focal.length","%0.1g mm",
    (double) raw_info->other.focal_len);
  (void) FormatImageProperty(image,"dng:wb.rb.levels","%g %g %g %g",
    (double) raw_info->color.cam_mul[0],(double) raw_info->color.cam_mul[2],
    (double) raw_info->color.cam_mul[1],(double) raw_info->color.cam_mul[3]);
  (void) SetImageProperty(image,"dng:lens.type",
    raw_info->lens.makernotes.LensFeatures_suf);
  (void) FormatImageProperty(image,"dng:lens","%0.1g-%0.1gmm f/%0.1g-%0.1g",
    (double) raw_info->lens.makernotes.MinFocal,
    (double) raw_info->lens.makernotes.MaxFocal,
    (double) raw_info->lens.makernotes.MaxAp4MinFocal,
    (double) raw_info->lens.makernotes.MaxAp4MaxFocal);
  (void) FormatImageProperty(image,"dng:lens.f.stops","%0.2f",
    (double) raw_info->lens.makernotes.LensFStops);
  (void) FormatImageProperty(image,"dng:min.focal.length","%0.1f mm",
    (double) raw_info->lens.makernotes.MinFocal);
  (void) FormatImageProperty(image,"dng:max.focal.length","%0.1g mm",
    (double) raw_info->lens.makernotes.MaxFocal);
  (void) FormatImageProperty(image,"dng:max.aperture.at.min.focal","%0.1g",
    (double) raw_info->lens.makernotes.MaxAp4MinFocal);
  (void) FormatImageProperty(image,"dng:max.aperture.at.max.focal","%0.1g",
    (double) raw_info->lens.makernotes.MaxAp4MaxFocal);
  (void) FormatImageProperty(image,"dng:focal.length.in.35mm.format","%d mm",
    raw_info->lens.FocalLengthIn35mmFormat);
#endif
#if LIBRAW_COMPILE_CHECK_VERSION_NOTLESS(0,20)
  (void) FormatImageProperty(image,"dng:gps.latitude",
    "%.0g deg %.0g' %.2g\" N",
    (double) raw_info->other.parsed_gps.latitude[0],
    (double) raw_info->other.parsed_gps.latitude[1],
    (double) raw_info->other.parsed_gps.latitude[2]);
  (void) FormatImageProperty(image,"dng:gps.longitude",
    "%.0g deg %.0g' %.2g\" W",
    (double) raw_info->other.parsed_gps.longitude[0],
    (double) raw_info->other.parsed_gps.longitude[1],
    (double) raw_info->other.parsed_gps.longitude[2]);
  (void) FormatImageProperty(image,"dng:gps.altitude","%.1g m",
    (double) raw_info->other.parsed_gps.altitude);
#endif
}
#endif

static Image *InvokeDNGDelegate(const ImageInfo *image_info,Image *image,
  ExceptionInfo *exception)
{
  ExceptionInfo
    *sans_exception;

  ImageInfo
    *read_info;

  MagickBooleanType
    status;

  /*
    Convert DNG to TIFF with delegate program.
  */
  (void) DestroyImageList(image);
  InitializeDcrawOpenCL(exception);
  image=AcquireImage(image_info);
  read_info=CloneImageInfo(image_info);
  SetImageInfoBlob(read_info,(void *) NULL,0);
  status=InvokeDelegate(read_info,image,"dng:decode",(char *) NULL,exception);
  image=DestroyImage(image);
  if (status == MagickFalse)
    {
      read_info=DestroyImageInfo(read_info);
      return(image);
    }
  *read_info->magick='\0';
  (void) FormatLocaleString(read_info->filename,MagickPathExtent,"%s.tif",
    read_info->unique);
  sans_exception=AcquireExceptionInfo();
  image=ReadImage(read_info,sans_exception);
  sans_exception=DestroyExceptionInfo(sans_exception);
  if (image != (Image *) NULL)
    (void) CopyMagickString(image->magick,read_info->magick,MagickPathExtent);
  (void) RelinquishUniqueFileResource(read_info->filename);
  read_info=DestroyImageInfo(read_info);
  return(image);
}

#if defined(MAGICKCORE_RAW_R_DELEGATE)
static void SetLibRawParams(const ImageInfo *image_info,Image *image,
  libraw_data_t *raw_info)
{
  const char
    *option;

#if LIBRAW_COMPILE_CHECK_VERSION_NOTLESS(0,20)
#if LIBRAW_COMPILE_CHECK_VERSION_NOTLESS(0,21)
  raw_info->rawparams.max_raw_memory_mb=8192;
#else
  raw_info->params.max_raw_memory_mb=8192;
#endif
  option=GetImageOption(image_info,"dng:max-raw-memory");
  if (option != (const char *) NULL)
#if LIBRAW_COMPILE_CHECK_VERSION_NOTLESS(0,21)
    raw_info->rawparams.max_raw_memory_mb=(unsigned int)
      StringToInteger(option);
#else
    raw_info->params.max_raw_memory_mb=(unsigned int) StringToInteger(option);
#endif
#endif
  raw_info->params.user_flip=0;
  raw_info->params.output_bps=16;
  raw_info->params.use_camera_wb=1;
  option=GetImageOption(image_info,"dng:use-camera-wb");
  if (option == (const char *) NULL)
    option=GetImageOption(image_info,"dng:use_camera_wb");
  if (option != (const char *) NULL)
    raw_info->params.use_camera_wb=IsStringTrue(option) != MagickFalse ? 1 : 0;
  option=GetImageOption(image_info,"dng:use-auto-wb");
  if (option == (const char *) NULL)
    option=GetImageOption(image_info,"dng:use_auto_wb");
  if (option != (const char *) NULL)
    raw_info->params.use_auto_wb=IsStringTrue(option) != MagickFalse ? 1 : 0;
  option=GetImageOption(image_info,"dng:no-auto-bright");
  if (option == (const char *) NULL)
    option=GetImageOption(image_info,"dng:no_auto_bright");
  if (option != (const char *) NULL)
    raw_info->params.no_auto_bright=IsStringTrue(option) != MagickFalse ? 1 : 0;
  option=GetImageOption(image_info,"dng:output-color");
  if (option == (const char *) NULL)
    option=GetImageOption(image_info,"dng:output_color");
  if (option != (const char *) NULL)
    {
      raw_info->params.output_color=StringToInteger(option);
      if (raw_info->params.output_color == 5)
        image->colorspace=XYZColorspace;
    }
#if LIBRAW_COMPILE_CHECK_VERSION_NOTLESS(0,21)
  option=GetImageOption(image_info,"dng:interpolation-quality");
  if (option != (const char *) NULL)
    {
      int
        value;

      value=StringToInteger(option);
      if (value == -1)
        raw_info->params.no_interpolation=1;
      else
        raw_info->params.user_qual=value;
    }
#endif
}

static void LibRawDataError(void *data,const char *magick_unused(file),
#if LIBRAW_COMPILE_CHECK_VERSION(0,22)
  const INT64 offset)
#else
  const int offset)
#endif
{
  magick_unreferenced(file);
  if (offset >= 0)
    {
      ExceptionInfo
        *exception;

      /*
        Value below zero is an EOF and an exception is raised instead.
      */
      exception=(ExceptionInfo *) data;
      (void) ThrowMagickException(exception,GetMagickModule(),
        CorruptImageWarning,"Data corrupted at","`%d'",offset);
  }
}

static void ReadLibRawThumbnail(const ImageInfo *image_info,Image *image,
  libraw_data_t *raw_info,ExceptionInfo *exception)
{
  const char
    *option;

  int
    errcode;

  libraw_processed_image_t
    *thumbnail;

  option=GetImageOption(image_info,"dng:read-thumbnail");
  if (IsStringTrue(option) == MagickFalse)
    return;
  errcode=libraw_unpack_thumb(raw_info);
  if (errcode != LIBRAW_SUCCESS)
    return;
  thumbnail=libraw_dcraw_make_mem_thumb(raw_info,&errcode);
  if (errcode == LIBRAW_SUCCESS)
    {
      StringInfo
        *profile;

      if (thumbnail->type == LIBRAW_IMAGE_JPEG)
        SetImageProperty(image,"dng:thumbnail.type","jpeg");
      else if (thumbnail->type == LIBRAW_IMAGE_BITMAP)
        {
          char
            value[15];

          SetImageProperty(image,"dng:thumbnail.type","bitmap");
          (void) FormatLocaleString(value,sizeof(value),"%hu",thumbnail->bits);
          SetImageProperty(image,"dng:thumbnail.bits",value);
          (void) FormatLocaleString(value,sizeof(value),"%hu",
            thumbnail->colors);
          SetImageProperty(image,"dng:thumbnail.colors",value);
          (void) FormatLocaleString(value,sizeof(value),"%hux%hu",
            thumbnail->width,thumbnail->height);
          SetImageProperty(image,"dng:thumbnail.geometry",value);
        }
      profile=BlobToStringInfo(thumbnail->data,thumbnail->data_size);
      (void) SetImageProfile(image,"dng:thumbnail",profile);
    }
  if (thumbnail != (libraw_processed_image_t *) NULL)
    libraw_dcraw_clear_mem(thumbnail);
}

static OrientationType LibRawFlipToOrientation(int flip)
{
  switch(flip)
  {
    case 5:
    {
      return(LeftBottomOrientation);
    }
    case 8:
    {
      return(LeftTopOrientation);
    }
    default:
    {
      return((OrientationType) flip);
    }
  }
}
#endif

static Image *ReadDNGImage(const ImageInfo *image_info,ExceptionInfo *exception)
{
  Image
    *image;

  MagickBooleanType
    status;

  /*
    Open image file.
  */
  assert(image_info != (const ImageInfo *) NULL);
  assert(image_info->signature == MagickCoreSignature);
  assert(exception != (ExceptionInfo *) NULL);
  assert(exception->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",
      image_info->filename);
  image=AcquireImage(image_info);
  status=OpenBlob(image_info,image,ReadBinaryBlobMode,exception);
  if (status == MagickFalse)
    {
      image=DestroyImageList(image);
      return((Image *) NULL);
    }
  (void) CloseBlob(image);
  if (LocaleCompare(image_info->magick,"DCRAW") == 0)
    return(InvokeDNGDelegate(image_info,image,exception));
#if defined(MAGICKCORE_RAW_R_DELEGATE)
  {
    int
      errcode;

    libraw_data_t
      *raw_info;

    libraw_processed_image_t
      *raw_image;

    ssize_t
      y;

    StringInfo
      *profile;

    unsigned int
      flags;

    unsigned short
      *p;

    errcode=LIBRAW_UNSPECIFIED_ERROR;
    flags=LIBRAW_OPIONS_NO_DATAERR_CALLBACK;
#if LIBRAW_SHLIB_CURRENT < 23
    flags|=LIBRAW_OPIONS_NO_MEMERR_CALLBACK;
#endif
    raw_info=libraw_init(flags);
    if (raw_info == (libraw_data_t *) NULL)
      {
        (void) ThrowMagickException(exception,GetMagickModule(),CoderError,
          libraw_strerror(errcode),"`%s'",image->filename);
        libraw_close(raw_info);
        return(DestroyImageList(image));
      }
    libraw_set_dataerror_handler(raw_info,LibRawDataError,exception);
#if defined(MAGICKCORE_WINDOWS_SUPPORT) && defined(_MSC_VER) && (_MSC_VER > 1310)
    {
      wchar_t
        *path;

      path=NTCreateWidePath(image->filename);
      if (path != (wchar_t *) NULL)
        {
          errcode=libraw_open_wfile(raw_info,path);
          path=(wchar_t *) RelinquishMagickMemory(path);
        }
    }
#else
    errcode=libraw_open_file(raw_info,image->filename);
#endif
    if (errcode != LIBRAW_SUCCESS)
      {
        (void) ThrowMagickException(exception,GetMagickModule(),CoderError,
          libraw_strerror(errcode),"`%s'",image->filename);
        libraw_close(raw_info);
        return(DestroyImageList(image));
      }
    image->columns=raw_info->sizes.width;
    image->rows=raw_info->sizes.height;
    image->page.width=raw_info->sizes.raw_width;
    image->page.height=raw_info->sizes.raw_height;
    image->page.x=raw_info->sizes.left_margin;
    image->page.y=raw_info->sizes.top_margin;
    image->orientation=LibRawFlipToOrientation(raw_info->sizes.flip);
    ReadLibRawThumbnail(image_info,image,raw_info,exception);
    if (image_info->ping != MagickFalse)
      {
        libraw_close(raw_info);
        return(image);
      }
    status=SetImageExtent(image,image->columns,image->rows);
    if (status == MagickFalse)
      {
        libraw_close(raw_info);
        return(image);
      }
    errcode=libraw_unpack(raw_info);
    if (errcode != LIBRAW_SUCCESS)
      {
        (void) ThrowMagickException(exception,GetMagickModule(),CoderError,
          libraw_strerror(errcode),"`%s'",image->filename);
        libraw_close(raw_info);
        return(DestroyImageList(image));
      }
    SetLibRawParams(image_info,image,raw_info);
    errcode=libraw_dcraw_process(raw_info);
    if (errcode != LIBRAW_SUCCESS)
      {
        (void) ThrowMagickException(exception,GetMagickModule(),CoderError,
          libraw_strerror(errcode),"`%s'",image->filename);
        libraw_close(raw_info);
        return(DestroyImageList(image));
      }
    raw_image=libraw_dcraw_make_mem_image(raw_info,&errcode);
    if ((errcode != LIBRAW_SUCCESS) || 
        (raw_image == (libraw_processed_image_t *) NULL) ||
        (raw_image->type != LIBRAW_IMAGE_BITMAP) || (raw_image->bits != 16) ||
        (raw_image->colors < 3) || (raw_image->colors > 4))
      {
        if (raw_image != (libraw_processed_image_t *) NULL)
          libraw_dcraw_clear_mem(raw_image);
        (void) ThrowMagickException(exception,GetMagickModule(),CoderError,
          libraw_strerror(errcode),"`%s'",image->filename);
        libraw_close(raw_info);
        return(DestroyImageList(image));
      }
    if (raw_image->colors < 3)
      {
        image->colorspace=GRAYColorspace;
        image->type=raw_image->colors == 1 ? GrayscaleType : GrayscaleMatteType;
      }
    image->columns=raw_image->width;
    image->rows=raw_image->height;
    image->depth=raw_image->bits;
    image->page.width=raw_info->sizes.width;
    image->page.height=raw_info->sizes.height;
    image->page.x=raw_info->sizes.left_margin;
    image->page.y=raw_info->sizes.top_margin;
    status=SetImageExtent(image,image->columns,image->rows);
    if (status == MagickFalse)
      {
        libraw_dcraw_clear_mem(raw_image);
        libraw_close(raw_info);
        return(DestroyImageList(image));
      }
    p=(unsigned short *) raw_image->data;
    for (y=0; y < (ssize_t) image->rows; y++)
    {
      PixelPacket
        *q;

      ssize_t
        x;

      q=QueueAuthenticPixels(image,0,y,image->columns,1,exception);
      if (q == (PixelPacket *) NULL)
        break;
      for (x=0; x < (ssize_t) image->columns; x++)
      {
        SetPixelRed(q,ScaleShortToQuantum(*p++));
        SetPixelGreen(q,ScaleShortToQuantum(*p++));
        SetPixelBlue(q,ScaleShortToQuantum(*p++));
        if (raw_image->colors > 3)
          SetPixelAlpha(q,ScaleShortToQuantum(*p++));
        q++;
      }
      if (SyncAuthenticPixels(image,exception) == MagickFalse)
        break;
      if (image->previous == (Image *) NULL)
        {
          status=SetImageProgress(image,LoadImageTag,(MagickOffsetType) y,
            image->rows);
          if (status == MagickFalse)
            break;
        }
    }
    libraw_dcraw_clear_mem(raw_image);
    /*
      Set DNG image metadata.
    */
    if (raw_info->color.profile != NULL)
      {
        profile=BlobToStringInfo(raw_info->color.profile,
          raw_info->color.profile_length);
        if (profile != (StringInfo *) NULL)
          {
            SetImageProfile(image,"ICC",profile);
            profile=DestroyStringInfo(profile);
          }
      }
#if LIBRAW_COMPILE_CHECK_VERSION_NOTLESS(0,18)
    if (raw_info->idata.xmpdata != NULL)
      {
        profile=BlobToStringInfo(raw_info->idata.xmpdata,
          raw_info->idata.xmplen);
        if (profile != (StringInfo *) NULL)
          {
            SetImageProfile(image,"XMP",profile);
            profile=DestroyStringInfo(profile);
          }
      }
#endif
    SetDNGProperties(image,raw_info,exception);
    libraw_close(raw_info);
    return(image);
  }
#else
  return(InvokeDNGDelegate(image_info,image,exception));
#endif
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   R e g i s t e r D N G I m a g e                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  RegisterDNGImage() adds attributes for the DNG image format to
%  the list of supported formats.  The attributes include the image format
%  tag, a method to read and/or write the format, whether the format
%  supports the saving of more than one frame to the same file or blob,
%  whether the format supports native in-memory I/O, and a brief
%  description of the format.
%
%  The format of the RegisterDNGImage method is:
%
%      size_t RegisterDNGImage(void)
%
*/
ModuleExport size_t RegisterDNGImage(void)
{
  MagickInfo
    *entry;

  entry=SetMagickInfo("3FR");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Hasselblad CFV/H3D39II");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("ARW");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Sony Alpha Raw Image Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("DNG");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Digital Negative");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("CR2");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Canon Digital Camera Raw Image Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("CR3");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Canon Digital Camera Raw Image Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("CRW");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Canon Digital Camera Raw Image Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("DCR");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Kodak Digital Camera Raw Image File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("DCR");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Raw Photo Decoder (dcraw)");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("ERF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Epson Raw Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("IIQ");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Phase One Raw Image Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("KDC");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Kodak Digital Camera Raw Image Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("K25");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Kodak Digital Camera Raw Image Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("MEF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Mamiya Raw Image File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("MRW");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Sony (Minolta) Raw Image File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("NEF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Nikon Digital SLR Camera Raw Image File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("NRW");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Nikon Digital SLR Camera Raw Image File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("ORF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Olympus Digital Camera Raw Image File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("PEF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Pentax Electronic File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("RAF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Fuji CCD-RAW Graphic File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("RAW");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Raw");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("RMF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Raw Media Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("RW2");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Panasonic Lumix Raw Image");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("SRF");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Sony Raw Format");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("SR2");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->seekable_stream=MagickTrue;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Sony Raw Format 2");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  entry=SetMagickInfo("X3F");
  entry->decoder=(DecodeImageHandler *) ReadDNGImage;
  entry->seekable_stream=MagickTrue;
  entry->blob_support=MagickFalse;
  entry->format_type=ExplicitFormatType;
  entry->description=ConstantString("Sigma Camera RAW Picture File");
  entry->magick_module=ConstantString("DNG");
  (void) RegisterMagickInfo(entry);
  return(MagickImageCoderSignature);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   U n r e g i s t e r D N G I m a g e                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  UnregisterDNGImage() removes format registrations made by the
%  BIM module from the list of supported formats.
%
%  The format of the UnregisterBIMImage method is:
%
%      UnregisterDNGImage(void)
%
*/
ModuleExport void UnregisterDNGImage(void)
{
  (void) UnregisterMagickInfo("X3F");
  (void) UnregisterMagickInfo("SR2");
  (void) UnregisterMagickInfo("SRF");
  (void) UnregisterMagickInfo("RW2");
  (void) UnregisterMagickInfo("RMF");
  (void) UnregisterMagickInfo("RAW");
  (void) UnregisterMagickInfo("RAF");
  (void) UnregisterMagickInfo("PEF");
  (void) UnregisterMagickInfo("ORF");
  (void) UnregisterMagickInfo("NRW");
  (void) UnregisterMagickInfo("NEF");
  (void) UnregisterMagickInfo("MRW");
  (void) UnregisterMagickInfo("MEF");
  (void) UnregisterMagickInfo("K25");
  (void) UnregisterMagickInfo("KDC");
  (void) UnregisterMagickInfo("IIQ");
  (void) UnregisterMagickInfo("ERF");
  (void) UnregisterMagickInfo("DCR");
  (void) UnregisterMagickInfo("CRW");
  (void) UnregisterMagickInfo("CR3");
  (void) UnregisterMagickInfo("CR2");
  (void) UnregisterMagickInfo("DNG");
  (void) UnregisterMagickInfo("DCRAW");
  (void) UnregisterMagickInfo("ARW");
  (void) UnregisterMagickInfo("3FR");
}
