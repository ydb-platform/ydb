/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%               FFFFF   OOO   U   U  RRRR   IIIII  EEEEE  RRRR                %
%               F      O   O  U   U  R   R    I    E      R   R               %
%               FFF    O   O  U   U  RRRR     I    EEE    RRRR                %
%               F      O   O  U   U  R R      I    E      R R                 %
%               F       OOO    UUU   R  R   IIIII  EEEEE  R  R                %
%                                                                             %
%                                                                             %
%                MagickCore Discrete Fourier Transform Methods                %
%                                                                             %
%                              Software Design                                %
%                                Sean Burke                                   %
%                               Fred Weinhaus                                 %
%                                   Cristy                                    %
%                                 July 2009                                   %
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
%
*/

/*
  Include declarations.
*/
#include "magick/studio.h"
#include "magick/artifact.h"
#include "magick/attribute.h"
#include "magick/blob.h"
#include "magick/cache.h"
#include "magick/image.h"
#include "magick/image-private.h"
#include "magick/list.h"
#include "magick/fourier.h"
#include "magick/log.h"
#include "magick/memory_.h"
#include "magick/monitor.h"
#include "magick/monitor-private.h"
#include "magick/pixel-accessor.h"
#include "magick/pixel-private.h"
#include "magick/property.h"
#include "magick/quantum-private.h"
#include "magick/resource_.h"
#include "magick/string-private.h"
#include "magick/thread-private.h"
#if defined(MAGICKCORE_FFTW_DELEGATE)
#if defined(_MSC_VER)
#define ENABLE_FFTW_DELEGATE
#elif !defined(__cplusplus) && !defined(c_plusplus)
#define ENABLE_FFTW_DELEGATE
#endif
#endif
#if defined(ENABLE_FFTW_DELEGATE)
#if defined(MAGICKCORE_HAVE_COMPLEX_H)
#include <complex.h>
#endif
#include <fftw3.h>
#if !defined(MAGICKCORE_HAVE_CABS)
#define cabs(z)  (sqrt(z[0]*z[0]+z[1]*z[1]))
#endif
#if !defined(MAGICKCORE_HAVE_CARG)
#define carg(z)  (atan2(cimag(z),creal(z)))
#endif
#if !defined(MAGICKCORE_HAVE_CIMAG)
#define cimag(z)  (z[1])
#endif
#if !defined(MAGICKCORE_HAVE_CREAL)
#define creal(z)  (z[0])
#endif
#endif

/*
  Typedef declarations.
*/
typedef struct _FourierInfo
{
  ChannelType
    channel;

  MagickBooleanType
    modulus;

  size_t
    width,
    height;

  ssize_t
    center;
} FourierInfo;

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     C o m p l e x I m a g e s                                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ComplexImages() performs complex mathematics on an image sequence.
%
%  The format of the ComplexImages method is:
%
%      MagickBooleanType ComplexImages(Image *images,const ComplexOperator op,
%        ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o op: A complex operator.
%
%    o exception: return any errors or warnings in this structure.
%
*/
MagickExport Image *ComplexImages(const Image *images,const ComplexOperator op,
  ExceptionInfo *exception)
{
#define ComplexImageTag  "Complex/Image"

  CacheView
    *Ai_view,
    *Ar_view,
    *Bi_view,
    *Br_view,
    *Ci_view,
    *Cr_view;

  const char
    *artifact;

  const Image
    *Ai_image,
    *Ar_image,
    *Bi_image,
    *Br_image;

  double
    snr;

  Image
    *Ci_image,
    *complex_images,
    *Cr_image,
    *image;

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  size_t
    columns,
    rows;

  ssize_t
    y;

  assert(images != (Image *) NULL);
  assert(images->signature == MagickCoreSignature);
  assert(exception != (ExceptionInfo *) NULL);
  assert(exception->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",images->filename);
  if (images->next == (Image *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),ImageError,
        "ImageSequenceRequired","`%s'",images->filename);
      return((Image *) NULL);
    }
  image=CloneImage(images,0,0,MagickTrue,exception);
  if (image == (Image *) NULL)
    return((Image *) NULL);
  if (SetImageStorageClass(image,DirectClass) == MagickFalse)
    {
      image=DestroyImageList(image);
      return(image);
    }
  image->depth=32UL;
  complex_images=NewImageList();
  AppendImageToList(&complex_images,image);
  image=CloneImage(images->next,0,0,MagickTrue,exception);
  if (image == (Image *) NULL)
    {
      complex_images=DestroyImageList(complex_images);
      return(complex_images);
    }
  if (SetImageStorageClass(image,DirectClass) == MagickFalse)
    {
      image=DestroyImageList(image);
      return(image);
    }
  image->depth=32UL;
  AppendImageToList(&complex_images,image);
  /*
    Apply complex mathematics to image pixels.
  */
  artifact=GetImageArtifact(image,"complex:snr");
  snr=0.0;
  if (artifact != (const char *) NULL)
    snr=StringToDouble(artifact,(char **) NULL);
  Ar_image=images;
  Ai_image=images->next;
  Br_image=images;
  Bi_image=images->next;
  if ((images->next->next != (Image *) NULL) &&
      (images->next->next->next != (Image *) NULL))
    {
      Br_image=images->next->next;
      Bi_image=images->next->next->next;
    }
  Cr_image=complex_images;
  Ci_image=complex_images->next;
  Ar_view=AcquireVirtualCacheView(Ar_image,exception);
  Ai_view=AcquireVirtualCacheView(Ai_image,exception);
  Br_view=AcquireVirtualCacheView(Br_image,exception);
  Bi_view=AcquireVirtualCacheView(Bi_image,exception);
  Cr_view=AcquireAuthenticCacheView(Cr_image,exception);
  Ci_view=AcquireAuthenticCacheView(Ci_image,exception);
  status=MagickTrue;
  progress=0;
  columns=MagickMin(Cr_image->columns,Ci_image->columns);
  rows=MagickMin(Cr_image->rows,Ci_image->rows);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(progress,status) \
    magick_number_threads(Cr_image,complex_images,rows,1L)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    const PixelPacket
      *magick_restrict Ai,
      *magick_restrict Ar,
      *magick_restrict Bi,
      *magick_restrict Br;

    PixelPacket
      *magick_restrict Ci,
      *magick_restrict Cr;

    ssize_t
      x;

    if (status == MagickFalse)
      continue;
    Ar=GetCacheViewVirtualPixels(Ar_view,0,y,columns,1,exception);
    Ai=GetCacheViewVirtualPixels(Ai_view,0,y,columns,1,exception);
    Br=GetCacheViewVirtualPixels(Br_view,0,y,columns,1,exception);
    Bi=GetCacheViewVirtualPixels(Bi_view,0,y,columns,1,exception);
    Cr=QueueCacheViewAuthenticPixels(Cr_view,0,y,columns,1,exception);
    Ci=QueueCacheViewAuthenticPixels(Ci_view,0,y,columns,1,exception);
    if ((Ar == (const PixelPacket *) NULL) ||
        (Ai == (const PixelPacket *) NULL) ||
        (Br == (const PixelPacket *) NULL) ||
        (Bi == (const PixelPacket *) NULL) ||
        (Cr == (PixelPacket *) NULL) || (Ci == (PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    for (x=0; x < (ssize_t) columns; x++)
    {
      DoublePixelPacket
        ai = { (double) (QuantumScale*(double) GetPixelRed(Ai)),
               (double) (QuantumScale*(double) GetPixelGreen(Ai)),
               (double) (QuantumScale*(double) GetPixelBlue(Ai)),
               (double) (image->matte != MagickFalse ? QuantumScale*
               (double) GetPixelOpacity(Ai) : (double) OpaqueOpacity), 0 },
        ar = { (double) (QuantumScale*(double) GetPixelRed(Ar)),
               (double) (QuantumScale*(double) GetPixelGreen(Ar)),
               (double) (QuantumScale*(double) GetPixelBlue(Ar)),
               (double) (image->matte != MagickFalse ? QuantumScale*
               (double) GetPixelOpacity(Ar) : (double) OpaqueOpacity), 0 },
        bi = { (double) (QuantumScale*(double) GetPixelRed(Bi)),
               (double) (QuantumScale*(double) GetPixelGreen(Bi)),
               (double) (QuantumScale*(double) GetPixelBlue(Bi)),
               (double) (image->matte != MagickFalse ? QuantumScale*
               (double) GetPixelOpacity(Bi) : (double) OpaqueOpacity), 0 },
        br = { (double) (QuantumScale*(double) GetPixelRed(Br)),
               (double) (QuantumScale*(double) GetPixelGreen(Br)),
               (double) (QuantumScale*(double) GetPixelBlue(Br)),
               (double) (image->matte != MagickFalse ? QuantumScale*
               (double) GetPixelOpacity(Br) : (double) OpaqueOpacity), 0 },
        ci,
        cr;

      switch (op)
      {
        case AddComplexOperator:
        {
          cr.red=ar.red+br.red;
          ci.red=ai.red+bi.red;
          cr.green=ar.green+br.green;
          ci.green=ai.green+bi.green;
          cr.blue=ar.blue+br.blue;
          ci.blue=ai.blue+bi.blue;
          cr.opacity=ar.opacity+br.opacity;
          ci.opacity=ai.opacity+bi.opacity;
          break;
        }
        case ConjugateComplexOperator:
        default:
        {
          cr.red=ar.red;
          ci.red=(-ai.red);
          cr.green=ar.green;
          ci.green=(-ai.green);
          cr.blue=ar.blue;
          ci.blue=(-ai.blue);
          cr.opacity=ar.opacity;
          ci.opacity=(-ai.opacity);
          break;
        }
        case DivideComplexOperator:
        {
          double
            gamma;

          gamma=MagickSafeReciprocal((double) br.red*(double) br.red+
            (double) bi.red*(double) bi.red+snr);
          cr.red=gamma*((double) ar.red*(double) br.red+(double) ai.red*
            (double) bi.red);
          ci.red=gamma*((double) ai.red*(double) br.red-(double) ar.red*
             (double) bi.red);
          gamma=MagickSafeReciprocal((double) br.green*(double) br.green+
            (double) bi.green*(double) bi.green+snr);
          cr.green=gamma*((double) ar.green*(double) br.green+
            (double) ai.green*(double) bi.green);
          ci.green=gamma*((double) ai.green*(double) br.green-
            (double) ar.green*(double) bi.green);
          gamma=MagickSafeReciprocal((double) br.blue*(double) br.blue+
            (double) bi.blue*(double) bi.blue+snr);
          cr.blue=gamma*((double) ar.blue*(double) br.blue+
            (double) ai.blue*(double) bi.blue);
          ci.blue=gamma*((double) ai.blue*(double) br.blue-
            (double) ar.blue*(double) bi.blue);
          gamma=MagickSafeReciprocal((double) br.opacity*(double) br.opacity+
            (double) bi.opacity*(double) bi.opacity+snr);
          cr.opacity=gamma*((double) ar.opacity*(double) br.opacity+
           (double) ai.opacity*(double) bi.opacity);
          ci.opacity=gamma*((double) ai.opacity*(double) br.opacity-
            (double) ar.opacity*(double) bi.opacity);
          break;
        }
        case MagnitudePhaseComplexOperator:
        {
          cr.red=sqrt((double) ar.red*(double) ar.red+(double) ai.red*
            (double) ai.red);
          ci.red=atan2((double) ai.red,(double) ar.red)/(2.0*MagickPI)+0.5;
          cr.green=sqrt((double) ar.green*(double) ar.green+(double) ai.green*
            (double) ai.green);
          ci.green=atan2((double) ai.green,(double) ar.green)/(2.0*MagickPI)+
            0.5;
          cr.blue=sqrt((double) ar.blue*(double) ar.blue+(double) ai.blue*
            (double) ai.blue);
          ci.blue=atan2((double) ai.blue,(double) ar.blue)/(2.0*MagickPI)+0.5;
          cr.opacity=sqrt((double) ar.opacity*(double) ar.opacity+
            (double) ai.opacity*(double) ai.opacity);
          ci.opacity=atan2((double) ai.opacity,(double) ar.opacity)/
            (2.0*MagickPI)+0.5;
          break;
        }
        case MultiplyComplexOperator:
        {
          cr.red=((double) ar.red*(double) br.red-(double) ai.red*
            (double) bi.red);
          ci.red=((double) ai.red*(double) br.red+(double) ar.red*
            (double) bi.red);
          cr.green=((double) ar.green*(double) br.green-(double) ai.green*
            (double) bi.green);
          ci.green=((double) ai.green*(double) br.green+(double) ar.green*
            (double) bi.green);
          cr.blue=((double) ar.blue*(double) br.blue-(double) ai.blue*
            (double) bi.blue);
          ci.blue=((double) ai.blue*(double) br.blue+(double) ar.blue*
            (double) bi.blue);
          cr.opacity=((double) ar.opacity*(double) br.opacity-
            (double) ai.opacity*(double) bi.opacity);
          ci.opacity=((double) ai.opacity*(double) br.opacity+
            (double) ar.opacity*(double) bi.opacity);
          break;
        }
        case RealImaginaryComplexOperator:
        {
          cr.red=(double) ar.red*cos(2.0*MagickPI*((double) ai.red-0.5));
          ci.red=(double) ar.red*sin(2.0*MagickPI*((double) ai.red-0.5));
          cr.green=(double) ar.green*cos(2.0*MagickPI*((double) ai.green-0.5));
          ci.green=(double) ar.green*sin(2.0*MagickPI*((double) ai.green-0.5));
          cr.blue=(double) ar.blue*cos(2.0*MagickPI*((double) ai.blue-0.5));
          ci.blue=(double) ar.blue*sin(2.0*MagickPI*((double) ai.blue-0.5));
          cr.opacity=(double) ar.opacity*cos(2.0*MagickPI*((double) ai.opacity-
            0.5));
          ci.opacity=(double) ar.opacity*sin(2.0*MagickPI*((double) ai.opacity-
            0.5));
          break;
        }
        case SubtractComplexOperator:
        {
          cr.red=ar.red-br.red;
          ci.red=ai.red-bi.red;
          cr.green=ar.green-br.green;
          ci.green=ai.green-bi.green;
          cr.blue=ar.blue-br.blue;
          ci.blue=ai.blue-bi.blue;
          cr.opacity=ar.opacity-br.opacity;
          ci.opacity=ai.opacity-bi.opacity;
          break;
        }
      }
      Cr->red=(MagickRealType) QuantumRange*(MagickRealType) cr.red;
      Ci->red=(MagickRealType) QuantumRange*(MagickRealType) ci.red;
      Cr->green=(MagickRealType) QuantumRange*(MagickRealType) cr.green;
      Ci->green=(MagickRealType) QuantumRange*(MagickRealType) ci.green;
      Cr->blue=(MagickRealType) QuantumRange*(MagickRealType) cr.blue;
      Ci->blue=(MagickRealType) QuantumRange*(MagickRealType) ci.blue;
      if (images->matte != MagickFalse)
        {
          Cr->opacity=(MagickRealType) QuantumRange*(MagickRealType) cr.opacity;
          Ci->opacity=(MagickRealType) QuantumRange*(MagickRealType) ci.opacity;
        }
      Ar++;
      Ai++;
      Br++;
      Bi++;
      Cr++;
      Ci++;
    }
    if (SyncCacheViewAuthenticPixels(Ci_view,exception) == MagickFalse)
      status=MagickFalse;
    if (SyncCacheViewAuthenticPixels(Cr_view,exception) == MagickFalse)
      status=MagickFalse;
    if (images->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

#if defined(MAGICKCORE_OPENMP_SUPPORT)
        #pragma omp atomic
#endif
        progress++;
        proceed=SetImageProgress(images,ComplexImageTag,progress,images->rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  Cr_view=DestroyCacheView(Cr_view);
  Ci_view=DestroyCacheView(Ci_view);
  Br_view=DestroyCacheView(Br_view);
  Bi_view=DestroyCacheView(Bi_view);
  Ar_view=DestroyCacheView(Ar_view);
  Ai_view=DestroyCacheView(Ai_view);
  if (status == MagickFalse)
    complex_images=DestroyImageList(complex_images);
  return(complex_images);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     F o r w a r d F o u r i e r T r a n s f o r m I m a g e                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ForwardFourierTransformImage() implements the discrete Fourier transform
%  (DFT) of the image either as a magnitude / phase or real / imaginary image
%  pair.
%
%  The format of the ForwardFourierTransformImage method is:
%
%      Image *ForwardFourierTransformImage(const Image *image,
%        const MagickBooleanType modulus,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o modulus: if true, return as transform as a magnitude / phase pair
%      otherwise a real / imaginary image pair.
%
%    o exception: return any errors or warnings in this structure.
%
*/

#if defined(ENABLE_FFTW_DELEGATE)

static MagickBooleanType RollFourier(const size_t width,const size_t height,
  const ssize_t x_offset,const ssize_t y_offset,double *roll_pixels)
{
  double
    *source_pixels;

  MemoryInfo
    *source_info;

  ssize_t
    i,
    u,
    v,
    x,
    y;

  /*
    Move zero frequency (DC, average color) from (0,0) to (width/2,height/2).
  */
  source_info=AcquireVirtualMemory(width,height*sizeof(*source_pixels));
  if (source_info == (MemoryInfo *) NULL)
    return(MagickFalse);
  source_pixels=(double *) GetVirtualMemoryBlob(source_info);
  i=0L;
  for (y=0L; y < (ssize_t) height; y++)
  {
    if (y_offset < 0L)
      v=((y+y_offset) < 0L) ? y+y_offset+(ssize_t) height : y+y_offset;
    else
      v=((y+y_offset) > ((ssize_t) height-1L)) ? y+y_offset-(ssize_t) height :
        y+y_offset;
    for (x=0L; x < (ssize_t) width; x++)
    {
      if (x_offset < 0L)
        u=((x+x_offset) < 0L) ? x+x_offset+(ssize_t) width : x+x_offset;
      else
        u=((x+x_offset) > ((ssize_t) width-1L)) ? x+x_offset-(ssize_t) width :
          x+x_offset;
      source_pixels[v*width+u]=roll_pixels[i++];
    }
  }
  (void) memcpy(roll_pixels,source_pixels,height*width*sizeof(*source_pixels));
  source_info=RelinquishVirtualMemory(source_info);
  return(MagickTrue);
}

static MagickBooleanType ForwardQuadrantSwap(const size_t width,
  const size_t height,double *source_pixels,double *forward_pixels)
{
  MagickBooleanType
    status;

  ssize_t
    center,
    x,
    y;

  /*
    Swap quadrants.
  */
  center=(ssize_t) (width/2L)+1L;
  status=RollFourier((size_t) center,height,0L,(ssize_t) height/2L,
    source_pixels);
  if (status == MagickFalse)
    return(MagickFalse);
  for (y=0L; y < (ssize_t) height; y++)
    for (x=0L; x < (ssize_t) (width/2L); x++)
      forward_pixels[y*width+x+width/2L]=source_pixels[y*center+x];
  for (y=1; y < (ssize_t) height; y++)
    for (x=0L; x < (ssize_t) (width/2L); x++)
      forward_pixels[(height-y)*width+width/2L-x-1L]=
        source_pixels[y*center+x+1L];
  for (x=0L; x < (ssize_t) (width/2L); x++)
    forward_pixels[width/2L-x-1L]=source_pixels[x+1L];
  return(MagickTrue);
}

static void CorrectPhaseLHS(const size_t width,const size_t height,
  double *fourier_pixels)
{
  ssize_t
    x,
    y;

  for (y=0L; y < (ssize_t) height; y++)
    for (x=0L; x < (ssize_t) (width/2L); x++)
      fourier_pixels[y*width+x]*=(-1.0);
}

static MagickBooleanType ForwardFourier(const FourierInfo *fourier_info,
  Image *image,double *magnitude,double *phase,ExceptionInfo *exception)
{
  CacheView
    *magnitude_view,
    *phase_view;

  double
    *magnitude_pixels,
    *phase_pixels;

  Image
    *magnitude_image,
    *phase_image;

  MagickBooleanType
    status;

  MemoryInfo
    *magnitude_info,
    *phase_info;

  IndexPacket
    *indexes;

  PixelPacket
    *q;

  ssize_t
    i,
    x,
    y;

  magnitude_image=GetFirstImageInList(image);
  phase_image=GetNextImageInList(image);
  if (phase_image == (Image *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),ImageError,
        "ImageSequenceRequired","`%s'",image->filename);
      return(MagickFalse);
    }
  /*
    Create "Fourier Transform" image from constituent arrays.
  */
  magnitude_info=AcquireVirtualMemory(fourier_info->width,fourier_info->height*
    sizeof(*magnitude_pixels));
  phase_info=AcquireVirtualMemory(fourier_info->width,fourier_info->height*
    sizeof(*phase_pixels));
  if ((magnitude_info == (MemoryInfo *) NULL) ||
      (phase_info == (MemoryInfo *) NULL))
    {
      if (phase_info != (MemoryInfo *) NULL)
        phase_info=RelinquishVirtualMemory(phase_info);
      if (magnitude_info != (MemoryInfo *) NULL)
        magnitude_info=RelinquishVirtualMemory(magnitude_info);
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",image->filename);
      return(MagickFalse);
    }
  magnitude_pixels=(double *) GetVirtualMemoryBlob(magnitude_info);
  (void) memset(magnitude_pixels,0,fourier_info->width*
    fourier_info->height*sizeof(*magnitude_pixels));
  phase_pixels=(double *) GetVirtualMemoryBlob(phase_info);
  (void) memset(phase_pixels,0,fourier_info->width*
    fourier_info->height*sizeof(*phase_pixels));
  status=ForwardQuadrantSwap(fourier_info->width,fourier_info->height,
    magnitude,magnitude_pixels);
  if (status != MagickFalse)
    status=ForwardQuadrantSwap(fourier_info->width,fourier_info->height,phase,
      phase_pixels);
  CorrectPhaseLHS(fourier_info->width,fourier_info->height,phase_pixels);
  if (fourier_info->modulus != MagickFalse)
    {
      i=0L;
      for (y=0L; y < (ssize_t) fourier_info->height; y++)
        for (x=0L; x < (ssize_t) fourier_info->width; x++)
        {
          phase_pixels[i]/=(2.0*MagickPI);
          phase_pixels[i]+=0.5;
          i++;
        }
    }
  magnitude_view=AcquireAuthenticCacheView(magnitude_image,exception);
  i=0L;
  for (y=0L; y < (ssize_t) fourier_info->height; y++)
  {
    q=GetCacheViewAuthenticPixels(magnitude_view,0L,y,fourier_info->width,1UL,
      exception);
    if (q == (PixelPacket *) NULL)
      break;
    indexes=GetCacheViewAuthenticIndexQueue(magnitude_view);
    for (x=0L; x < (ssize_t) fourier_info->width; x++)
    {
      switch (fourier_info->channel)
      {
        case RedChannel:
        default:
        {
          SetPixelRed(q,ClampToQuantum((MagickRealType) QuantumRange*magnitude_pixels[i]));
          break;
        }
        case GreenChannel:
        {
          SetPixelGreen(q,ClampToQuantum((MagickRealType) QuantumRange*magnitude_pixels[i]));
          break;
        }
        case BlueChannel:
        {
          SetPixelBlue(q,ClampToQuantum((MagickRealType) QuantumRange*magnitude_pixels[i]));
          break;
        }
        case OpacityChannel:
        {
          SetPixelOpacity(q,ClampToQuantum((MagickRealType) QuantumRange*magnitude_pixels[i]));
          break;
        }
        case IndexChannel:
        {
          SetPixelIndex(indexes+x,ClampToQuantum((MagickRealType) QuantumRange*
            magnitude_pixels[i]));
          break;
        }
        case GrayChannels:
        {
          SetPixelGray(q,ClampToQuantum((MagickRealType) QuantumRange*magnitude_pixels[i]));
          break;
        }
      }
      i++;
      q++;
    }
    status=SyncCacheViewAuthenticPixels(magnitude_view,exception);
    if (status == MagickFalse)
      break;
  }
  magnitude_view=DestroyCacheView(magnitude_view);
  i=0L;
  phase_view=AcquireAuthenticCacheView(phase_image,exception);
  for (y=0L; y < (ssize_t) fourier_info->height; y++)
  {
    q=GetCacheViewAuthenticPixels(phase_view,0L,y,fourier_info->width,1UL,
      exception);
    if (q == (PixelPacket *) NULL)
      break;
    indexes=GetCacheViewAuthenticIndexQueue(phase_view);
    for (x=0L; x < (ssize_t) fourier_info->width; x++)
    {
      switch (fourier_info->channel)
      {
        case RedChannel:
        default:
        {
          SetPixelRed(q,ClampToQuantum((MagickRealType) QuantumRange*phase_pixels[i]));
          break;
        }
        case GreenChannel:
        {
          SetPixelGreen(q,ClampToQuantum((MagickRealType) QuantumRange*phase_pixels[i]));
          break;
        }
        case BlueChannel:
        {
          SetPixelBlue(q,ClampToQuantum((MagickRealType) QuantumRange*phase_pixels[i]));
          break;
        }
        case OpacityChannel:
        {
          SetPixelOpacity(q,ClampToQuantum((MagickRealType) QuantumRange*phase_pixels[i]));
          break;
        }
        case IndexChannel:
        {
          SetPixelIndex(indexes+x,ClampToQuantum((MagickRealType) QuantumRange*phase_pixels[i]));
          break;
        }
        case GrayChannels:
        {
          SetPixelGray(q,ClampToQuantum((MagickRealType) QuantumRange*phase_pixels[i]));
          break;
        }
      }
      i++;
      q++;
    }
    status=SyncCacheViewAuthenticPixels(phase_view,exception);
    if (status == MagickFalse)
      break;
   }
  phase_view=DestroyCacheView(phase_view);
  phase_info=RelinquishVirtualMemory(phase_info);
  magnitude_info=RelinquishVirtualMemory(magnitude_info);
  return(status);
}

static MagickBooleanType ForwardFourierTransform(FourierInfo *fourier_info,
  const Image *image,double *magnitude_pixels,double *phase_pixels,
  ExceptionInfo *exception)
{
  CacheView
    *image_view;

  const char
    *value;

  double
    *source_pixels;

  fftw_complex
    *forward_pixels;

    
  fftw_plan
    fftw_r2c_plan;

  MemoryInfo
    *forward_info,
    *source_info;

  const IndexPacket
    *indexes;

  const PixelPacket
    *p;

  ssize_t
    i,
    x,
    y;

  /*
    Generate the forward Fourier transform.
  */
  if ((fourier_info->width >= INT_MAX) || (fourier_info->height >= INT_MAX))
    {
      (void) ThrowMagickException(exception,GetMagickModule(),ImageError,
        "WidthOrHeightExceedsLimit","`%s'",image->filename);
      return(MagickFalse);
    }
  source_info=AcquireVirtualMemory(fourier_info->width,fourier_info->height*
    sizeof(*source_pixels));
  if (source_info == (MemoryInfo *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",image->filename);
      return(MagickFalse);
    }
  source_pixels=(double *) GetVirtualMemoryBlob(source_info);
  memset(source_pixels,0,fourier_info->width*fourier_info->height*
    sizeof(*source_pixels));
  i=0L;
  image_view=AcquireVirtualCacheView(image,exception);
  for (y=0L; y < (ssize_t) fourier_info->height; y++)
  {
    p=GetCacheViewVirtualPixels(image_view,0L,y,fourier_info->width,1UL,
      exception);
    if (p == (const PixelPacket *) NULL)
      break;
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    for (x=0L; x < (ssize_t) fourier_info->width; x++)
    {
      switch (fourier_info->channel)
      {
        case RedChannel:
        default:
        {
          source_pixels[i]=QuantumScale*(MagickRealType) GetPixelRed(p);
          break;
        }
        case GreenChannel:
        {
          source_pixels[i]=QuantumScale*(MagickRealType) GetPixelGreen(p);
          break;
        }
        case BlueChannel:
        {
          source_pixels[i]=QuantumScale*(MagickRealType) GetPixelBlue(p);
          break;
        }
        case OpacityChannel:
        {
          source_pixels[i]=QuantumScale*(MagickRealType) GetPixelOpacity(p);
          break;
        }
        case IndexChannel:
        {
          source_pixels[i]=QuantumScale*(MagickRealType)
            GetPixelIndex(indexes+x);
          break;
        }
        case GrayChannels:
        {
          source_pixels[i]=QuantumScale*(MagickRealType) GetPixelGray(p);
          break;
        }
      }
      i++;
      p++;
    }
  }
  image_view=DestroyCacheView(image_view);
  forward_info=AcquireVirtualMemory(fourier_info->width,(fourier_info->height/2+
    1)*sizeof(*forward_pixels));
  if (forward_info == (MemoryInfo *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",image->filename);
      source_info=(MemoryInfo *) RelinquishVirtualMemory(source_info);
      return(MagickFalse);
    }
  forward_pixels=(fftw_complex *) GetVirtualMemoryBlob(forward_info);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp critical (MagickCore_ForwardFourierTransform)
#endif
  fftw_r2c_plan=fftw_plan_dft_r2c_2d((int) fourier_info->width,
    (int) fourier_info->height,source_pixels,forward_pixels,FFTW_ESTIMATE);
  fftw_execute_dft_r2c(fftw_r2c_plan,source_pixels,forward_pixels);
  fftw_destroy_plan(fftw_r2c_plan);
  source_info=(MemoryInfo *) RelinquishVirtualMemory(source_info);
  value=GetImageArtifact(image,"fourier:normalize");
  if ((value == (const char *) NULL) || (LocaleCompare(value,"forward") == 0))
    {
      double
        gamma;

      /*
        Normalize forward transform.
      */
      i=0L;
      gamma=MagickSafeReciprocal((double) fourier_info->width*
        fourier_info->height);
      for (y=0L; y < (ssize_t) fourier_info->height; y++)
        for (x=0L; x < (ssize_t) fourier_info->center; x++)
        {
#if defined(MAGICKCORE_HAVE_COMPLEX_H)
          forward_pixels[i]*=gamma;
#else
          forward_pixels[i][0]*=gamma;
          forward_pixels[i][1]*=gamma;
#endif
          i++;
        }
    }
  /*
    Generate magnitude and phase (or real and imaginary).
  */
  i=0L;
  if (fourier_info->modulus != MagickFalse)
    for (y=0L; y < (ssize_t) fourier_info->height; y++)
      for (x=0L; x < (ssize_t) fourier_info->center; x++)
      {
        magnitude_pixels[i]=cabs(forward_pixels[i]);
        phase_pixels[i]=carg(forward_pixels[i]);
        i++;
      }
  else
    for (y=0L; y < (ssize_t) fourier_info->height; y++)
      for (x=0L; x < (ssize_t) fourier_info->center; x++)
      {
        magnitude_pixels[i]=creal(forward_pixels[i]);
        phase_pixels[i]=cimag(forward_pixels[i]);
        i++;
      }
  forward_info=(MemoryInfo *) RelinquishVirtualMemory(forward_info);
  return(MagickTrue);
}

static MagickBooleanType ForwardFourierTransformChannel(const Image *image,
  const ChannelType channel,const MagickBooleanType modulus,
  Image *fourier_image,ExceptionInfo *exception)
{
  double
    *magnitude_pixels,
    *phase_pixels;

  FourierInfo
    fourier_info;

  MagickBooleanType
    status;

  MemoryInfo
    *magnitude_info,
    *phase_info;

  fourier_info.width=image->columns;
  fourier_info.height=image->rows;
  if ((image->columns != image->rows) || ((image->columns % 2) != 0) ||
      ((image->rows % 2) != 0))
    {
      size_t extent=image->columns < image->rows ? image->rows : image->columns;
      fourier_info.width=(extent & 0x01) == 1 ? extent+1UL : extent;
    }
  fourier_info.height=fourier_info.width;
  fourier_info.center=(ssize_t) (fourier_info.width/2L)+1L;
  fourier_info.channel=channel;
  fourier_info.modulus=modulus;
  magnitude_info=AcquireVirtualMemory(fourier_info.width,(fourier_info.height/2+
    1)*sizeof(*magnitude_pixels));
  phase_info=AcquireVirtualMemory(fourier_info.width,(fourier_info.height/2+1)*
    sizeof(*phase_pixels));
  if ((magnitude_info == (MemoryInfo *) NULL) ||
      (phase_info == (MemoryInfo *) NULL))
    {
      if (phase_info != (MemoryInfo *) NULL)
        phase_info=RelinquishVirtualMemory(phase_info);
      if (magnitude_info == (MemoryInfo *) NULL)
        magnitude_info=RelinquishVirtualMemory(magnitude_info);
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",image->filename);
      return(MagickFalse);
    }
  magnitude_pixels=(double *) GetVirtualMemoryBlob(magnitude_info);
  phase_pixels=(double *) GetVirtualMemoryBlob(phase_info);
  status=ForwardFourierTransform(&fourier_info,image,magnitude_pixels,
    phase_pixels,exception);
  if (status != MagickFalse)
    status=ForwardFourier(&fourier_info,fourier_image,magnitude_pixels,
      phase_pixels,exception);
  phase_info=RelinquishVirtualMemory(phase_info);
  magnitude_info=RelinquishVirtualMemory(magnitude_info);
  return(status);
}
#endif

MagickExport Image *ForwardFourierTransformImage(const Image *image,
  const MagickBooleanType modulus,ExceptionInfo *exception)
{
  Image
    *fourier_image;

  fourier_image=NewImageList();
#if !defined(ENABLE_FFTW_DELEGATE)
  (void) modulus;
  (void) ThrowMagickException(exception,GetMagickModule(),
    MissingDelegateWarning,"DelegateLibrarySupportNotBuiltIn","`%s' (FFTW)",
    image->filename);
#else
  {
    Image
      *magnitude_image;

    size_t
      height,
      width;

    width=image->columns;
    height=image->rows;
    if ((image->columns != image->rows) || ((image->columns % 2) != 0) ||
        ((image->rows % 2) != 0))
      {
        size_t extent=image->columns < image->rows ? image->rows :
          image->columns;
        width=(extent & 0x01) == 1 ? extent+1UL : extent;
      }
    height=width;
    magnitude_image=CloneImage(image,width,height,MagickTrue,exception);
    if (magnitude_image != (Image *) NULL)
      {
        Image
          *phase_image;

        magnitude_image->storage_class=DirectClass;
        magnitude_image->depth=32UL;
        phase_image=CloneImage(image,width,height,MagickTrue,exception);
        if (phase_image == (Image *) NULL)
          magnitude_image=DestroyImage(magnitude_image);
        else
          {
            MagickBooleanType
              is_gray,
              status;

            phase_image->storage_class=DirectClass;
            phase_image->depth=32UL;
            AppendImageToList(&fourier_image,magnitude_image);
            AppendImageToList(&fourier_image,phase_image);
            status=MagickTrue;
            is_gray=IsGrayImage(image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
            #pragma omp parallel sections
#endif
            {
#if defined(MAGICKCORE_OPENMP_SUPPORT)
              #pragma omp section
#endif
              {
                MagickBooleanType
                  thread_status;

                if (is_gray != MagickFalse)
                  thread_status=ForwardFourierTransformChannel(image,
                    GrayChannels,modulus,fourier_image,exception);
                else
                  thread_status=ForwardFourierTransformChannel(image,RedChannel,
                    modulus,fourier_image,exception);
                if (thread_status == MagickFalse)
                  status=thread_status;
              }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
              #pragma omp section
#endif
              {
                MagickBooleanType
                  thread_status;

                thread_status=MagickTrue;
                if (is_gray == MagickFalse)
                  thread_status=ForwardFourierTransformChannel(image,
                    GreenChannel,modulus,fourier_image,exception);
                if (thread_status == MagickFalse)
                  status=thread_status;
              }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
              #pragma omp section
#endif
              {
                MagickBooleanType
                  thread_status;

                thread_status=MagickTrue;
                if (is_gray == MagickFalse)
                  thread_status=ForwardFourierTransformChannel(image,
                    BlueChannel,modulus,fourier_image,exception);
                if (thread_status == MagickFalse)
                  status=thread_status;
              }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
              #pragma omp section
#endif
              {
                MagickBooleanType
                  thread_status;

                thread_status=MagickTrue;
                if (image->matte != MagickFalse)
                  thread_status=ForwardFourierTransformChannel(image,
                    OpacityChannel,modulus,fourier_image,exception);
                if (thread_status == MagickFalse)
                  status=thread_status;
              }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
              #pragma omp section
#endif
              {
                MagickBooleanType
                  thread_status;

                thread_status=MagickTrue;
                if (image->colorspace == CMYKColorspace)
                  thread_status=ForwardFourierTransformChannel(image,
                    IndexChannel,modulus,fourier_image,exception);
                if (thread_status == MagickFalse)
                  status=thread_status;
              }
            }
            if (status == MagickFalse)
              fourier_image=DestroyImageList(fourier_image);
            fftw_cleanup();
          }
      }
  }
#endif
  return(fourier_image);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     I n v e r s e F o u r i e r T r a n s f o r m I m a g e                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  InverseFourierTransformImage() implements the inverse discrete Fourier
%  transform (DFT) of the image either as a magnitude / phase or real /
%  imaginary image pair.
%
%  The format of the InverseFourierTransformImage method is:
%
%      Image *InverseFourierTransformImage(const Image *magnitude_image,
%        const Image *phase_image,const MagickBooleanType modulus,
%        ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o magnitude_image: the magnitude or real image.
%
%    o phase_image: the phase or imaginary image.
%
%    o modulus: if true, return transform as a magnitude / phase pair
%      otherwise a real / imaginary image pair.
%
%    o exception: return any errors or warnings in this structure.
%
*/

#if defined(ENABLE_FFTW_DELEGATE)
static MagickBooleanType InverseQuadrantSwap(const size_t width,
  const size_t height,const double *source,double *destination)
{
  ssize_t
    center,
    x,
    y;

  /*
    Swap quadrants.
  */
  center=(ssize_t) (width/2L)+1L;
  for (y=1L; y < (ssize_t) height; y++)
    for (x=0L; x < (ssize_t) (width/2L+1L); x++)
      destination[(height-y)*center-x+width/2L]=source[y*width+x];
  for (y=0L; y < (ssize_t) height; y++)
    destination[y*center]=source[y*width+width/2L];
  for (x=0L; x < center; x++)
    destination[x]=source[center-x-1L];
  return(RollFourier(center,height,0L,(ssize_t) height/-2L,destination));
}

static MagickBooleanType InverseFourier(FourierInfo *fourier_info,
  const Image *magnitude_image,const Image *phase_image,
  fftw_complex *fourier_pixels,ExceptionInfo *exception)
{
  CacheView
    *magnitude_view,
    *phase_view;

  double
    *inverse_pixels,
    *magnitude_pixels,
    *phase_pixels;

  MagickBooleanType
    status;

  MemoryInfo
    *inverse_info,
    *magnitude_info,
    *phase_info;

  const IndexPacket
    *indexes;

  const PixelPacket
    *p;

  ssize_t
    i,
    x,
    y;

  /*
    Inverse Fourier - read image and break down into a double array.
  */
  magnitude_info=AcquireVirtualMemory(fourier_info->width,fourier_info->height*
    sizeof(*magnitude_pixels));
  phase_info=AcquireVirtualMemory(fourier_info->width,fourier_info->height*
    sizeof(*phase_pixels));
  inverse_info=AcquireVirtualMemory(fourier_info->width,(fourier_info->height/
    2+1)*sizeof(*inverse_pixels));
  if ((magnitude_info == (MemoryInfo *) NULL) ||
      (phase_info == (MemoryInfo *) NULL) ||
      (inverse_info == (MemoryInfo *) NULL))
    {
      if (magnitude_info != (MemoryInfo *) NULL)
        magnitude_info=RelinquishVirtualMemory(magnitude_info);
      if (phase_info != (MemoryInfo *) NULL)
        phase_info=RelinquishVirtualMemory(phase_info);
      if (inverse_info != (MemoryInfo *) NULL)
        inverse_info=RelinquishVirtualMemory(inverse_info);
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",
        magnitude_image->filename);
      return(MagickFalse);
    }
  magnitude_pixels=(double *) GetVirtualMemoryBlob(magnitude_info);
  phase_pixels=(double *) GetVirtualMemoryBlob(phase_info);
  inverse_pixels=(double *) GetVirtualMemoryBlob(inverse_info);
  i=0L;
  magnitude_view=AcquireVirtualCacheView(magnitude_image,exception);
  for (y=0L; y < (ssize_t) fourier_info->height; y++)
  {
    p=GetCacheViewVirtualPixels(magnitude_view,0L,y,fourier_info->width,1UL,
      exception);
    if (p == (const PixelPacket *) NULL)
      break;
    indexes=GetCacheViewAuthenticIndexQueue(magnitude_view);
    for (x=0L; x < (ssize_t) fourier_info->width; x++)
    {
      switch (fourier_info->channel)
      {
        case RedChannel:
        default:
        {
          magnitude_pixels[i]=QuantumScale*(MagickRealType) GetPixelRed(p);
          break;
        }
        case GreenChannel:
        {
          magnitude_pixels[i]=QuantumScale*(MagickRealType) GetPixelGreen(p);
          break;
        }
        case BlueChannel:
        {
          magnitude_pixels[i]=QuantumScale*(MagickRealType) GetPixelBlue(p);
          break;
        }
        case OpacityChannel:
        {
          magnitude_pixels[i]=QuantumScale*(MagickRealType) GetPixelOpacity(p);
          break;
        }
        case IndexChannel:
        {
          magnitude_pixels[i]=QuantumScale*(MagickRealType)
           GetPixelIndex(indexes+x);
          break;
        }
        case GrayChannels:
        {
          magnitude_pixels[i]=QuantumScale*(MagickRealType) GetPixelGray(p);
          break;
        }
      }
      i++;
      p++;
    }
  }
  magnitude_view=DestroyCacheView(magnitude_view);
  status=InverseQuadrantSwap(fourier_info->width,fourier_info->height,
    magnitude_pixels,inverse_pixels);
  (void) memcpy(magnitude_pixels,inverse_pixels,fourier_info->height*
    fourier_info->center*sizeof(*magnitude_pixels));
  i=0L;
  phase_view=AcquireVirtualCacheView(phase_image,exception);
  for (y=0L; y < (ssize_t) fourier_info->height; y++)
  {
    p=GetCacheViewVirtualPixels(phase_view,0,y,fourier_info->width,1,
      exception);
    if (p == (const PixelPacket *) NULL)
      break;
    indexes=GetCacheViewAuthenticIndexQueue(phase_view);
    for (x=0L; x < (ssize_t) fourier_info->width; x++)
    {
      switch (fourier_info->channel)
      {
        case RedChannel:
        default:
        {
          phase_pixels[i]=QuantumScale*(MagickRealType) GetPixelRed(p);
          break;
        }
        case GreenChannel:
        {
          phase_pixels[i]=QuantumScale*(MagickRealType) GetPixelGreen(p);
          break;
        }
        case BlueChannel:
        {
          phase_pixels[i]=QuantumScale*(MagickRealType) GetPixelBlue(p);
          break;
        }
        case OpacityChannel:
        {
          phase_pixels[i]=QuantumScale*(MagickRealType) GetPixelOpacity(p);
          break;
        }
        case IndexChannel:
        {
          phase_pixels[i]=QuantumScale*(MagickRealType)
            GetPixelIndex(indexes+x);
          break;
        }
        case GrayChannels:
        {
          phase_pixels[i]=QuantumScale*(MagickRealType) GetPixelGray(p);
          break;
        }
      }
      i++;
      p++;
    }
  }
  if (fourier_info->modulus != MagickFalse)
    {
      i=0L;
      for (y=0L; y < (ssize_t) fourier_info->height; y++)
        for (x=0L; x < (ssize_t) fourier_info->width; x++)
        {
          phase_pixels[i]-=0.5;
          phase_pixels[i]*=(2.0*MagickPI);
          i++;
        }
    }
  phase_view=DestroyCacheView(phase_view);
  CorrectPhaseLHS(fourier_info->width,fourier_info->height,phase_pixels);
  if (status != MagickFalse)
    status=InverseQuadrantSwap(fourier_info->width,fourier_info->height,
      phase_pixels,inverse_pixels);
  (void) memcpy(phase_pixels,inverse_pixels,fourier_info->height*
    fourier_info->center*sizeof(*phase_pixels));
  inverse_info=RelinquishVirtualMemory(inverse_info);
  /*
    Merge two sets.
  */
  i=0L;
  if (fourier_info->modulus != MagickFalse)
    for (y=0L; y < (ssize_t) fourier_info->height; y++)
       for (x=0L; x < (ssize_t) fourier_info->center; x++)
       {
#if defined(MAGICKCORE_HAVE_COMPLEX_H)
         fourier_pixels[i]=magnitude_pixels[i]*cos(phase_pixels[i])+I*
           magnitude_pixels[i]*sin(phase_pixels[i]);
#else
         fourier_pixels[i][0]=magnitude_pixels[i]*cos(phase_pixels[i]);
         fourier_pixels[i][1]=magnitude_pixels[i]*sin(phase_pixels[i]);
#endif
         i++;
      }
  else
    for (y=0L; y < (ssize_t) fourier_info->height; y++)
      for (x=0L; x < (ssize_t) fourier_info->center; x++)
      {
#if defined(MAGICKCORE_HAVE_COMPLEX_H)
        fourier_pixels[i]=magnitude_pixels[i]+I*phase_pixels[i];
#else
        fourier_pixels[i][0]=magnitude_pixels[i];
        fourier_pixels[i][1]=phase_pixels[i];
#endif
        i++;
      }
  magnitude_info=RelinquishVirtualMemory(magnitude_info);
  phase_info=RelinquishVirtualMemory(phase_info);
  return(status);
}

static MagickBooleanType InverseFourierTransform(FourierInfo *fourier_info,
  fftw_complex *fourier_pixels,Image *image,ExceptionInfo *exception)
{
  CacheView
    *image_view;

  double
    *source_pixels;

  const char
    *value;

  fftw_plan
    fftw_c2r_plan;

  MemoryInfo
    *source_info;

  IndexPacket
    *indexes;

  PixelPacket
    *q;

  ssize_t
    i,
    x,
    y;

  /*
    Generate the inverse Fourier transform.
  */
  if ((fourier_info->width >= INT_MAX) || (fourier_info->height >= INT_MAX))
    {
      (void) ThrowMagickException(exception,GetMagickModule(),ImageError,
        "WidthOrHeightExceedsLimit","`%s'",image->filename);
      return(MagickFalse);
    }
  source_info=AcquireVirtualMemory(fourier_info->width,fourier_info->height*
    sizeof(*source_pixels));
  if (source_info == (MemoryInfo *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",image->filename);
      return(MagickFalse);
    }
  source_pixels=(double *) GetVirtualMemoryBlob(source_info);
  value=GetImageArtifact(image,"fourier:normalize");
  if (LocaleCompare(value,"inverse") == 0)
    {
      double
        gamma;

      /*
        Normalize inverse transform.
      */
      i=0L;
      gamma=MagickSafeReciprocal((double) fourier_info->width*
        fourier_info->height);
      for (y=0L; y < (ssize_t) fourier_info->height; y++)
        for (x=0L; x < (ssize_t) fourier_info->center; x++)
        {
#if defined(MAGICKCORE_HAVE_COMPLEX_H)
          fourier_pixels[i]*=gamma;
#else
          fourier_pixels[i][0]*=gamma;
          fourier_pixels[i][1]*=gamma;
#endif
          i++;
        }
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp critical (MagickCore_InverseFourierTransform)
#endif
  fftw_c2r_plan=fftw_plan_dft_c2r_2d((int) fourier_info->width,
    (int) fourier_info->height,fourier_pixels,source_pixels,FFTW_ESTIMATE);
  fftw_execute_dft_c2r(fftw_c2r_plan,fourier_pixels,source_pixels);
  fftw_destroy_plan(fftw_c2r_plan);
  i=0L;
  image_view=AcquireAuthenticCacheView(image,exception);
  for (y=0L; y < (ssize_t) fourier_info->height; y++)
  {
    if (y >= (ssize_t) image->rows)
      break;
    q=GetCacheViewAuthenticPixels(image_view,0L,y,fourier_info->width >
      image->columns ? image->columns : fourier_info->width,1UL,exception);
    if (q == (PixelPacket *) NULL)
      break;
    indexes=GetCacheViewAuthenticIndexQueue(image_view);
    for (x=0L; x < (ssize_t) fourier_info->width; x++)
    {
      if (x < (ssize_t) image->columns)
        switch (fourier_info->channel)
        {
          case RedChannel:
          default:
          {
            SetPixelRed(q,ClampToQuantum((MagickRealType) QuantumRange*
              source_pixels[i]));
            break;
          }
          case GreenChannel:
          {
            SetPixelGreen(q,ClampToQuantum((MagickRealType) QuantumRange*
              source_pixels[i]));
            break;
          }
          case BlueChannel:
          {
            SetPixelBlue(q,ClampToQuantum((MagickRealType) QuantumRange*
              source_pixels[i]));
            break;
          }
          case OpacityChannel:
          {
            SetPixelOpacity(q,ClampToQuantum((MagickRealType) QuantumRange*
              source_pixels[i]));
            break;
          }
          case IndexChannel:
          {
            SetPixelIndex(indexes+x,ClampToQuantum((MagickRealType)
              QuantumRange*source_pixels[i]));
            break;
          }
          case GrayChannels:
          {
            SetPixelGray(q,ClampToQuantum((MagickRealType) QuantumRange*
              source_pixels[i]));
            break;
          }
        }
      i++;
      q++;
    }
    if (SyncCacheViewAuthenticPixels(image_view,exception) == MagickFalse)
      break;
  }
  image_view=DestroyCacheView(image_view);
  source_info=RelinquishVirtualMemory(source_info);
  return(MagickTrue);
}

static MagickBooleanType InverseFourierTransformChannel(
  const Image *magnitude_image,const Image *phase_image,
  const ChannelType channel,const MagickBooleanType modulus,
  Image *fourier_image,ExceptionInfo *exception)
{
  fftw_complex
    *inverse_pixels;

  FourierInfo
    fourier_info;

  MagickBooleanType
    status;

  MemoryInfo
    *inverse_info;

  fourier_info.width=magnitude_image->columns;
  fourier_info.height=magnitude_image->rows;
  if ((magnitude_image->columns != magnitude_image->rows) ||
      ((magnitude_image->columns % 2) != 0) ||
      ((magnitude_image->rows % 2) != 0))
    {
      size_t extent=magnitude_image->columns < magnitude_image->rows ?
        magnitude_image->rows : magnitude_image->columns;
      fourier_info.width=(extent & 0x01) == 1 ? extent+1UL : extent;
    }
  fourier_info.height=fourier_info.width;
  fourier_info.center=(ssize_t) (fourier_info.width/2L)+1L;
  fourier_info.channel=channel;
  fourier_info.modulus=modulus;
  inverse_info=AcquireVirtualMemory(fourier_info.width,(fourier_info.height/2+
    1)*sizeof(*inverse_pixels));
  if (inverse_info == (MemoryInfo *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",
        magnitude_image->filename);
      return(MagickFalse);
    }
  inverse_pixels=(fftw_complex *) GetVirtualMemoryBlob(inverse_info);
  status=InverseFourier(&fourier_info,magnitude_image,phase_image,
    inverse_pixels,exception);
  if (status != MagickFalse)
    status=InverseFourierTransform(&fourier_info,inverse_pixels,fourier_image,
      exception);
  inverse_info=RelinquishVirtualMemory(inverse_info);
  return(status);
}
#endif

MagickExport Image *InverseFourierTransformImage(const Image *magnitude_image,
  const Image *phase_image,const MagickBooleanType modulus,
  ExceptionInfo *exception)
{
  Image
    *fourier_image;

  assert(magnitude_image != (Image *) NULL);
  assert(magnitude_image->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",
      magnitude_image->filename);
  if (phase_image == (Image *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),ImageError,
        "ImageSequenceRequired","`%s'",magnitude_image->filename);
      return((Image *) NULL);
    }
#if !defined(ENABLE_FFTW_DELEGATE)
  fourier_image=(Image *) NULL;
  (void) modulus;
  (void) ThrowMagickException(exception,GetMagickModule(),
    MissingDelegateWarning,"DelegateLibrarySupportNotBuiltIn","`%s' (FFTW)",
    magnitude_image->filename);
#else
  {
    fourier_image=CloneImage(magnitude_image,magnitude_image->columns,
      magnitude_image->rows,MagickTrue,exception);
    if (fourier_image != (Image *) NULL)
      {
        MagickBooleanType
          is_gray,
          status;

        status=MagickTrue;
        is_gray=IsGrayImage(magnitude_image,exception);
        if (is_gray != MagickFalse)
          is_gray=IsGrayImage(phase_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
        #pragma omp parallel sections
#endif
        {
#if defined(MAGICKCORE_OPENMP_SUPPORT)
          #pragma omp section
#endif
          {
            MagickBooleanType
              thread_status;

            if (is_gray != MagickFalse)
              thread_status=InverseFourierTransformChannel(magnitude_image,
                phase_image,GrayChannels,modulus,fourier_image,exception);
            else
              thread_status=InverseFourierTransformChannel(magnitude_image,
                phase_image,RedChannel,modulus,fourier_image,exception);
            if (thread_status == MagickFalse)
              status=thread_status;
          }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
          #pragma omp section
#endif
          {
            MagickBooleanType
              thread_status;

            thread_status=MagickTrue;
            if (is_gray == MagickFalse)
              thread_status=InverseFourierTransformChannel(magnitude_image,
                phase_image,GreenChannel,modulus,fourier_image,exception);
            if (thread_status == MagickFalse)
              status=thread_status;
          }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
          #pragma omp section
#endif
          {
            MagickBooleanType
              thread_status;

            thread_status=MagickTrue;
            if (is_gray == MagickFalse)
              thread_status=InverseFourierTransformChannel(magnitude_image,
                phase_image,BlueChannel,modulus,fourier_image,exception);
            if (thread_status == MagickFalse)
              status=thread_status;
          }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
          #pragma omp section
#endif
          {
            MagickBooleanType
              thread_status;

            thread_status=MagickTrue;
            if (magnitude_image->matte != MagickFalse)
              thread_status=InverseFourierTransformChannel(magnitude_image,
                phase_image,OpacityChannel,modulus,fourier_image,exception);
            if (thread_status == MagickFalse)
              status=thread_status;
          }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
          #pragma omp section
#endif
          {
            MagickBooleanType
              thread_status;

            thread_status=MagickTrue;
            if (magnitude_image->colorspace == CMYKColorspace)
              thread_status=InverseFourierTransformChannel(magnitude_image,
                phase_image,IndexChannel,modulus,fourier_image,exception);
            if (thread_status == MagickFalse)
              status=thread_status;
          }
        }
        if (status == MagickFalse)
          fourier_image=DestroyImage(fourier_image);
      }
    fftw_cleanup();
  }
#endif
  return(fourier_image);
}
