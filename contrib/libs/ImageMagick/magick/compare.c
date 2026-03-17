/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%               CCCC   OOO   M   M  PPPP    AAA   RRRR    EEEEE               %
%              C      O   O  MM MM  P   P  A   A  R   R   E                   %
%              C      O   O  M M M  PPPP   AAAAA  RRRR    EEE                 %
%              C      O   O  M   M  P      A   A  R R     E                   %
%               CCCC   OOO   M   M  P      A   A  R  R    EEEEE               %
%                                                                             %
%                                                                             %
%                      MagickCore Image Comparison Methods                    %
%                                                                             %
%                              Software Design                                %
%                                  Cristy                                     %
%                               December 2003                                 %
%                                                                             %
%                                                                             %
%  Copyright 1999 ImageMagick Studio LLC, a non-profit organization dedicated %
%  to making software imaging solutions freely available.                     %
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
#include "magick/cache-view.h"
#include "magick/channel.h"
#include "magick/client.h"
#include "magick/color.h"
#include "magick/color-private.h"
#include "magick/colorspace.h"
#include "magick/colorspace-private.h"
#include "magick/compare.h"
#include "magick/compare-private.h"
#include "magick/composite-private.h"
#include "magick/constitute.h"
#include "magick/exception-private.h"
#include "magick/geometry.h"
#include "magick/image-private.h"
#include "magick/list.h"
#include "magick/log.h"
#include "magick/memory_.h"
#include "magick/monitor.h"
#include "magick/monitor-private.h"
#include "magick/option.h"
#include "magick/pixel-private.h"
#include "magick/property.h"
#include "magick/resource_.h"
#include "magick/statistic-private.h"
#include "magick/string_.h"
#include "magick/string-private.h"
#include "magick/statistic.h"
#include "magick/thread-private.h"
#include "magick/transform.h"
#include "magick/utility.h"
#include "magick/version.h"

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o m p a r e I m a g e C h a n n e l s                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  CompareImageChannels() compares one or more image channels of an image
%  to a reconstructed image and returns the difference image.
%
%  The format of the CompareImageChannels method is:
%
%      Image *CompareImageChannels(const Image *image,
%        const Image *reconstruct_image,const ChannelType channel,
%        const MetricType metric,double *distortion,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o reconstruct_image: the reconstruct image.
%
%    o channel: the channel.
%
%    o metric: the metric.
%
%    o distortion: the computed distortion between the images.
%
%    o exception: return any errors or warnings in this structure.
%
*/

MagickExport Image *CompareImages(Image *image,const Image *reconstruct_image,
  const MetricType metric,double *distortion,ExceptionInfo *exception)
{
  Image
    *highlight_image;

  highlight_image=CompareImageChannels(image,reconstruct_image,
    CompositeChannels,metric,distortion,exception);
  return(highlight_image);
}

static size_t GetNumberChannels(const Image *image,const ChannelType channel)
{
  size_t
    channels;

  channels=0;
  if ((channel & RedChannel) != 0)
    channels++;
  if ((channel & GreenChannel) != 0)
    channels++;
  if ((channel & BlueChannel) != 0)
    channels++;
  if (((channel & OpacityChannel) != 0) && (image->matte != MagickFalse))
    channels++;
  if (((channel & IndexChannel) != 0) && (image->colorspace == CMYKColorspace))
    channels++;
  return(channels == 0 ? 1UL : channels);
}

static inline MagickBooleanType ValidateImageMorphology(
  const Image *magick_restrict image,
  const Image *magick_restrict reconstruct_image)
{
  /*
    Does the image match the reconstructed image morphology?
  */
  if (GetNumberChannels(image,DefaultChannels) !=
      GetNumberChannels(reconstruct_image,DefaultChannels))
    return(MagickFalse);
  return(MagickTrue);
}

MagickExport Image *CompareImageChannels(Image *image,
  const Image *reconstruct_image,const ChannelType channel,
  const MetricType metric,double *distortion,ExceptionInfo *exception)
{
  CacheView
    *highlight_view,
    *image_view,
    *reconstruct_view;

  const char
    *artifact;

  Image
    *clone_image,
    *difference_image,
    *highlight_image;

  MagickBooleanType
    status = MagickTrue;

  MagickPixelPacket
    highlight,
    lowlight,
    zero;

  size_t
    columns,
    rows;

  ssize_t
    y;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(reconstruct_image != (const Image *) NULL);
  assert(reconstruct_image->signature == MagickCoreSignature);
  assert(distortion != (double *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  *distortion=0.0;
  if (metric != PerceptualHashErrorMetric)
    if (ValidateImageMorphology(image,reconstruct_image) == MagickFalse)
      ThrowImageException(ImageError,"ImageMorphologyDiffers");
  status=GetImageChannelDistortion(image,reconstruct_image,channel,metric,
    distortion,exception);
  if (status == MagickFalse)
    return((Image *) NULL);
  clone_image=CloneImage(image,0,0,MagickTrue,exception);
  if (clone_image == (Image *) NULL)
    return((Image *) NULL);
  (void) SetImageMask(clone_image,(Image *) NULL);
  difference_image=CloneImage(clone_image,0,0,MagickTrue,exception);
  clone_image=DestroyImage(clone_image);
  if (difference_image == (Image *) NULL)
    return((Image *) NULL);
  (void) SetImageAlphaChannel(difference_image,OpaqueAlphaChannel);
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  highlight_image=CloneImage(image,columns,rows,MagickTrue,exception);
  if (highlight_image == (Image *) NULL)
    {
      difference_image=DestroyImage(difference_image);
      return((Image *) NULL);
    }
  if (SetImageStorageClass(highlight_image,DirectClass) == MagickFalse)
    {
      InheritException(exception,&highlight_image->exception);
      difference_image=DestroyImage(difference_image);
      highlight_image=DestroyImage(highlight_image);
      return((Image *) NULL);
    }
  (void) SetImageMask(highlight_image,(Image *) NULL);
  (void) SetImageAlphaChannel(highlight_image,OpaqueAlphaChannel);
  (void) QueryMagickColor("#f1001ecc",&highlight,exception);
  artifact=GetImageArtifact(image,"compare:highlight-color");
  if (artifact != (const char *) NULL)
    (void) QueryMagickColor(artifact,&highlight,exception);
  (void) QueryMagickColor("#ffffffcc",&lowlight,exception);
  artifact=GetImageArtifact(image,"compare:lowlight-color");
  if (artifact != (const char *) NULL)
    (void) QueryMagickColor(artifact,&lowlight,exception);
  if (highlight_image->colorspace == CMYKColorspace)
    {
      ConvertRGBToCMYK(&highlight);
      ConvertRGBToCMYK(&lowlight);
    }
  /*
    Generate difference image.
  */
  GetMagickPixelPacket(image,&zero);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
  highlight_view=AcquireAuthenticCacheView(highlight_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(status) \
    magick_number_threads(image,highlight_image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    MagickBooleanType
      sync;

    MagickPixelPacket
      pixel,
      reconstruct_pixel;

    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    IndexPacket
      *magick_restrict highlight_indexes;

    PixelPacket
      *magick_restrict r;

    ssize_t
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    r=QueueCacheViewAuthenticPixels(highlight_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) ||
        (q == (const PixelPacket *) NULL) || (r == (PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    highlight_indexes=GetCacheViewAuthenticIndexQueue(highlight_view);
    pixel=zero;
    reconstruct_pixel=zero;
    for (x=0; x < (ssize_t) columns; x++)
    {
      SetMagickPixelPacket(image,p,indexes == (IndexPacket *) NULL ? NULL :
        indexes+x,&pixel);
      SetMagickPixelPacket(reconstruct_image,q,reconstruct_indexes ==
        (IndexPacket *) NULL ? NULL : reconstruct_indexes+x,&reconstruct_pixel);
      if (IsMagickColorSimilar(&pixel,&reconstruct_pixel) == MagickFalse)
        SetPixelPacket(highlight_image,&highlight,r,highlight_indexes ==
          (IndexPacket *) NULL ? NULL : highlight_indexes+x);
      else
        SetPixelPacket(highlight_image,&lowlight,r,highlight_indexes ==
          (IndexPacket *) NULL ? NULL : highlight_indexes+x);
      p++;
      q++;
      r++;
    }
    sync=SyncCacheViewAuthenticPixels(highlight_view,exception);
    if (sync == MagickFalse)
      status=MagickFalse;
  }
  highlight_view=DestroyCacheView(highlight_view);
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  (void) CompositeImage(difference_image,image->compose,highlight_image,0,0);
  highlight_image=DestroyImage(highlight_image);
  if (status == MagickFalse)
    difference_image=DestroyImage(difference_image);
  return(difference_image);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t I m a g e C h a n n e l D i s t o r t i o n                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetImageChannelDistortion() compares one or more image channels of an image
%  to a reconstructed image and returns the specified distortion metric.
%
%  The format of the GetImageChannelDistortion method is:
%
%      MagickBooleanType GetImageChannelDistortion(const Image *image,
%        const Image *reconstruct_image,const ChannelType channel,
%        const MetricType metric,double *distortion,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o reconstruct_image: the reconstruct image.
%
%    o channel: the channel.
%
%    o metric: the metric.
%
%    o distortion: the computed distortion between the images.
%
%    o exception: return any errors or warnings in this structure.
%
*/

MagickExport MagickBooleanType GetImageDistortion(Image *image,
  const Image *reconstruct_image,const MetricType metric,double *distortion,
  ExceptionInfo *exception)
{
  MagickBooleanType
    status;

  status=GetImageChannelDistortion(image,reconstruct_image,CompositeChannels,
    metric,distortion,exception);
  return(status);
}

static MagickBooleanType GetAESimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,double *similarity,
  ExceptionInfo *exception)
{
  CacheView
    *image_view,
    *reconstruct_view;

  double
    area,
    fuzz;

  MagickBooleanType
    status = MagickTrue;

  size_t
    columns,
    rows;

  ssize_t
    j,
    y;

  /*
    Compute the absolute difference in pixels between two images.
  */
  fuzz=GetFuzzyColorDistance(image,reconstruct_image);
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(similarity,status) \
    magick_number_threads(image,image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    double
      channel_similarity[CompositeChannels+1] = { 0.0 };

    ssize_t
      i,
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    (void) memset(channel_similarity,0,sizeof(channel_similarity));
    for (x=0; x < (ssize_t) columns; x++)
    {
      double
        Da,
        error,
        Sa;

      size_t
        count = 0;

      Sa=QuantumScale*(image->matte != MagickFalse ? (double) GetPixelAlpha(p) :
        ((double) QuantumRange-(double) OpaqueOpacity));
      Da=QuantumScale*(image->matte != MagickFalse ? (double) GetPixelAlpha(q) :
        ((double) QuantumRange-(double) OpaqueOpacity));
      if ((channel & RedChannel) != 0)
        {
          error=Sa*(double) GetPixelRed(p)-Da*(double)
            GetPixelRed(q);
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[RedChannel]++;
              count++;
            }
        }
      if ((channel & GreenChannel) != 0)
        {
          error=Sa*(double) GetPixelGreen(p)-Da*(double)
            GetPixelGreen(q);
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[GreenChannel]++;
              count++;
            }
        }
      if ((channel & BlueChannel) != 0)
        {
          error=Sa*(double) GetPixelBlue(p)-Da*(double)
            GetPixelBlue(q);
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[BlueChannel]++;
              count++;
            }
        }
      if (((channel & OpacityChannel) != 0) &&
          (image->matte != MagickFalse))
        {
          error=(double) GetPixelOpacity(p)-(double) GetPixelOpacity(q);
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[OpacityChannel]++;
              count++;
            }
        }
      if (((channel & IndexChannel) != 0) &&
          (image->colorspace == CMYKColorspace))
        {
          error=Sa*(double) indexes[x]-Da*(double) reconstruct_indexes[x];
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[IndexChannel]++;
              count++;
            }
        }
      if (count != 0)
        channel_similarity[CompositeChannels]++;
      p++;
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (MagickCore_GetAESimilarity)
#endif
    for (i=0; i <= (ssize_t) CompositeChannels; i++)
      similarity[i]+=channel_similarity[i];
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  area=MagickSafeReciprocal((double) columns*rows);
  for (j=0; j <= CompositeChannels; j++)
    similarity[j]*=area;
  return(status);
}

static MagickBooleanType GetFUZZSimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,
  double *similarity,ExceptionInfo *exception)
{
  CacheView
    *image_view,
    *reconstruct_view;

  double
    area = 0.0,
    fuzz;

  MagickBooleanType
    status = MagickTrue;

  size_t
    columns,
    rows;

  ssize_t
    i,
    y;

  fuzz=GetFuzzyColorDistance(image,reconstruct_image);
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(status) \
    magick_number_threads(image,image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    double
      channel_area = 0.0,
      channel_similarity[CompositeChannels+1] = { 0.0 };

    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    ssize_t
      i,
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    for (x=0; x < (ssize_t) columns; x++)
    {
      MagickRealType
        Da,
        error,
        Sa;
 
      Sa=QuantumScale*(image->matte != MagickFalse ? (double)
        GetPixelAlpha(p) : ((double) QuantumRange-(double) OpaqueOpacity));
      Da=QuantumScale*(reconstruct_image->matte != MagickFalse ?
        (double) GetPixelAlpha(q) : ((double) QuantumRange-(double)
        OpaqueOpacity));
      if ((channel & RedChannel) != 0)
        {
          error=QuantumScale*(Sa*GetPixelRed(p)-Da*GetPixelRed(q));
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[RedChannel]+=error*error;
              channel_similarity[CompositeChannels]+=error*error;
              channel_area++;
            }
        }
      if ((channel & GreenChannel) != 0)
        {
          error=QuantumScale*(Sa*GetPixelGreen(p)-Da*GetPixelGreen(q));
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[GreenChannel]+=error*error;
              channel_similarity[CompositeChannels]+=error*error;
              channel_area++;
            }
        }
      if ((channel & BlueChannel) != 0)
        {
          error=QuantumScale*(Sa*GetPixelBlue(p)-Da*GetPixelBlue(q));
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[BlueChannel]+=error*error;
              channel_similarity[CompositeChannels]+=error*error;
              channel_area++;
            }
        }
      if (((channel & OpacityChannel) != 0) && (image->matte != MagickFalse))
        {
          error=QuantumScale*((double) GetPixelOpacity(p)-GetPixelOpacity(q));
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[OpacityChannel]+=error*error;
              channel_similarity[CompositeChannels]+=error*error;
              channel_area++;
            }
        }
      if (((channel & IndexChannel) != 0) &&
          (image->colorspace == CMYKColorspace))
        {
          error=QuantumScale*(Sa*GetPixelIndex(indexes+x)-Da*
            GetPixelIndex(reconstruct_indexes+x));
          if (MagickSafeSignificantError(error*error,fuzz) != MagickFalse)
            {
              channel_similarity[BlackChannel]+=error*error;
              channel_similarity[CompositeChannels]+=error*error;
              channel_area++;
            }
        }
      p++;
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (MagickCore_GetMeanAbsoluteError)
#endif
    {
      area+=channel_area;
      for (i=0; i <= (ssize_t) CompositeChannels; i++)
        similarity[i]+=channel_similarity[i];
    }
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  area=MagickSafeReciprocal(area);
  for (i=0; i <= (ssize_t) CompositeChannels; i++)
    similarity[i]*=area;
  return(status);
}

static MagickBooleanType GetMAESimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,
  double *similarity,ExceptionInfo *exception)
{
  CacheView
    *image_view,
    *reconstruct_view;

  MagickBooleanType
    status;

  size_t
    columns,
    rows;

  ssize_t
    i,
    y;

  status=MagickTrue;
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(status) \
    magick_number_threads(image,image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    double
      channel_similarity[CompositeChannels+1];

    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    ssize_t
      i,
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    (void) memset(channel_similarity,0,sizeof(channel_similarity));
    for (x=0; x < (ssize_t) columns; x++)
    {
      MagickRealType
        distance,
        Da,
        Sa;

      Sa=QuantumScale*(image->matte != MagickFalse ? (double) GetPixelAlpha(p) :
        ((double) QuantumRange-(double) OpaqueOpacity));
      Da=QuantumScale*(reconstruct_image->matte != MagickFalse ?
        (double) GetPixelAlpha(q) : ((double) QuantumRange-(double)
        OpaqueOpacity));
      if ((channel & RedChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelRed(p)-Da*
            (double) GetPixelRed(q));
          channel_similarity[RedChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
        }
      if ((channel & GreenChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelGreen(p)-Da*
            (double) GetPixelGreen(q));
          channel_similarity[GreenChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
        }
      if ((channel & BlueChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelBlue(p)-Da*
            (double) GetPixelBlue(q));
          channel_similarity[BlueChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
        }
      if (((channel & OpacityChannel) != 0) &&
          (image->matte != MagickFalse))
        {
          distance=QuantumScale*fabs((double) GetPixelOpacity(p)-(double)
            GetPixelOpacity(q));
          channel_similarity[OpacityChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
        }
      if (((channel & IndexChannel) != 0) &&
          (image->colorspace == CMYKColorspace))
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelIndex(indexes+x)-Da*
            (double) GetPixelIndex(reconstruct_indexes+x));
          channel_similarity[BlackChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
        }
      p++;
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (MagickCore_GetMeanAbsoluteError)
#endif
    for (i=0; i <= (ssize_t) CompositeChannels; i++)
      similarity[i]+=channel_similarity[i];
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  for (i=0; i <= (ssize_t) CompositeChannels; i++)
    similarity[i]/=((double) columns*rows);
  similarity[CompositeChannels]/=(double) GetNumberChannels(image,channel);
  return(status);
}

static MagickBooleanType GetMEPPSimilarity(Image *image,
  const Image *reconstruct_image,const ChannelType channel,double *similarity,
  ExceptionInfo *exception)
{
  CacheView
    *image_view,
    *reconstruct_view;

  double
    maximum_error = -MagickMaximumValue,
    mean_error = 0.0;

  MagickBooleanType
    status;

  size_t
    columns,
    rows;

  ssize_t
    i,
    y;

  status=MagickTrue;
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(maximum_error,status) \
    magick_number_threads(image,image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    double
      channel_similarity[CompositeChannels+1] = { 0.0 },
      local_maximum = maximum_error,
      local_mean_error = 0.0;

    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    ssize_t
      i,
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    (void) memset(channel_similarity,0,sizeof(channel_similarity));
    for (x=0; x < (ssize_t) columns; x++)
    {
      MagickRealType
        distance,
        Da,
        Sa;

      Sa=QuantumScale*(image->matte != MagickFalse ? (double) GetPixelAlpha(p) :
        ((double) QuantumRange-(double) OpaqueOpacity));
      Da=QuantumScale*(reconstruct_image->matte != MagickFalse ?
        (double) GetPixelAlpha(q) : ((double) QuantumRange-(double)
        OpaqueOpacity));
      if ((channel & RedChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelRed(p)-Da*
            (double) GetPixelRed(q));
          channel_similarity[RedChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
          local_mean_error+=distance*distance;
          if (distance > local_maximum)
            local_maximum=distance;
        }
      if ((channel & GreenChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelGreen(p)-Da*
            (double) GetPixelGreen(q));
          channel_similarity[GreenChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
          local_mean_error+=distance*distance;
          if (distance > local_maximum)
            local_maximum=distance;
        }
      if ((channel & BlueChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelBlue(p)-Da*
            (double) GetPixelBlue(q));
          channel_similarity[BlueChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
          local_mean_error+=distance*distance;
          if (distance > local_maximum)
            local_maximum=distance;
        }
      if (((channel & OpacityChannel) != 0) &&
          (image->matte != MagickFalse))
        {
          distance=QuantumScale*fabs((double) GetPixelOpacity(p)-(double)
            GetPixelOpacity(q));
          channel_similarity[OpacityChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
          local_mean_error+=distance*distance;
          if (distance > local_maximum)
            local_maximum=distance;
        }
      if (((channel & IndexChannel) != 0) &&
          (image->colorspace == CMYKColorspace))
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelIndex(indexes+x)-Da*
            (double) GetPixelIndex(reconstruct_indexes+x));
          channel_similarity[BlackChannel]+=distance;
          channel_similarity[CompositeChannels]+=distance;
          local_mean_error+=distance*distance;
          if (distance > local_maximum)
            local_maximum=distance;
        }
      p++;
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (MagickCore_GetMeanAbsoluteError)
#endif
    {
      for (i=0; i <= (ssize_t) CompositeChannels; i++)
        similarity[i]+=channel_similarity[i];
      mean_error+=local_mean_error;
      if (local_maximum > maximum_error)
        maximum_error=local_maximum;
    }
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  for (i=0; i <= (ssize_t) CompositeChannels; i++)
    similarity[i]/=((double) columns*rows);
  similarity[CompositeChannels]/=(double) GetNumberChannels(image,channel);
  image->error.mean_error_per_pixel=QuantumRange*similarity[CompositeChannels];
  image->error.normalized_mean_error=mean_error/((double) columns*rows);
  image->error.normalized_maximum_error=maximum_error;
  return(status);
}

static MagickBooleanType GetMSESimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,
  double *similarity,ExceptionInfo *exception)
{
  CacheView
    *image_view,
    *reconstruct_view;

  double
    area = 0.0;

  MagickBooleanType
    status;

  size_t
    columns,
    rows;

  ssize_t
    i,
    y;

  status=MagickTrue;
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(similarity,status) \
    magick_number_threads(image,image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    double
      channel_similarity[CompositeChannels+1] = { 0.0 };

    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    ssize_t
      i,
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    for (x=0; x < (ssize_t) columns; x++)
    {
      double
        distance,
        Da,
        Sa;

      Sa=QuantumScale*(image->matte != MagickFalse ? (double) GetPixelAlpha(p) :
        ((double) QuantumRange-(double) OpaqueOpacity));
      Da=QuantumScale*(reconstruct_image->matte != MagickFalse ?
        (double) GetPixelAlpha(q) : ((double) QuantumRange-(double)
        OpaqueOpacity));
      if ((channel & RedChannel) != 0)
        {
          distance=QuantumScale*(Sa*(double) GetPixelRed(p)-Da*(double)
            GetPixelRed(q));
          channel_similarity[RedChannel]+=distance*distance;
          channel_similarity[CompositeChannels]+=distance*distance;
        }
      if ((channel & GreenChannel) != 0)
        {
          distance=QuantumScale*(Sa*(double) GetPixelGreen(p)-Da*(double)
            GetPixelGreen(q));
          channel_similarity[GreenChannel]+=distance*distance;
          channel_similarity[CompositeChannels]+=distance*distance;
        }
      if ((channel & BlueChannel) != 0)
        {
          distance=QuantumScale*(Sa*(double) GetPixelBlue(p)-Da*(double)
            GetPixelBlue(q));
          channel_similarity[BlueChannel]+=distance*distance;
          channel_similarity[CompositeChannels]+=distance*distance;
        }
      if (((channel & OpacityChannel) != 0) &&
          (image->matte != MagickFalse))
        {
          distance=QuantumScale*((double) GetPixelOpacity(p)-(double)
            GetPixelOpacity(q));
          channel_similarity[OpacityChannel]+=distance*distance;
          channel_similarity[CompositeChannels]+=distance*distance;
        }
      if (((channel & IndexChannel) != 0) &&
          (image->colorspace == CMYKColorspace) &&
          (reconstruct_image->colorspace == CMYKColorspace))
        {
          distance=QuantumScale*(Sa*(double) GetPixelIndex(indexes+x)-Da*
            (double) GetPixelIndex(reconstruct_indexes+x));
          channel_similarity[BlackChannel]+=distance*distance;
          channel_similarity[CompositeChannels]+=distance*distance;
        }
      p++;
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (MagickCore_GetMeanSquaredError)
#endif
    for (i=0; i <= (ssize_t) CompositeChannels; i++)
      similarity[i]+=channel_similarity[i];
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  area=MagickSafeReciprocal((double) columns*rows);
  for (i=0; i <= (ssize_t) CompositeChannels; i++)
    similarity[i]*=area;
  similarity[CompositeChannels]/=(double) GetNumberChannels(image,channel);
  return(status);
}

static MagickBooleanType GetNCCSimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,double *similarity,
  ExceptionInfo *exception)
{
#define SimilarityImageTag  "Similarity/Image"

  CacheView
    *image_view,
    *reconstruct_view;

  ChannelStatistics
    *image_statistics,
    *reconstruct_statistics;

  double
    alpha_variance[CompositeChannels+1] = { 0.0 },
    beta_variance[CompositeChannels+1] = { 0.0 };

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  size_t
    columns,
    rows;

  ssize_t
    i,
    y;

  /*
    Normalize to account for variation due to lighting and exposure condition.
  */
  image_statistics=GetImageChannelStatistics(image,exception);
  reconstruct_statistics=GetImageChannelStatistics(reconstruct_image,exception);
  if ((image_statistics == (ChannelStatistics *) NULL) ||
      (reconstruct_statistics == (ChannelStatistics *) NULL))
    {
      if (image_statistics != (ChannelStatistics *) NULL)
        image_statistics=(ChannelStatistics *) RelinquishMagickMemory(
          image_statistics);
      if (reconstruct_statistics != (ChannelStatistics *) NULL)
        reconstruct_statistics=(ChannelStatistics *) RelinquishMagickMemory(
          reconstruct_statistics);
      return(MagickFalse);
    }
  (void) memset(similarity,0,(CompositeChannels+1)*sizeof(*similarity));
  status=MagickTrue;
  progress=0;
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(status) \
    magick_number_threads(image,image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    double
      channel_alpha_variance[CompositeChannels+1] = { 0.0 },
      channel_beta_variance[CompositeChannels+1] = { 0.0 },
      channel_similarity[CompositeChannels+1] = { 0.0 };

    ssize_t
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    for (x=0; x < (ssize_t) columns; x++)
    {
      MagickRealType
        alpha,
        beta,
        Da,
        Sa;

      Sa=QuantumScale*(image->matte != MagickFalse ? (double) GetPixelAlpha(p) :
        (double) QuantumRange);
      Da=QuantumScale*(reconstruct_image->matte != MagickFalse ?
        (double) GetPixelAlpha(q) : (double) QuantumRange);
      if ((channel & RedChannel) != 0)
        {
          alpha=QuantumScale*(Sa*(double) GetPixelRed(p)-
            image_statistics[RedChannel].mean);
          beta=QuantumScale*(Da*(double) GetPixelRed(q)-
            reconstruct_statistics[RedChannel].mean);
          channel_similarity[RedChannel]+=alpha*beta;
          channel_similarity[CompositeChannels]+=alpha*beta;
          channel_alpha_variance[RedChannel]+=alpha*alpha;
          channel_alpha_variance[CompositeChannels]+=alpha*alpha;
          channel_beta_variance[RedChannel]+=beta*beta;
          channel_beta_variance[CompositeChannels]+=beta*beta;
        }
      if ((channel & GreenChannel) != 0)
        {
          alpha=QuantumScale*(Sa*(double) GetPixelGreen(p)-
            image_statistics[GreenChannel].mean);
          beta=QuantumScale*(Da*(double) GetPixelGreen(q)-
            reconstruct_statistics[GreenChannel].mean);
          channel_similarity[GreenChannel]+=alpha*beta;
          channel_similarity[CompositeChannels]+=alpha*beta;
          channel_alpha_variance[GreenChannel]+=alpha*alpha;
          channel_alpha_variance[CompositeChannels]+=alpha*alpha;
          channel_beta_variance[GreenChannel]+=beta*beta;
          channel_beta_variance[CompositeChannels]+=beta*beta;
        }
      if ((channel & BlueChannel) != 0)
        {
          alpha=QuantumScale*(Sa*(double) GetPixelBlue(p)-
            image_statistics[BlueChannel].mean);
          beta=QuantumScale*(Da*(double) GetPixelBlue(q)-
            reconstruct_statistics[BlueChannel].mean);
          channel_similarity[BlueChannel]+=alpha*beta;
          channel_alpha_variance[BlueChannel]+=alpha*alpha;
          channel_beta_variance[BlueChannel]+=beta*beta;
        }
      if (((channel & OpacityChannel) != 0) && (image->matte != MagickFalse))
        {
          alpha=QuantumScale*((double) GetPixelAlpha(p)-
            image_statistics[AlphaChannel].mean);
          beta=QuantumScale*((double) GetPixelAlpha(q)-
            reconstruct_statistics[AlphaChannel].mean);
          channel_similarity[OpacityChannel]+=alpha*beta;
          channel_similarity[CompositeChannels]+=alpha*beta;
          channel_alpha_variance[OpacityChannel]+=alpha*alpha;
          channel_alpha_variance[CompositeChannels]+=alpha*alpha;
          channel_beta_variance[OpacityChannel]+=beta*beta;
          channel_beta_variance[CompositeChannels]+=beta*beta;
        }
      if (((channel & IndexChannel) != 0) &&
          (image->colorspace == CMYKColorspace) &&
          (reconstruct_image->colorspace == CMYKColorspace))
        {
          alpha=QuantumScale*(Sa*(double) GetPixelIndex(indexes+x)-
            image_statistics[BlackChannel].mean);
          beta=QuantumScale*(Da*(double) GetPixelIndex(reconstruct_indexes+
            x)-reconstruct_statistics[BlackChannel].mean);
          channel_similarity[BlackChannel]+=alpha*beta;
          channel_similarity[CompositeChannels]+=alpha*beta;
          channel_alpha_variance[BlackChannel]+=alpha*alpha;
          channel_alpha_variance[CompositeChannels]+=alpha*alpha;
          channel_beta_variance[BlackChannel]+=beta*beta;
          channel_beta_variance[CompositeChannels]+=beta*beta;
        }
      p++;
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (GetNCCSimilarity)
#endif
    {
      ssize_t
        j;

      for (j=0; j <= (ssize_t) CompositeChannels; j++)
      {
        similarity[j]+=channel_similarity[j];
        alpha_variance[j]+=channel_alpha_variance[j];
        beta_variance[j]+=channel_beta_variance[j];
      }
    }
    if (image->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

#if defined(MAGICKCORE_OPENMP_SUPPORT)
        #pragma omp atomic
#endif
        progress++;
        proceed=SetImageProgress(image,SimilarityImageTag,progress,rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  /*
    Divide by the standard deviation.
  */
  for (i=0; i <= (ssize_t) CompositeChannels; i++)
    similarity[i]*=MagickSafeReciprocal(sqrt(alpha_variance[i])*
      sqrt(beta_variance[i]));
  /*
    Free resources.
  */
  reconstruct_statistics=(ChannelStatistics *) RelinquishMagickMemory(
    reconstruct_statistics);
  image_statistics=(ChannelStatistics *) RelinquishMagickMemory(
    image_statistics);
  return(status);
}

static MagickBooleanType GetPASimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,
  double *similarity,ExceptionInfo *exception)
{
  CacheView
    *image_view,
    *reconstruct_view;

  MagickBooleanType
    status;

  size_t
    columns,
    rows;

  ssize_t
    y;

  status=MagickTrue;
  (void) memset(similarity,0,(CompositeChannels+1)*sizeof(*similarity));
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(status) \
    magick_number_threads(image,image,rows,1)
#endif
  for (y=0; y < (ssize_t) rows; y++)
  {
    double
      channel_similarity[CompositeChannels+1];

    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    ssize_t
      i,
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    (void) memset(channel_similarity,0,(CompositeChannels+1)*
      sizeof(*channel_similarity));
    for (x=0; x < (ssize_t) columns; x++)
    {
      MagickRealType
        distance,
        Da,
        Sa;

      Sa=QuantumScale*(image->matte != MagickFalse ? (double) GetPixelAlpha(p) :
        ((double) QuantumRange-(double) OpaqueOpacity));
      Da=QuantumScale*(reconstruct_image->matte != MagickFalse ?
        (double) GetPixelAlpha(q) : ((double) QuantumRange-(double)
        OpaqueOpacity));
      if ((channel & RedChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelRed(p)-Da*
            (double) GetPixelRed(q));
          if (distance > channel_similarity[RedChannel])
            channel_similarity[RedChannel]=distance;
          if (distance > channel_similarity[CompositeChannels])
            channel_similarity[CompositeChannels]=distance;
        }
      if ((channel & GreenChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelGreen(p)-Da*
            (double) GetPixelGreen(q));
          if (distance > channel_similarity[GreenChannel])
            channel_similarity[GreenChannel]=distance;
          if (distance > channel_similarity[CompositeChannels])
            channel_similarity[CompositeChannels]=distance;
        }
      if ((channel & BlueChannel) != 0)
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelBlue(p)-Da*
            (double) GetPixelBlue(q));
          if (distance > channel_similarity[BlueChannel])
            channel_similarity[BlueChannel]=distance;
          if (distance > channel_similarity[CompositeChannels])
            channel_similarity[CompositeChannels]=distance;
        }
      if (((channel & OpacityChannel) != 0) &&
          (image->matte != MagickFalse))
        {
          distance=QuantumScale*fabs((double) GetPixelOpacity(p)-(double)
            GetPixelOpacity(q));
          if (distance > channel_similarity[OpacityChannel])
            channel_similarity[OpacityChannel]=distance;
          if (distance > channel_similarity[CompositeChannels])
            channel_similarity[CompositeChannels]=distance;
        }
      if (((channel & IndexChannel) != 0) &&
          (image->colorspace == CMYKColorspace) &&
          (reconstruct_image->colorspace == CMYKColorspace))
        {
          distance=QuantumScale*fabs(Sa*(double) GetPixelIndex(indexes+x)-Da*
            (double) GetPixelIndex(reconstruct_indexes+x));
          if (distance > channel_similarity[BlackChannel])
            channel_similarity[BlackChannel]=distance;
          if (distance > channel_similarity[CompositeChannels])
            channel_similarity[CompositeChannels]=distance;
        }
      p++;
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (MagickCore_GetPeakAbsoluteError)
#endif
    for (i=0; i <= (ssize_t) CompositeChannels; i++)
      if (channel_similarity[i] > similarity[i])
        similarity[i]=channel_similarity[i];
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  return(status);
}

static MagickBooleanType GetPSNRSimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,
  double *similarity,ExceptionInfo *exception)
{
  MagickBooleanType
    status;

  status=GetMSESimilarity(image,reconstruct_image,channel,similarity,
    exception);
  if ((channel & RedChannel) != 0)
    similarity[RedChannel]=10.0*MagickSafeLog10(MagickSafeReciprocal(
      similarity[RedChannel]))/MagickSafePSNRRecipicol(10.0);
  if ((channel & GreenChannel) != 0)
    similarity[GreenChannel]=10.0*MagickSafeLog10(MagickSafeReciprocal(
    similarity[GreenChannel]))/MagickSafePSNRRecipicol(10.0);
  if ((channel & BlueChannel) != 0)
    similarity[BlueChannel]=10.0*MagickSafeLog10(MagickSafeReciprocal(
      similarity[BlueChannel]))/MagickSafePSNRRecipicol(10.0);
  if (((channel & OpacityChannel) != 0) && (image->matte != MagickFalse))
    similarity[OpacityChannel]=10.0*MagickSafeLog10(MagickSafeReciprocal(
      similarity[OpacityChannel]))/MagickSafePSNRRecipicol(10.0);
  if (((channel & IndexChannel) != 0) && (image->colorspace == CMYKColorspace))
    similarity[BlackChannel]=10.0*MagickSafeLog10(MagickSafeReciprocal(
      similarity[BlackChannel]))/MagickSafePSNRRecipicol(10.0);
  similarity[CompositeChannels]=10.0*MagickSafeLog10(MagickSafeReciprocal(
    similarity[CompositeChannels]))/MagickSafePSNRRecipicol(10.0);
  return(status);
}

static MagickBooleanType GetPHASHSimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,double *similarity,
  ExceptionInfo *exception)
{
#define PHASHNormalizationFactor  389.373723242

  ChannelPerceptualHash
    *image_phash,
    *reconstruct_phash;

  double
    error,
    difference;

  ssize_t
    i;

  /*
    Compute perceptual hash in the sRGB colorspace.
  */
  image_phash=GetImageChannelPerceptualHash(image,exception);
  if (image_phash == (ChannelPerceptualHash *) NULL)
    return(MagickFalse);
  reconstruct_phash=GetImageChannelPerceptualHash(reconstruct_image,exception);
  if (reconstruct_phash == (ChannelPerceptualHash *) NULL)
    {
      image_phash=(ChannelPerceptualHash *) RelinquishMagickMemory(image_phash);
      return(MagickFalse);
    }
  for (i=0; i < MaximumNumberOfImageMoments; i++)
  {
    /*
      Compute sum of moment differences squared.
    */
    if ((channel & RedChannel) != 0)
      {
        error=reconstruct_phash[RedChannel].P[i]-image_phash[RedChannel].P[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[RedChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if ((channel & GreenChannel) != 0)
      {
        error=reconstruct_phash[GreenChannel].P[i]-
          image_phash[GreenChannel].P[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[GreenChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if ((channel & BlueChannel) != 0)
      {
        error=reconstruct_phash[BlueChannel].P[i]-image_phash[BlueChannel].P[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[BlueChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if (((channel & OpacityChannel) != 0) && (image->matte != MagickFalse) &&
        (reconstruct_image->matte != MagickFalse))
      {
        error=reconstruct_phash[OpacityChannel].P[i]-
          image_phash[OpacityChannel].P[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[OpacityChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if (((channel & IndexChannel) != 0) &&
        (image->colorspace == CMYKColorspace) &&
        (reconstruct_image->colorspace == CMYKColorspace))
      {
        error=reconstruct_phash[IndexChannel].P[i]-
          image_phash[IndexChannel].P[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[IndexChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
  }
  /*
    Compute perceptual hash in the HCLP colorspace.
  */
  for (i=0; i < MaximumNumberOfImageMoments; i++)
  {
    /*
      Compute sum of moment differences squared.
    */
    if ((channel & RedChannel) != 0)
      {
        error=reconstruct_phash[RedChannel].Q[i]-image_phash[RedChannel].Q[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[RedChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if ((channel & GreenChannel) != 0)
      {
        error=reconstruct_phash[GreenChannel].Q[i]-
          image_phash[GreenChannel].Q[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[GreenChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if ((channel & BlueChannel) != 0)
      {
        error=reconstruct_phash[BlueChannel].Q[i]-image_phash[BlueChannel].Q[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[BlueChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if (((channel & OpacityChannel) != 0) && (image->matte != MagickFalse) &&
        (reconstruct_image->matte != MagickFalse))
      {
        error=reconstruct_phash[OpacityChannel].Q[i]-
          image_phash[OpacityChannel].Q[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[OpacityChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
    if (((channel & IndexChannel) != 0) &&
        (image->colorspace == CMYKColorspace) &&
        (reconstruct_image->colorspace == CMYKColorspace))
      {
        error=reconstruct_phash[IndexChannel].Q[i]-
          image_phash[IndexChannel].Q[i];
        if (IsNaN(error) != 0)
          error=0.0;
        difference=error*error/PHASHNormalizationFactor;
        similarity[IndexChannel]+=difference;
        similarity[CompositeChannels]+=difference;
      }
  }
  similarity[CompositeChannels]/=(double) GetNumberChannels(image,channel);
  /*
    Free resources.
  */
  reconstruct_phash=(ChannelPerceptualHash *) RelinquishMagickMemory(
    reconstruct_phash);
  image_phash=(ChannelPerceptualHash *) RelinquishMagickMemory(image_phash);
  return(MagickTrue);
}

static MagickBooleanType GetRMSESimilarity(const Image *image,
  const Image *reconstruct_image,const ChannelType channel,double *similarity,
  ExceptionInfo *exception)
{
#define RMSESquareRoot(x)  sqrt((x) < 0.0 ? 0.0 : (x))

  MagickBooleanType
    status;

  status=GetMSESimilarity(image,reconstruct_image,channel,similarity,
    exception);
  if ((channel & RedChannel) != 0)
    similarity[RedChannel]=RMSESquareRoot(similarity[RedChannel]);
  if ((channel & GreenChannel) != 0)
    similarity[GreenChannel]=RMSESquareRoot(similarity[GreenChannel]);
  if ((channel & BlueChannel) != 0)
    similarity[BlueChannel]=RMSESquareRoot(similarity[BlueChannel]);
  if (((channel & OpacityChannel) != 0) &&
      (image->matte != MagickFalse))
    similarity[OpacityChannel]=RMSESquareRoot(similarity[OpacityChannel]);
  if (((channel & IndexChannel) != 0) &&
      (image->colorspace == CMYKColorspace))
    similarity[BlackChannel]=RMSESquareRoot(similarity[BlackChannel]);
  similarity[CompositeChannels]=RMSESquareRoot(similarity[CompositeChannels]);
  return(status);
}

MagickExport MagickBooleanType GetImageChannelDistortion(Image *image,
  const Image *reconstruct_image,const ChannelType channel,
  const MetricType metric,double *distortion,ExceptionInfo *exception)
{
  double
    *channel_similarity;

  MagickBooleanType
    status;

  size_t
    length;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(reconstruct_image != (const Image *) NULL);
  assert(reconstruct_image->signature == MagickCoreSignature);
  assert(distortion != (double *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  *distortion=0.0;
  if (metric != PerceptualHashErrorMetric)
    if (ValidateImageMorphology(image,reconstruct_image) == MagickFalse)
      ThrowBinaryException(ImageError,"ImageMorphologyDiffers",image->filename);
  /*
    Get image distortion.
  */
  length=CompositeChannels+1UL;
  channel_similarity=(double *) AcquireQuantumMemory(length,
    sizeof(*channel_similarity));
  if (channel_similarity == (double *) NULL)
    ThrowFatalException(ResourceLimitFatalError,"MemoryAllocationFailed");
  (void) memset(channel_similarity,0,length*sizeof(*channel_similarity));
  switch (metric)
  {
    case AbsoluteErrorMetric:
    {
      status=GetAESimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case FuzzErrorMetric:
    {
      status=GetFUZZSimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case MeanAbsoluteErrorMetric:
    {
      status=GetMAESimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case MeanErrorPerPixelMetric:
    {
      status=GetMEPPSimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case MeanSquaredErrorMetric:
    {
      status=GetMSESimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case NormalizedCrossCorrelationErrorMetric:
    {
      status=GetNCCSimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case PeakAbsoluteErrorMetric:
    {
      status=GetPASimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case PeakSignalToNoiseRatioMetric:
    {
      status=GetPSNRSimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case PerceptualHashErrorMetric:
    {
      status=GetPHASHSimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
    case RootMeanSquaredErrorMetric:
    case UndefinedErrorMetric:
    default:
    {
      status=GetRMSESimilarity(image,reconstruct_image,channel,
        channel_similarity,exception);
      break;
    }
  }
  *distortion=channel_similarity[CompositeChannels];
  switch (metric)
  {
    case NormalizedCrossCorrelationErrorMetric:
    {
      *distortion=(1.0-(*distortion))/2.0;
      break;
    }
    default: break;
  } 
  if (fabs(*distortion) < MagickEpsilon)
    *distortion=0.0;
  channel_similarity=(double *) RelinquishMagickMemory(channel_similarity);
  (void) FormatImageProperty(image,"distortion","%.*g",GetMagickPrecision(),
    *distortion);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t I m a g e C h a n n e l D i s t o r t i o n s                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetImageChannelDistortions() compares the image channels of an image to a
%  reconstructed image and returns the specified distortion metric for each
%  channel.
%
%  The format of the GetImageChannelDistortions method is:
%
%      double *GetImageChannelDistortions(const Image *image,
%        const Image *reconstruct_image,const MetricType metric,
%        ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o reconstruct_image: the reconstruct image.
%
%    o metric: the metric.
%
%    o exception: return any errors or warnings in this structure.
%
*/
MagickExport double *GetImageChannelDistortions(Image *image,
  const Image *reconstruct_image,const MetricType metric,
  ExceptionInfo *exception)
{
  double
    *distortion,
    *similarity;

  MagickBooleanType
    status;

  size_t
    length;

  ssize_t
    i;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(reconstruct_image != (const Image *) NULL);
  assert(reconstruct_image->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  if (metric != PerceptualHashErrorMetric)
    if (ValidateImageMorphology(image,reconstruct_image) == MagickFalse)
      {
        (void) ThrowMagickException(&image->exception,GetMagickModule(),
          ImageError,"ImageMorphologyDiffers","`%s'",image->filename);
        return((double *) NULL);
      }
  /*
    Get image distortion.
  */
  length=CompositeChannels+1UL;
  similarity=(double *) AcquireQuantumMemory(length,
    sizeof(*similarity));
  if (similarity == (double *) NULL)
    ThrowFatalException(ResourceLimitFatalError,"MemoryAllocationFailed");
  (void) memset(similarity,0,length*sizeof(*similarity));
  status=MagickTrue;
  switch (metric)
  {
    case AbsoluteErrorMetric:
    {
      status=GetAESimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case FuzzErrorMetric:
    {
      status=GetFUZZSimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case MeanAbsoluteErrorMetric:
    {
      status=GetMAESimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case MeanErrorPerPixelMetric:
    {
      status=GetMEPPSimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case MeanSquaredErrorMetric:
    {
      status=GetMSESimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case NormalizedCrossCorrelationErrorMetric:
    {
      status=GetNCCSimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case PeakAbsoluteErrorMetric:
    {
      status=GetPASimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case PeakSignalToNoiseRatioMetric:
    {
      status=GetPSNRSimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case PerceptualHashErrorMetric:
    {
      status=GetPHASHSimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
    case RootMeanSquaredErrorMetric:
    case UndefinedErrorMetric:
    default:
    {
      status=GetRMSESimilarity(image,reconstruct_image,CompositeChannels,
        similarity,exception);
      break;
    }
  }
  if (status == MagickFalse)
    {
      similarity=(double *) RelinquishMagickMemory(similarity);
      return((double *) NULL);
    }
  distortion=similarity;
  switch (metric)
  {
    case NormalizedCrossCorrelationErrorMetric:
    {
      for (i=0; i <= (ssize_t) CompositeChannels; i++)
        distortion[i]=(1.0-distortion[i])/2.0;
      break;
    }
    default: break;
  }
  for (i=0; i <= (ssize_t) CompositeChannels; i++)
    if (fabs(distortion[i]) < MagickEpsilon)
      distortion[i]=0.0;
  return(distortion);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%  I s I m a g e s E q u a l                                                  %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  IsImagesEqual() measures the difference between colors at each pixel
%  location of two images.  A value other than 0 means the colors match
%  exactly.  Otherwise an error measure is computed by summing over all
%  pixels in an image the distance squared in RGB space between each image
%  pixel and its corresponding pixel in the reconstruct image.  The error
%  measure is assigned to these image members:
%
%    o mean_error_per_pixel:  The mean error for any single pixel in
%      the image.
%
%    o normalized_mean_error:  The normalized mean quantization error for
%      any single pixel in the image.  This distance measure is normalized to
%      a range between 0 and 1.  It is independent of the range of red, green,
%      and blue values in the image.
%
%    o normalized_maximum_error:  The normalized maximum quantization
%      error for any single pixel in the image.  This distance measure is
%      normalized to a range between 0 and 1.  It is independent of the range
%      of red, green, and blue values in your image.
%
%  A small normalized mean square error, accessed as
%  image->normalized_mean_error, suggests the images are very similar in
%  spatial layout and color.
%
%  The format of the IsImagesEqual method is:
%
%      MagickBooleanType IsImagesEqual(Image *image,
%        const Image *reconstruct_image)
%
%  A description of each parameter follows.
%
%    o image: the image.
%
%    o reconstruct_image: the reconstruct image.
%
*/
MagickExport MagickBooleanType IsImagesEqual(Image *image,
  const Image *reconstruct_image)
{
  CacheView
    *image_view,
    *reconstruct_view;

  ExceptionInfo
    *exception;

  MagickBooleanType
    status;

  MagickRealType
    area,
    gamma,
    maximum_error,
    mean_error,
    mean_error_per_pixel;

  size_t
    columns,
    rows;

  ssize_t
    y;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(reconstruct_image != (const Image *) NULL);
  assert(reconstruct_image->signature == MagickCoreSignature);
  exception=(&image->exception);
  if (ValidateImageMorphology(image,reconstruct_image) == MagickFalse)
    ThrowBinaryException(ImageError,"ImageMorphologyDiffers",image->filename);
  area=0.0;
  maximum_error=0.0;
  mean_error_per_pixel=0.0;
  mean_error=0.0;
  SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
  image_view=AcquireVirtualCacheView(image,exception);
  reconstruct_view=AcquireVirtualCacheView(reconstruct_image,exception);
  for (y=0; y < (ssize_t) rows; y++)
  {
    const IndexPacket
      *magick_restrict indexes,
      *magick_restrict reconstruct_indexes;

    const PixelPacket
      *magick_restrict p,
      *magick_restrict q;

    ssize_t
      x;

    p=GetCacheViewVirtualPixels(image_view,0,y,columns,1,exception);
    q=GetCacheViewVirtualPixels(reconstruct_view,0,y,columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (const PixelPacket *) NULL))
      break;
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    reconstruct_indexes=GetCacheViewVirtualIndexQueue(reconstruct_view);
    for (x=0; x < (ssize_t) columns; x++)
    {
      MagickRealType
        distance;

      distance=fabs((double) GetPixelRed(p)-(double) GetPixelRed(q));
      mean_error_per_pixel+=distance;
      mean_error+=distance*distance;
      if (distance > maximum_error)
        maximum_error=distance;
      area++;
      distance=fabs((double) GetPixelGreen(p)-(double) GetPixelGreen(q));
      mean_error_per_pixel+=distance;
      mean_error+=distance*distance;
      if (distance > maximum_error)
        maximum_error=distance;
      area++;
      distance=fabs((double) GetPixelBlue(p)-(double) GetPixelBlue(q));
      mean_error_per_pixel+=distance;
      mean_error+=distance*distance;
      if (distance > maximum_error)
        maximum_error=distance;
      area++;
      if (image->matte != MagickFalse)
        {
          distance=fabs((double) GetPixelOpacity(p)-(double)
            GetPixelOpacity(q));
          mean_error_per_pixel+=distance;
          mean_error+=distance*distance;
          if (distance > maximum_error)
            maximum_error=distance;
          area++;
        }
      if ((image->colorspace == CMYKColorspace) &&
          (reconstruct_image->colorspace == CMYKColorspace))
        {
          distance=fabs((double) GetPixelIndex(indexes+x)-(double)
            GetPixelIndex(reconstruct_indexes+x));
          mean_error_per_pixel+=distance;
          mean_error+=distance*distance;
          if (distance > maximum_error)
            maximum_error=distance;
          area++;
        }
      p++;
      q++;
    }
  }
  reconstruct_view=DestroyCacheView(reconstruct_view);
  image_view=DestroyCacheView(image_view);
  gamma=MagickSafeReciprocal(area);
  image->error.mean_error_per_pixel=gamma*mean_error_per_pixel;
  image->error.normalized_mean_error=gamma*QuantumScale*QuantumScale*mean_error;
  image->error.normalized_maximum_error=QuantumScale*maximum_error;
  status=image->error.mean_error_per_pixel == 0.0 ? MagickTrue : MagickFalse;
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   S i m i l a r i t y I m a g e                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  SimilarityImage() compares the reference image of the image and returns the
%  best match offset.  In addition, it returns a similarity image such that an
%  exact match location is completely white and if none of the pixels match,
%  black, otherwise some gray level in-between.
%
%  The format of the SimilarityImageImage method is:
%
%      Image *SimilarityImage(const Image *image,const Image *reference,
%        RectangleInfo *offset,double *similarity,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o reference: find an area of the image that closely resembles this image.
%
%    o the best match offset of the reference image within the image.
%
%    o similarity: the computed similarity between the images.
%
%    o exception: return any errors or warnings in this structure.
%
*/

static double GetSimilarityMetric(const Image *image,
  const Image *reconstruct_image,const MetricType metric,
  const ssize_t x_offset,const ssize_t y_offset,ExceptionInfo *exception)
{
  double
    *channel_similarity,
    similarity = 0.0;

  ExceptionInfo
    *sans_exception = AcquireExceptionInfo();

  Image
    *similarity_image;

  MagickBooleanType
    status = MagickTrue;

  RectangleInfo
    geometry;

  size_t
    length = CompositeChannels+1UL;

  SetGeometry(reconstruct_image,&geometry);
  geometry.x=x_offset;
  geometry.y=y_offset;
  similarity_image=CropImage(image,&geometry,sans_exception);
  sans_exception=DestroyExceptionInfo(sans_exception);
  if (similarity_image == (Image *) NULL)
    return(NAN);
  /*
    Get image distortion.
  */
  channel_similarity=(double *) AcquireQuantumMemory(length,
    sizeof(*channel_similarity));
  if (channel_similarity == (double *) NULL)
    ThrowFatalException(ResourceLimitFatalError,"MemoryAllocationFailed");
  (void) memset(channel_similarity,0,length*sizeof(*channel_similarity));
  switch (metric)
  {
    case AbsoluteErrorMetric:
    {
      status=GetAESimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case FuzzErrorMetric:
    {
      status=GetFUZZSimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case MeanAbsoluteErrorMetric:
    {
      status=GetMAESimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case MeanErrorPerPixelMetric:
    {
      status=GetMEPPSimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case MeanSquaredErrorMetric:
    {
      status=GetMSESimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case NormalizedCrossCorrelationErrorMetric:
    {
      status=GetNCCSimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case PeakAbsoluteErrorMetric:
    {
      status=GetPASimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case PeakSignalToNoiseRatioMetric:
    {
      status=GetPSNRSimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case PerceptualHashErrorMetric:
    {
      status=GetPHASHSimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
    case RootMeanSquaredErrorMetric:
    case UndefinedErrorMetric:
    default:
    {
      status=GetRMSESimilarity(similarity_image,reconstruct_image,
        CompositeChannels,channel_similarity,exception);
      break;
    }
  }
  similarity_image=DestroyImage(similarity_image);
  similarity=channel_similarity[CompositeChannels];
  channel_similarity=(double *) RelinquishMagickMemory(channel_similarity);
  if (status == MagickFalse)
    return(NAN);
  return(similarity);
}

MagickExport Image *SimilarityImage(Image *image,const Image *reference,
  RectangleInfo *offset,double *similarity_metric,ExceptionInfo *exception)
{
  Image
    *similarity_image;

  similarity_image=SimilarityMetricImage(image,reference,
    RootMeanSquaredErrorMetric,offset,similarity_metric,exception);
  return(similarity_image);
}

MagickExport Image *SimilarityMetricImage(Image *image,const Image *reconstruct,
  const MetricType metric,RectangleInfo *offset,double *similarity_metric,
  ExceptionInfo *exception)
{
#define SimilarityImageTag  "Similarity/Image"

  typedef struct
  {     
    double
      similarity;

    ssize_t
      x,
      y;
  } SimilarityInfo;

  CacheView
    *similarity_view;

  const char
    *artifact;

  double
    similarity_threshold;

  Image
    *similarity_image = (Image *) NULL;

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  SimilarityInfo
    similarity_info = { 0 };

  ssize_t
    y;

  assert(image != (const Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(exception != (ExceptionInfo *) NULL);
  assert(exception->signature == MagickCoreSignature);
  assert(offset != (RectangleInfo *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  SetGeometry(reconstruct,offset);
  *similarity_metric=0.0;
  offset->x=0;
  offset->y=0;
  if (ValidateImageMorphology(image,reconstruct) == MagickFalse)
    ThrowImageException(ImageError,"ImageMorphologyDiffers");
  if ((image->columns < reconstruct->columns) ||
      (image->rows < reconstruct->rows))
    {
      (void) ThrowMagickException(&image->exception,GetMagickModule(),
        OptionWarning,"GeometryDoesNotContainImage","`%s'",image->filename);
      return((Image *) NULL);
    }
  similarity_image=CloneImage(image,image->columns,image->rows,MagickTrue,
    exception);
  if (similarity_image == (Image *) NULL)
    return((Image *) NULL);
  similarity_image->depth=32;
  similarity_image->colorspace=GRAYColorspace;
  similarity_image->matte=MagickFalse;
  status=SetImageStorageClass(similarity_image,DirectClass);
  if (status == MagickFalse)
    {
      InheritException(exception,&similarity_image->exception);
      return(DestroyImage(similarity_image));
    }
  /*
    Measure similarity of reconstruction image against image.
  */
  similarity_threshold=DefaultSimilarityThreshold;
  artifact=GetImageArtifact(image,"compare:similarity-threshold");
  if (artifact != (const char *) NULL)
    similarity_threshold=StringToDouble(artifact,(char **) NULL);
  status=MagickTrue;
  similarity_info.similarity=GetSimilarityMetric(image,reconstruct,metric,
    similarity_info.x,similarity_info.y,exception);
  progress=0;
  similarity_view=AcquireVirtualCacheView(similarity_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(status,similarity_info) \
    magick_number_threads(image,reconstruct,similarity_image->rows << 2,1)
#endif    
  for (y=0; y < (ssize_t) similarity_image->rows; y++)
  {
    double
      similarity;

    MagickBooleanType
      threshold_trigger = MagickFalse;

    PixelPacket
      *magick_restrict q;

    SimilarityInfo
      channel_info = similarity_info;

    ssize_t
      x;

    if (status == MagickFalse)
      continue;
    if (threshold_trigger != MagickFalse)
      continue;
    q=QueueCacheViewAuthenticPixels(similarity_view,0,y,
      similarity_image->columns,1,exception);
    if (q == (PixelPacket *) NULL)
      {
        status=MagickFalse;
        continue;
      }
    for (x=0; x < (ssize_t) similarity_image->columns; x++)
    {
      MagickBooleanType
        update = MagickFalse;

      similarity=GetSimilarityMetric(image,reconstruct,metric,x,y,exception);
      switch (metric)
      {
        case NormalizedCrossCorrelationErrorMetric:
        case PeakSignalToNoiseRatioMetric:
        {
          if (similarity > channel_info.similarity)
            update=MagickTrue;
          break;
        }
        default:
        {
          if (similarity < channel_info.similarity)
            update=MagickTrue;
          break;
        }
      }
      if (update != MagickFalse)
        {
          channel_info.similarity=similarity;
          channel_info.x=x;
          channel_info.y=y;
        }
      switch (metric)
      {
        case NormalizedCrossCorrelationErrorMetric:
        case PeakSignalToNoiseRatioMetric:
        {
          SetPixelRed(q,ClampToQuantum((double) QuantumRange*similarity));
          break;
        }
        default:
        {
          SetPixelRed(q,ClampToQuantum((double) QuantumRange*(1.0-similarity)));
          break;
        }
      }
      SetPixelGreen(q,GetPixelRed(q));
      SetPixelBlue(q,GetPixelRed(q));
      q++;
    }
#if defined(MAGICKCORE_OPENMP_SUPPORT)
    #pragma omp critical (MagickCore_SimilarityMetricImage)
#endif
    switch (metric)
    {
      case NormalizedCrossCorrelationErrorMetric:
      case PeakSignalToNoiseRatioMetric:
      {
        if (similarity_threshold != DefaultSimilarityThreshold)
          if (channel_info.similarity >= similarity_threshold)
            threshold_trigger=MagickTrue;
        if (channel_info.similarity >= similarity_info.similarity)
          similarity_info=channel_info;
        break;
      }
      default:
      {
        if (similarity_threshold != DefaultSimilarityThreshold)
          if (channel_info.similarity < similarity_threshold)
            threshold_trigger=MagickTrue;
        if (channel_info.similarity < similarity_info.similarity)
          similarity_info=channel_info;
        break;
      }
    }
    if (SyncCacheViewAuthenticPixels(similarity_view,exception) == MagickFalse)
      status=MagickFalse;
    if (image->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

        progress++;
        proceed=SetImageProgress(image,SimilarityImageTag,progress,image->rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  similarity_view=DestroyCacheView(similarity_view);
  if (status == MagickFalse)
    similarity_image=DestroyImage(similarity_image);
  *similarity_metric=similarity_info.similarity;
  if (fabs(*similarity_metric) < MagickEpsilon)
    *similarity_metric=0.0;
  offset->x=similarity_info.x;
  offset->y=similarity_info.y;
  (void) FormatImageProperty((Image *) image,"similarity","%.*g",
    GetMagickPrecision(),*similarity_metric);
  (void) FormatImageProperty((Image *) image,"similarity.offset.x","%.*g",
    GetMagickPrecision(),(double) offset->x);
  (void) FormatImageProperty((Image *) image,"similarity.offset.y","%.*g",
    GetMagickPrecision(),(double) offset->y);
  return(similarity_image);
}
