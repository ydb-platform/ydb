/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%                      PPPP    AAA   IIIII  N   N  TTTTT                      %
%                      P   P  A   A    I    NN  N    T                        %
%                      PPPP   AAAAA    I    N N N    T                        %
%                      P      A   A    I    N  NN    T                        %
%                      P      A   A  IIIII  N   N    T                        %
%                                                                             %
%                                                                             %
%                        Methods to Paint on an Image                         %
%                                                                             %
%                              Software Design                                %
%                                   Cristy                                    %
%                                 July 1998                                   %
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
#include "magick/artifact.h"
#include "magick/cache.h"
#include "magick/channel.h"
#include "magick/color-private.h"
#include "magick/colorspace-private.h"
#include "magick/composite.h"
#include "magick/composite-private.h"
#include "magick/draw.h"
#include "magick/draw-private.h"
#include "magick/exception.h"
#include "magick/exception-private.h"
#include "magick/gem.h"
#include "magick/monitor.h"
#include "magick/monitor-private.h"
#include "magick/option.h"
#include "magick/paint.h"
#include "magick/pixel-private.h"
#include "magick/resource_.h"
#include "magick/string_.h"
#include "magick/string-private.h"
#include "magick/thread-private.h"

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   F l o o d f i l l P a i n t I m a g e                                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  FloodfillPaintImage() changes the color value of any pixel that matches
%  target and is an immediate neighbor.  If the method FillToBorderMethod is
%  specified, the color value is changed for any neighbor pixel that does not
%  match the bordercolor member of image.
%
%  By default target must match a particular pixel color exactly.
%  However, in many cases two colors may differ by a small amount.  The
%  fuzz member of image defines how much tolerance is acceptable to
%  consider two colors as the same.  For example, set fuzz to 10 and the
%  color red at intensities of 100 and 102 respectively are now
%  interpreted as the same color for the purposes of the floodfill.
%
%  The format of the FloodfillPaintImage method is:
%
%      MagickBooleanType FloodfillPaintImage(Image *image,
%        const ChannelType channel,const DrawInfo *draw_info,
%        const MagickPixelPacket target,const ssize_t x_offset,
%        const ssize_t y_offset,const MagickBooleanType invert)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o channel: the channel(s).
%
%    o draw_info: the draw info.
%
%    o target: the RGB value of the target color.
%
%    o x_offset,y_offset: the starting location of the operation.
%
%    o invert: paint any pixel that does not match the target color.
%
*/
MagickExport MagickBooleanType FloodfillPaintImage(Image *image,
  const ChannelType channel,const DrawInfo *draw_info,
  const MagickPixelPacket *target,const ssize_t x_offset,const ssize_t y_offset,
  const MagickBooleanType invert)
{
#define MaxStacksize  524288UL
#define PushSegmentStack(up,left,right,delta) \
{ \
  if (s >= (segment_stack+MaxStacksize)) \
    { \
      segment_info=RelinquishVirtualMemory(segment_info); \
      image_view=DestroyCacheView(image_view); \
      floodplane_view=DestroyCacheView(floodplane_view); \
      floodplane_image=DestroyImage(floodplane_image); \
      ThrowBinaryException(DrawError,"SegmentStackOverflow",image->filename) \
    } \
  else \
    { \
      if ((((up)+(delta)) >= 0) && (((up)+(delta)) < (ssize_t) image->rows)) \
        { \
          s->x1=(double) (left); \
          s->y1=(double) (up); \
          s->x2=(double) (right); \
          s->y2=(double) (delta); \
          s++; \
        } \
    } \
}

  CacheView
    *floodplane_view,
    *image_view;

  ExceptionInfo
    *exception;

  Image
    *floodplane_image;

  MagickBooleanType
    skip,
    status;

  MagickPixelPacket
    pixel;

  MemoryInfo
    *segment_info;

  SegmentInfo
    *s,
    *segment_stack;

  ssize_t
    offset,
    start,
    x,
    x1,
    x2,
    y;

  /*
    Check boundary conditions.
  */
  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(draw_info != (DrawInfo *) NULL);
  assert(draw_info->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  if ((x_offset < 0) || (x_offset >= (ssize_t) image->columns))
    return(MagickFalse);
  if ((y_offset < 0) || (y_offset >= (ssize_t) image->rows))
    return(MagickFalse);
  if (SetImageStorageClass(image,DirectClass) == MagickFalse)
    return(MagickFalse);
  exception=(&image->exception);
  if (IsGrayColorspace(image->colorspace) != MagickFalse)
    (void) SetImageColorspace(image,sRGBColorspace);
  if ((image->matte == MagickFalse) &&
      (draw_info->fill.opacity != OpaqueOpacity))
    (void) SetImageAlphaChannel(image,OpaqueAlphaChannel);
  /*
    Set floodfill state.
  */
  floodplane_image=CloneImage(image,0,0,MagickTrue,&image->exception);
  if (floodplane_image == (Image *) NULL)
    return(MagickFalse);
  floodplane_image->matte=MagickFalse;
  floodplane_image->colorspace=GRAYColorspace;
  (void) QueryColorCompliance("#000",AllCompliance,
    &floodplane_image->background_color,exception);
  (void) SetImageBackgroundColor(floodplane_image);
  segment_info=AcquireVirtualMemory(MaxStacksize,sizeof(*segment_stack));
  if (segment_info == (MemoryInfo *) NULL)
    {
      floodplane_image=DestroyImage(floodplane_image);
      ThrowBinaryException(ResourceLimitError,"MemoryAllocationFailed",
        image->filename);
    }
  segment_stack=(SegmentInfo *) GetVirtualMemoryBlob(segment_info);
  /*
    Push initial segment on stack.
  */
  x=x_offset;
  y=y_offset;
  start=0;
  s=segment_stack;
  GetMagickPixelPacket(image,&pixel);
  image_view=AcquireVirtualCacheView(image,exception);
  floodplane_view=AcquireAuthenticCacheView(floodplane_image,exception);
  PushSegmentStack(y,x,x,1);
  PushSegmentStack(y+1,x,x,-1);
  while (s > segment_stack)
  {
    const IndexPacket
      *magick_restrict indexes;

    const PixelPacket
      *magick_restrict p;

    PixelPacket
      *magick_restrict q;

    ssize_t
      x;

    /*
      Pop segment off stack.
    */
    s--;
    x1=(ssize_t) s->x1;
    x2=(ssize_t) s->x2;
    offset=(ssize_t) s->y2;
    y=(ssize_t) s->y1+offset;
    /*
      Recolor neighboring pixels.
    */
    p=GetCacheViewVirtualPixels(image_view,0,y,(size_t) (x1+1),1,exception);
    q=GetCacheViewAuthenticPixels(floodplane_view,0,y,(size_t) (x1+1),1,
      exception);
    if ((p == (const PixelPacket *) NULL) || (q == (PixelPacket *) NULL))
      break;
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    p+=(ptrdiff_t) x1;
    q+=(ptrdiff_t) x1;
    for (x=x1; x >= 0; x--)
    {
      if (GetPixelGray(q) != 0)
        break;
      SetMagickPixelPacket(image,p,indexes+x,&pixel);
      if (IsMagickColorSimilar(&pixel,target) == invert)
        break;
      SetPixelGray(q,QuantumRange);
      p--;
      q--;
    }
    if (SyncCacheViewAuthenticPixels(floodplane_view,exception) == MagickFalse)
      break;
    skip=x >= x1 ? MagickTrue : MagickFalse;
    if (skip == MagickFalse)
      {
        start=x+1;
        if (start < x1)
          PushSegmentStack(y,start,x1-1,-offset);
        x=x1+1;
      }
    do
    {
      if (skip == MagickFalse)
        {
          if (x < (ssize_t) image->columns)
            {
              p=GetCacheViewVirtualPixels(image_view,x,y,image->columns-x,1,
                exception);
              q=GetCacheViewAuthenticPixels(floodplane_view,x,y,
                image->columns-x,1,exception);
              if ((p == (const PixelPacket *) NULL) ||
                  (q == (PixelPacket *) NULL))
                break;
              indexes=GetCacheViewVirtualIndexQueue(image_view);
              for ( ; x < (ssize_t) image->columns; x++)
              {
                if (GetPixelGray(q) != 0)
                  break;
                SetMagickPixelPacket(image,p,indexes+x,&pixel);
                if (IsMagickColorSimilar(&pixel,target) == invert)
                  break;
                SetPixelGray(q,QuantumRange);
                p++;
                q++;
              }
              if (SyncCacheViewAuthenticPixels(floodplane_view,exception) == MagickFalse)
                break;
            }
          PushSegmentStack(y,start,x-1,offset);
          if (x > (x2+1))
            PushSegmentStack(y,x2+1,x-1,-offset);
        }
      skip=MagickFalse;
      x++;
      if (x <= x2)
        {
          p=GetCacheViewVirtualPixels(image_view,x,y,(size_t) (x2-x+1),1,
            exception);
          q=GetCacheViewAuthenticPixels(floodplane_view,x,y,(size_t) (x2-x+1),1,
            exception);
          if ((p == (const PixelPacket *) NULL) || (q == (PixelPacket *) NULL))
            break;
          indexes=GetCacheViewVirtualIndexQueue(image_view);
          for ( ; x <= x2; x++)
          {
            if (GetPixelGray(q) != 0)
              break;
            SetMagickPixelPacket(image,p,indexes+x,&pixel);
            if (IsMagickColorSimilar(&pixel,target) != invert)
              break;
            p++;
            q++;
          }
        }
      start=x;
    } while (x <= x2);
  }
  status=MagickTrue;
#if defined(MMAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(status) \
    magick_number_threads(floodplane_image,image,image->rows,2)
#endif
  for (y=0; y < (ssize_t) image->rows; y++)
  {
    const PixelPacket
      *magick_restrict p;

    IndexPacket
      *magick_restrict indexes;

    PixelPacket
      *magick_restrict q;

    ssize_t
      x;

    /*
      Tile fill color onto floodplane.
    */
    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(floodplane_view,0,y,image->columns,1,exception);
    q=GetCacheViewAuthenticPixels(image_view,0,y,image->columns,1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewAuthenticIndexQueue(image_view);
    for (x=0; x < (ssize_t) image->columns; x++)
    {
      if (GetPixelGray(p) != 0)
        {
          MagickPixelPacket
            fill;

          PixelPacket
            fill_color;

          (void) GetFillColor(draw_info,x,y,&fill_color);
          GetMagickPixelPacket(image,&fill);
          SetMagickPixelPacket(image,&fill_color,(IndexPacket *) NULL,&fill);
          if (image->colorspace == CMYKColorspace)
            ConvertRGBToCMYK(&fill);
          if ((channel & RedChannel) != 0)
            SetPixelRed(q,ClampToQuantum(fill.red));
          if ((channel & GreenChannel) != 0)
            SetPixelGreen(q,ClampToQuantum(fill.green));
          if ((channel & BlueChannel) != 0)
            SetPixelBlue(q,ClampToQuantum(fill.blue));
          if (((channel & OpacityChannel) != 0) ||
              (draw_info->fill.opacity != OpaqueOpacity))
            SetPixelOpacity(q,ClampToQuantum(fill.opacity));
          if (((channel & IndexChannel) != 0) &&
              (image->colorspace == CMYKColorspace))
            SetPixelIndex(indexes+x,ClampToQuantum(fill.index));
        }
      p++;
      q++;
    }
    if (SyncCacheViewAuthenticPixels(image_view,exception) == MagickFalse)
      status=MagickFalse;
  }
  floodplane_view=DestroyCacheView(floodplane_view);
  image_view=DestroyCacheView(image_view);
  segment_info=RelinquishVirtualMemory(segment_info);
  floodplane_image=DestroyImage(floodplane_image);
  return(y == (ssize_t) image->rows ? MagickTrue : MagickFalse);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+     G r a d i e n t I m a g e                                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GradientImage() applies a continuously smooth color transitions along a
%  vector from one color to another.
%
%  Note, the interface of this method will change in the future to support
%  more than one transition.
%
%  The format of the GradientImage method is:
%
%      MagickBooleanType GradientImage(Image *image,const GradientType type,
%        const SpreadMethod method,const PixelPacket *start_color,
%        const PixelPacket *stop_color)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o type: the gradient type: linear or radial.
%
%    o spread: the gradient spread method: pad, reflect, or repeat.
%
%    o start_color: the start color.
%
%    o stop_color: the stop color.
%
% This provides a good example of making use of the DrawGradientImage
% function and the gradient structure in draw_info.
%
*/
MagickExport MagickBooleanType GradientImage(Image *image,
  const GradientType type,const SpreadMethod method,
  const PixelPacket *start_color,const PixelPacket *stop_color)
{
  const char
    *artifact;

  DrawInfo
    *draw_info;

  GradientInfo
    *gradient;

  MagickBooleanType
    status;

  ssize_t
    i;

  /*
    Set gradient start-stop end points.
  */
  assert(image != (const Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(start_color != (const PixelPacket *) NULL);
  assert(stop_color != (const PixelPacket *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  draw_info=AcquireDrawInfo();
  gradient=(&draw_info->gradient);
  gradient->type=type;
  gradient->bounding_box.width=image->columns;
  gradient->bounding_box.height=image->rows;
  artifact=GetImageArtifact(image,"gradient:bounding-box");
  if (artifact != (const char *) NULL)
    (void) ParseAbsoluteGeometry(artifact,&gradient->bounding_box);
  gradient->gradient_vector.x2=(double) image->columns-1;
  gradient->gradient_vector.y2=(double) image->rows-1;
  artifact=GetImageArtifact(image,"gradient:direction");
  if (artifact != (const char *) NULL)
    {
      GravityType
        direction;

      direction=(GravityType) ParseCommandOption(MagickGravityOptions,
        MagickFalse,artifact);
      switch (direction)
      {
        case NorthWestGravity:
        {
          gradient->gradient_vector.x1=(double) image->columns-1;
          gradient->gradient_vector.y1=(double) image->rows-1;
          gradient->gradient_vector.x2=0.0;
          gradient->gradient_vector.y2=0.0;
          break;
        }
        case NorthGravity:
        {
          gradient->gradient_vector.x1=0.0;
          gradient->gradient_vector.y1=(double) image->rows-1;
          gradient->gradient_vector.x2=0.0;
          gradient->gradient_vector.y2=0.0;
          break;
        }
        case NorthEastGravity:
        {
          gradient->gradient_vector.x1=0.0;
          gradient->gradient_vector.y1=(double) image->rows-1;
          gradient->gradient_vector.x2=(double) image->columns-1;
          gradient->gradient_vector.y2=0.0;
          break;
        }
        case WestGravity:
        {
          gradient->gradient_vector.x1=(double) image->columns-1;
          gradient->gradient_vector.y1=0.0;
          gradient->gradient_vector.x2=0.0;
          gradient->gradient_vector.y2=0.0;
          break;
        }
        case EastGravity:
        {
          gradient->gradient_vector.x1=0.0;
          gradient->gradient_vector.y1=0.0;
          gradient->gradient_vector.x2=(double) image->columns-1;
          gradient->gradient_vector.y2=0.0;
          break;
        }
        case SouthWestGravity:
        {
          gradient->gradient_vector.x1=(double) image->columns-1;
          gradient->gradient_vector.y1=0.0;
          gradient->gradient_vector.x2=0.0;
          gradient->gradient_vector.y2=(double) image->rows-1;
          break;
        }
        case SouthGravity:
        {
          gradient->gradient_vector.x1=0.0;
          gradient->gradient_vector.y1=0.0;
          gradient->gradient_vector.x2=0.0;
          gradient->gradient_vector.y2=(double) image->columns-1;
          break;
        }
        case SouthEastGravity:
        {
          gradient->gradient_vector.x1=0.0;
          gradient->gradient_vector.y1=0.0;
          gradient->gradient_vector.x2=(double) image->columns-1;
          gradient->gradient_vector.y2=(double) image->rows-1;
          break;
        }
        default:
          break;
      }
    }
  artifact=GetImageArtifact(image,"gradient:angle");
  if (artifact != (const char *) NULL)
    gradient->angle=(MagickRealType) StringToDouble(artifact,(char **) NULL);
  artifact=GetImageArtifact(image,"gradient:vector");
  if (artifact != (const char *) NULL)
    (void) sscanf(artifact,"%lf%*[ ,]%lf%*[ ,]%lf%*[ ,]%lf",
      &gradient->gradient_vector.x1,&gradient->gradient_vector.y1,
      &gradient->gradient_vector.x2,&gradient->gradient_vector.y2);
  if ((GetImageArtifact(image,"gradient:angle") == (const char *) NULL) &&
      (GetImageArtifact(image,"gradient:direction") == (const char *) NULL) &&
      (GetImageArtifact(image,"gradient:extent") == (const char *) NULL) &&
      (GetImageArtifact(image,"gradient:vector") == (const char *) NULL))
    if ((type == LinearGradient) && (gradient->gradient_vector.y2 != 0.0))
      gradient->gradient_vector.x2=0.0;
  gradient->center.x=(double) gradient->gradient_vector.x2/2.0;
  gradient->center.y=(double) gradient->gradient_vector.y2/2.0;
  artifact=GetImageArtifact(image,"gradient:center");
  if (artifact != (const char *) NULL)
    (void) sscanf(artifact,"%lf%*[ ,]%lf",&gradient->center.x,
      &gradient->center.y);
  artifact=GetImageArtifact(image,"gradient:angle");
  if ((type == LinearGradient) && (artifact != (const char *) NULL))
    {
      double
        sine,
        cosine,
        distance;

      /*
        Reference https://drafts.csswg.org/css-images-3/#linear-gradients.
      */
      sine=sin((double) DegreesToRadians(gradient->angle-90.0));
      cosine=cos((double) DegreesToRadians(gradient->angle-90.0));
      distance=fabs((double) (image->columns-1)*cosine)+
        fabs((double) (image->rows-1)*sine);
      gradient->gradient_vector.x1=0.5*((image->columns-1)-distance*cosine);
      gradient->gradient_vector.y1=0.5*((image->rows-1)-distance*sine);
      gradient->gradient_vector.x2=0.5*((image->columns-1)+distance*cosine);
      gradient->gradient_vector.y2=0.5*((image->rows-1)+distance*sine);
    }
  gradient->radii.x=(double) MagickMax((image->columns-1),(image->rows-1))/2.0;
  gradient->radii.y=gradient->radii.x;
  artifact=GetImageArtifact(image,"gradient:extent");
  if (artifact != (const char *) NULL)
    {
      if (LocaleCompare(artifact,"Circle") == 0)
        {
          gradient->radii.x=(double) (MagickMax((image->columns-1),
           (image->rows-1)))/2.0;
          gradient->radii.y=gradient->radii.x;
        }
      if (LocaleCompare(artifact,"Diagonal") == 0)
        {
          gradient->radii.x=(double) (sqrt((double) (image->columns-1)*
            (image->columns-1)+(image->rows-1)*(image->rows-1)))/2.0;
          gradient->radii.y=gradient->radii.x;
        }
      if (LocaleCompare(artifact,"Ellipse") == 0)
        {
          gradient->radii.x=(double) (image->columns-1)/2.0;
          gradient->radii.y=(double) (image->rows-1)/2.0;
        }
      if (LocaleCompare(artifact,"Maximum") == 0)
        {
          gradient->radii.x=(double) MagickMax((image->columns-1),
            (image->rows-1))/2.0;
          gradient->radii.y=gradient->radii.x;
        }
      if (LocaleCompare(artifact,"Minimum") == 0)
        {
          gradient->radii.x=(double) MagickMin((image->columns-1),
            (image->rows-1))/2.0;
          gradient->radii.y=gradient->radii.x;
        }
    }
  artifact=GetImageArtifact(image,"gradient:radii");
  if (artifact != (const char *) NULL)
    (void) sscanf(artifact,"%lf%*[ ,]%lf",&gradient->radii.x,
      &gradient->radii.y);
  gradient->radius=MagickMax(gradient->radii.x,gradient->radii.y);
  gradient->spread=method;
  /*
    Define the gradient to fill between the stops.
  */
  gradient->number_stops=2;
  gradient->stops=(StopInfo *) AcquireQuantumMemory(gradient->number_stops,
    sizeof(*gradient->stops));
  if (gradient->stops == (StopInfo *) NULL)
    ThrowBinaryImageException(ResourceLimitError,"MemoryAllocationFailed",
      image->filename);
  (void) memset(gradient->stops,0,gradient->number_stops*
    sizeof(*gradient->stops));
  for (i=0; i < (ssize_t) gradient->number_stops; i++)
    GetMagickPixelPacket(image,&gradient->stops[i].color);
  SetMagickPixelPacket(image,start_color,(IndexPacket *) NULL,
    &gradient->stops[0].color);
  gradient->stops[0].offset=0.0;
  SetMagickPixelPacket(image,stop_color,(IndexPacket *) NULL,
    &gradient->stops[1].color);
  gradient->stops[1].offset=1.0;
  /*
    Draw a gradient on the image.
  */
  status=DrawGradientImage(image,draw_info);
  draw_info=DestroyDrawInfo(draw_info);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     O i l P a i n t I m a g e                                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  OilPaintImage() applies a special effect filter that simulates an oil
%  painting.  Each pixel is replaced by the most frequent color occurring
%  in a circular region defined by radius.
%
%  The format of the OilPaintImage method is:
%
%      Image *OilPaintImage(const Image *image,const double radius,
%        ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o radius: the radius of the circular neighborhood.
%
%    o exception: return any errors or warnings in this structure.
%
*/

static size_t **DestroyHistogramTLS(size_t **histogram)
{
  ssize_t
    i;

  assert(histogram != (size_t **) NULL);
  for (i=0; i < (ssize_t) GetMagickResourceLimit(ThreadResource); i++)
    if (histogram[i] != (size_t *) NULL)
      histogram[i]=(size_t *) RelinquishMagickMemory(histogram[i]);
  histogram=(size_t **) RelinquishMagickMemory(histogram);
  return(histogram);
}

static size_t **AcquireHistogramTLS(const size_t count)
{
  ssize_t
    i;

  size_t
    **histogram,
    number_threads;

  number_threads=(size_t) GetMagickResourceLimit(ThreadResource);
  histogram=(size_t **) AcquireQuantumMemory(number_threads,
    sizeof(*histogram));
  if (histogram == (size_t **) NULL)
    return((size_t **) NULL);
  (void) memset(histogram,0,number_threads*sizeof(*histogram));
  for (i=0; i < (ssize_t) number_threads; i++)
  {
    histogram[i]=(size_t *) AcquireQuantumMemory(count,
      sizeof(**histogram));
    if (histogram[i] == (size_t *) NULL)
      return(DestroyHistogramTLS(histogram));
  }
  return(histogram);
}

MagickExport Image *OilPaintImage(const Image *image,const double radius,
  ExceptionInfo *exception)
{
#define NumberPaintBins  256
#define OilPaintImageTag  "OilPaint/Image"

  CacheView
    *image_view,
    *paint_view;

  Image
    *linear_image,
    *paint_image;

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  size_t
    **magick_restrict histograms,
    width;

  ssize_t
    y;

  /*
    Initialize painted image attributes.
  */
  assert(image != (const Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(exception != (ExceptionInfo *) NULL);
  assert(exception->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  width=GetOptimalKernelWidth2D(radius,0.5);
  linear_image=CloneImage(image,0,0,MagickTrue,exception);
  paint_image=CloneImage(image,0,0,MagickTrue,exception);
  if ((linear_image == (Image *) NULL) || (paint_image == (Image *) NULL))
    {
      if (linear_image != (Image *) NULL)
        linear_image=DestroyImage(linear_image);
      if (paint_image != (Image *) NULL)
        linear_image=DestroyImage(paint_image);
      return((Image *) NULL);
    }
  if (SetImageStorageClass(paint_image,DirectClass) == MagickFalse)
    {
      InheritException(exception,&paint_image->exception);
      linear_image=DestroyImage(linear_image);
      paint_image=DestroyImage(paint_image);
      return((Image *) NULL);
    }
  histograms=AcquireHistogramTLS(NumberPaintBins);
  if (histograms == (size_t **) NULL)
    {
      linear_image=DestroyImage(linear_image);
      paint_image=DestroyImage(paint_image);
      ThrowImageException(ResourceLimitError,"MemoryAllocationFailed");
    }
  /*
    Oil paint image.
  */
  status=MagickTrue;
  progress=0;
  image_view=AcquireVirtualCacheView(linear_image,exception);
  paint_view=AcquireAuthenticCacheView(paint_image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(progress,status) \
    magick_number_threads(linear_image,paint_image,linear_image->rows,1)
#endif
  for (y=0; y < (ssize_t) linear_image->rows; y++)
  {
    const IndexPacket
      *magick_restrict indexes;

    const PixelPacket
      *magick_restrict p;

    IndexPacket
      *magick_restrict paint_indexes;

    ssize_t
      x;

    PixelPacket
      *magick_restrict q;

    size_t
      *histogram;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,-((ssize_t) width/2L),y-(ssize_t)
      (width/2L),linear_image->columns+width,width,exception);
    q=QueueCacheViewAuthenticPixels(paint_view,0,y,paint_image->columns,1,
      exception);
    if ((p == (const PixelPacket *) NULL) || (q == (PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    paint_indexes=GetCacheViewAuthenticIndexQueue(paint_view);
    histogram=histograms[GetOpenMPThreadId()];
    for (x=0; x < (ssize_t) linear_image->columns; x++)
    {
      ssize_t
        i,
        u;

      size_t
        count;

      ssize_t
        j,
        k,
        v;

      /*
        Assign most frequent color.
      */
      i=0;
      j=0;
      count=0;
      (void) memset(histogram,0,NumberPaintBins*sizeof(*histogram));
      for (v=0; v < (ssize_t) width; v++)
      {
        for (u=0; u < (ssize_t) width; u++)
        {
          k=(ssize_t) ScaleQuantumToChar(ClampToQuantum(GetPixelIntensity(
            linear_image,p+u+i)));
          histogram[k]++;
          if (histogram[k] > count)
            {
              j=i+u;
              count=histogram[k];
            }
        }
        i+=(ssize_t) (linear_image->columns+width);
      }
      *q=(*(p+j));
      if (linear_image->colorspace == CMYKColorspace)
        SetPixelIndex(paint_indexes+x,GetPixelIndex(indexes+x+j));
      p++;
      q++;
    }
    if (SyncCacheViewAuthenticPixels(paint_view,exception) == MagickFalse)
      status=MagickFalse;
    if (image->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

#if defined(MAGICKCORE_OPENMP_SUPPORT)
        #pragma omp atomic
#endif
        progress++;
        proceed=SetImageProgress(image,OilPaintImageTag,progress,image->rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  paint_view=DestroyCacheView(paint_view);
  image_view=DestroyCacheView(image_view);
  histograms=DestroyHistogramTLS(histograms);
  linear_image=DestroyImage(linear_image);
  if (status == MagickFalse)
    paint_image=DestroyImage(paint_image);
  return(paint_image);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     O p a q u e P a i n t I m a g e                                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  OpaquePaintImage() changes any pixel that matches color with the color
%  defined by fill.
%
%  By default color must match a particular pixel color exactly.  However,
%  in many cases two colors may differ by a small amount.  Fuzz defines
%  how much tolerance is acceptable to consider two colors as the same.
%  For example, set fuzz to 10 and the color red at intensities of 100 and
%  102 respectively are now interpreted as the same color.
%
%  The format of the OpaquePaintImage method is:
%
%      MagickBooleanType OpaquePaintImage(Image *image,
%        const PixelPacket *target,const PixelPacket *fill,
%        const MagickBooleanType invert)
%      MagickBooleanType OpaquePaintImageChannel(Image *image,
%        const ChannelType channel,const PixelPacket *target,
%        const PixelPacket *fill,const MagickBooleanType invert)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o channel: the channel(s).
%
%    o target: the RGB value of the target color.
%
%    o fill: the replacement color.
%
%    o invert: paint any pixel that does not match the target color.
%
*/

MagickExport MagickBooleanType OpaquePaintImage(Image *image,
  const MagickPixelPacket *target,const MagickPixelPacket *fill,
  const MagickBooleanType invert)
{
  return(OpaquePaintImageChannel(image,CompositeChannels,target,fill,invert));
}

MagickExport MagickBooleanType OpaquePaintImageChannel(Image *image,
  const ChannelType channel,const MagickPixelPacket *target,
  const MagickPixelPacket *fill,const MagickBooleanType invert)
{
#define OpaquePaintImageTag  "Opaque/Image"

  CacheView
    *image_view;

  ExceptionInfo
    *exception;

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  MagickPixelPacket
    conform_fill,
    conform_target,
    zero;

  ssize_t
    y;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(target != (MagickPixelPacket *) NULL);
  assert(fill != (MagickPixelPacket *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  if (SetImageStorageClass(image,DirectClass) == MagickFalse)
    return(MagickFalse);
  exception=(&image->exception);
  ConformMagickPixelPacket(image,fill,&conform_fill,exception);
  ConformMagickPixelPacket(image,target,&conform_target,exception);
  /*
    Make image color opaque.
  */
  status=MagickTrue;
  progress=0;
  GetMagickPixelPacket(image,&zero);
  image_view=AcquireAuthenticCacheView(image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(progress,status) \
    magick_number_threads(image,image,image->rows,1)
#endif
  for (y=0; y < (ssize_t) image->rows; y++)
  {
    MagickPixelPacket
      pixel;

    IndexPacket
      *magick_restrict indexes;

    ssize_t
      x;

    PixelPacket
      *magick_restrict q;

    if (status == MagickFalse)
      continue;
    q=GetCacheViewAuthenticPixels(image_view,0,y,image->columns,1,exception);
    if (q == (PixelPacket *) NULL)
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewAuthenticIndexQueue(image_view);
    pixel=zero;
    for (x=0; x < (ssize_t) image->columns; x++)
    {
      SetMagickPixelPacket(image,q,indexes+x,&pixel);
      if (IsMagickColorSimilar(&pixel,&conform_target) != invert)
        {
          if ((channel & RedChannel) != 0)
            SetPixelRed(q,ClampToQuantum(conform_fill.red));
          if ((channel & GreenChannel) != 0)
            SetPixelGreen(q,ClampToQuantum(conform_fill.green));
          if ((channel & BlueChannel) != 0)
            SetPixelBlue(q,ClampToQuantum(conform_fill.blue));
          if ((channel & OpacityChannel) != 0)
            SetPixelOpacity(q,ClampToQuantum(conform_fill.opacity));
          if (((channel & IndexChannel) != 0) &&
              (image->colorspace == CMYKColorspace))
            SetPixelIndex(indexes+x,ClampToQuantum(conform_fill.index));
        }
      q++;
    }
    if (SyncCacheViewAuthenticPixels(image_view,exception) == MagickFalse)
      status=MagickFalse;
    if (image->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

#if defined(MAGICKCORE_OPENMP_SUPPORT)
        #pragma omp atomic
#endif
        progress++;
        proceed=SetImageProgress(image,OpaquePaintImageTag,progress,
          image->rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  image_view=DestroyCacheView(image_view);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     T r a n s p a r e n t P a i n t I m a g e                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  TransparentPaintImage() changes the opacity value associated with any pixel
%  that matches color to the value defined by opacity.
%
%  By default color must match a particular pixel color exactly.  However,
%  in many cases two colors may differ by a small amount.  Fuzz defines
%  how much tolerance is acceptable to consider two colors as the same.
%  For example, set fuzz to 10 and the color red at intensities of 100 and
%  102 respectively are now interpreted as the same color.
%
%  The format of the TransparentPaintImage method is:
%
%      MagickBooleanType TransparentPaintImage(Image *image,
%        const MagickPixelPacket *target,const Quantum opacity,
%        const MagickBooleanType invert)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o target: the target color.
%
%    o opacity: the replacement opacity value.
%
%    o invert: paint any pixel that does not match the target color.
%
*/
MagickExport MagickBooleanType TransparentPaintImage(Image *image,
  const MagickPixelPacket *target,const Quantum opacity,
  const MagickBooleanType invert)
{
#define TransparentPaintImageTag  "Transparent/Image"

  CacheView
    *image_view;

  ExceptionInfo
    *exception;

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  MagickPixelPacket
    zero;

  ssize_t
    y;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(target != (MagickPixelPacket *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  if (SetImageStorageClass(image,DirectClass) == MagickFalse)
    return(MagickFalse);
  if (image->matte == MagickFalse)
    (void) SetImageAlphaChannel(image,OpaqueAlphaChannel);
  /*
    Make image color transparent.
  */
  status=MagickTrue;
  progress=0;
  exception=(&image->exception);
  GetMagickPixelPacket(image,&zero);
  image_view=AcquireAuthenticCacheView(image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(progress,status) \
    magick_number_threads(image,image,image->rows,1)
#endif
  for (y=0; y < (ssize_t) image->rows; y++)
  {
    MagickPixelPacket
      pixel;

    IndexPacket
      *magick_restrict indexes;

    ssize_t
      x;

    PixelPacket
      *magick_restrict q;

    if (status == MagickFalse)
      continue;
    q=GetCacheViewAuthenticPixels(image_view,0,y,image->columns,1,exception);
    if (q == (PixelPacket *) NULL)
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewAuthenticIndexQueue(image_view);
    pixel=zero;
    for (x=0; x < (ssize_t) image->columns; x++)
    {
      SetMagickPixelPacket(image,q,indexes+x,&pixel);
      if (IsMagickColorSimilar(&pixel,target) != invert)
        q->opacity=opacity;
      q++;
    }
    if (SyncCacheViewAuthenticPixels(image_view,exception) == MagickFalse)
      status=MagickFalse;
    if (image->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

#if defined(MAGICKCORE_OPENMP_SUPPORT)
        #pragma omp atomic
#endif
        progress++;
        proceed=SetImageProgress(image,TransparentPaintImageTag,progress,
          image->rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  image_view=DestroyCacheView(image_view);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     T r a n s p a r e n t P a i n t I m a g e C h r o m a                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  TransparentPaintImageChroma() changes the opacity value associated with any
%  pixel that matches color to the value defined by opacity.
%
%  As there is one fuzz value for the all the channels, the
%  TransparentPaintImage() API is not suitable for the operations like chroma,
%  where the tolerance for similarity of two color component (RGB) can be
%  different, Thus we define this method take two target pixels (one
%  low and one hight) and all the pixels of an image which are lying between
%  these two pixels are made transparent.
%
%  The format of the TransparentPaintImage method is:
%
%      MagickBooleanType TransparentPaintImage(Image *image,
%        const MagickPixelPacket *low,const MagickPixelPacket *hight,
%        const Quantum opacity,const MagickBooleanType invert)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o low: the low target color.
%
%    o high: the high target color.
%
%    o opacity: the replacement opacity value.
%
%    o invert: paint any pixel that does not match the target color.
%
*/
MagickExport MagickBooleanType TransparentPaintImageChroma(Image *image,
  const MagickPixelPacket *low,const MagickPixelPacket *high,
  const Quantum opacity,const MagickBooleanType invert)
{
#define TransparentPaintImageTag  "Transparent/Image"

  CacheView
    *image_view;

  ExceptionInfo
    *exception;

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  ssize_t
    y;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(high != (MagickPixelPacket *) NULL);
  assert(low != (MagickPixelPacket *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  if (SetImageStorageClass(image,DirectClass) == MagickFalse)
    return(MagickFalse);
  if (image->matte == MagickFalse)
    (void) SetImageAlphaChannel(image,ResetAlphaChannel);
  /*
    Make image color transparent.
  */
  status=MagickTrue;
  progress=0;
  exception=(&image->exception);
  image_view=AcquireAuthenticCacheView(image,exception);
#if defined(MAGICKCORE_OPENMP_SUPPORT)
  #pragma omp parallel for schedule(static) shared(progress,status) \
    magick_number_threads(image,image,image->rows,1)
#endif
  for (y=0; y < (ssize_t) image->rows; y++)
  {
    MagickBooleanType
      match;

    MagickPixelPacket
      pixel;

    IndexPacket
      *magick_restrict indexes;

    ssize_t
      x;

    PixelPacket
      *magick_restrict q;

    if (status == MagickFalse)
      continue;
    q=GetCacheViewAuthenticPixels(image_view,0,y,image->columns,1,exception);
    if (q == (PixelPacket *) NULL)
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewAuthenticIndexQueue(image_view);
    GetMagickPixelPacket(image,&pixel);
    for (x=0; x < (ssize_t) image->columns; x++)
    {
      SetMagickPixelPacket(image,q,indexes+x,&pixel);
      match=((pixel.red >= low->red) && (pixel.red <= high->red) &&
        (pixel.green >= low->green) && (pixel.green <= high->green) &&
        (pixel.blue  >= low->blue) && (pixel.blue <= high->blue)) ? MagickTrue :        MagickFalse;
      if (match != invert)
        q->opacity=opacity;
      q++;
    }
    if (SyncCacheViewAuthenticPixels(image_view,exception) == MagickFalse)
      status=MagickFalse;
    if (image->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

#if defined(MAGICKCORE_OPENMP_SUPPORT)
        #pragma omp atomic
#endif
        progress++;
        proceed=SetImageProgress(image,TransparentPaintImageTag,progress,
          image->rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  image_view=DestroyCacheView(image_view);
  return(status);
}
