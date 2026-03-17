/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%                   V   V  IIIII  SSSSS  IIIII   OOO   N   N                  %
%                   V   V    I    SS       I    O   O  NN  N                  %
%                   V   V    I     SSS     I    O   O  N N N                  %
%                    V V     I       SS    I    O   O  N  NN                  %
%                     V    IIIII  SSSSS  IIIII   OOO   N   N                  %
%                                                                             %
%                                                                             %
%                      MagickCore Computer Vision Methods                     %
%                                                                             %
%                              Software Design                                %
%                                   Cristy                                    %
%                               September 2014                                %
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

#include "magick/studio.h"
#include "magick/artifact.h"
#include "magick/blob.h"
#include "magick/cache-view.h"
#include "magick/color.h"
#include "magick/color-private.h"
#include "magick/colormap.h"
#include "magick/colorspace.h"
#include "magick/constitute.h"
#include "magick/decorate.h"
#include "magick/distort.h"
#include "magick/draw.h"
#include "magick/enhance.h"
#include "magick/exception.h"
#include "magick/exception-private.h"
#include "magick/effect.h"
#include "magick/gem.h"
#include "magick/geometry.h"
#include "magick/image-private.h"
#include "magick/list.h"
#include "magick/log.h"
#include "magick/matrix.h"
#include "magick/memory_.h"
#include "magick/memory-private.h"
#include "magick/monitor.h"
#include "magick/monitor-private.h"
#include "magick/montage.h"
#include "magick/morphology.h"
#include "magick/morphology-private.h"
#include "magick/opencl-private.h"
#include "magick/paint.h"
#include "magick/pixel-accessor.h"
#include "magick/pixel-private.h"
#include "magick/property.h"
#include "magick/quantum.h"
#include "magick/resource_.h"
#include "magick/signature-private.h"
#include "magick/string_.h"
#include "magick/string-private.h"
#include "magick/thread-private.h"
#include "magick/token.h"
#include "magick/vision.h"

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%     C o n n e c t e d C o m p o n e n t s I m a g e                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConnectedComponentsImage() returns the connected-components of the image
%  uniquely labeled.  Choose from 4 or 8-way connectivity.
%
%  The format of the ConnectedComponentsImage method is:
%
%      Image *ConnectedComponentsImage(const Image *image,
%        const size_t connectivity,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image: the image.
%
%    o connectivity: how many neighbors to visit, choose from 4 or 8.
%
%    o exception: return any errors or warnings in this structure.
%
*/

typedef struct _CCObjectInfo
{
  ssize_t
    id;

  RectangleInfo
    bounding_box;

  MagickPixelPacket
    color;

  PointInfo
    centroid;

  double
    area,
    census;

  MagickBooleanType
    merge;
} CCObjectInfo;

static int CCObjectInfoCompare(const void *x,const void *y)
{
  CCObjectInfo
    *p,
    *q;

  p=(CCObjectInfo *) x;
  q=(CCObjectInfo *) y;
  return((int) (q->area-(ssize_t) p->area));
}

MagickExport Image *ConnectedComponentsImage(const Image *image,
  const size_t connectivity,ExceptionInfo *exception)
{
#define ConnectedComponentsImageTag  "ConnectedComponents/Image"

  CacheView
    *component_view,
    *image_view,
    *object_view;

  CCObjectInfo
    *object;

  char
    *c;

  const char
    *artifact;

  double
    max_threshold,
    min_threshold;

  Image
    *component_image;

  MagickBooleanType
    status;

  MagickOffsetType
    progress;

  MatrixInfo
    *equivalences;

  ssize_t
    i;

  size_t
    size;

  ssize_t
    background_id,
    connect4[2][2] = { { -1,  0 }, {  0, -1 } },
    connect8[4][2] = { { -1, -1 }, { -1,  0 }, { -1,  1 }, {  0, -1 } },
    dx,
    dy,
    first,
    last,
    n,
    step,
    y;

  /*
    Initialize connected components image attributes.
  */
  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  assert(exception != (ExceptionInfo *) NULL);
  assert(exception->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  component_image=CloneImage(image,0,0,MagickTrue,exception);
  if (component_image == (Image *) NULL)
    return((Image *) NULL);
  component_image->depth=MAGICKCORE_QUANTUM_DEPTH;
  if (AcquireImageColormap(component_image,MaxColormapSize) == MagickFalse)
    {
      component_image=DestroyImage(component_image);
      ThrowImageException(ResourceLimitError,"MemoryAllocationFailed");
    }
  /*
    Initialize connected components equivalences.
  */
  size=image->columns*image->rows;
  if (image->columns != (size/image->rows))
    {
      component_image=DestroyImage(component_image);
      ThrowImageException(ResourceLimitError,"MemoryAllocationFailed");
    }
  equivalences=AcquireMatrixInfo(size,1,sizeof(ssize_t),exception);
  if (equivalences == (MatrixInfo *) NULL)
    {
      component_image=DestroyImage(component_image);
      return((Image *) NULL);
    }
  for (n=0; n < (ssize_t) (image->columns*image->rows); n++)
    (void) SetMatrixElement(equivalences,n,0,&n);
  object=(CCObjectInfo *) AcquireQuantumMemory(MaxColormapSize,sizeof(*object));
  if (object == (CCObjectInfo *) NULL)
    {
      equivalences=DestroyMatrixInfo(equivalences);
      component_image=DestroyImage(component_image);
      ThrowImageException(ResourceLimitError,"MemoryAllocationFailed");
    }
  (void) memset(object,0,MaxColormapSize*sizeof(*object));
  for (i=0; i < (ssize_t) MaxColormapSize; i++)
  {
    object[i].id=i;
    object[i].bounding_box.x=(ssize_t) image->columns;
    object[i].bounding_box.y=(ssize_t) image->rows;
    GetMagickPixelPacket(image,&object[i].color);
  }
  /*
    Find connected components.
  */
  status=MagickTrue;
  progress=0;
  image_view=AcquireVirtualCacheView(image,exception);
  for (n=0; n < (ssize_t) (connectivity > 4 ? 4 : 2); n++)
  {
    if (status == MagickFalse)
      continue;
    dx=connectivity > 4 ? connect8[n][1] : connect4[n][1];
    dy=connectivity > 4 ? connect8[n][0] : connect4[n][0];
    for (y=0; y < (ssize_t) image->rows; y++)
    {
      const PixelPacket
        *magick_restrict p;

      ssize_t
        x;

      if (status == MagickFalse)
        continue;
      p=GetCacheViewVirtualPixels(image_view,0,y-1,image->columns,3,exception);
      if (p == (const PixelPacket *) NULL)
        {
          status=MagickFalse;
          continue;
        }
      p+=(ptrdiff_t) image->columns;
      for (x=0; x < (ssize_t) image->columns; x++)
      {
        ssize_t
          neighbor_offset,
          obj,
          offset,
          ox,
          oy,
          root;

        /*
          Is neighbor an authentic pixel and a different color than the pixel?
        */
        if (((x+dx) < 0) || ((x+dx) >= (ssize_t) image->columns) ||
            ((y+dy) < 0) || ((y+dy) >= (ssize_t) image->rows))
          {
            p++;
            continue;
          }
        neighbor_offset=dy*image->columns+dx;
        if (IsColorSimilar(image,p,p+neighbor_offset) == MagickFalse)
          {
            p++;
            continue;
          }
        /*
          Resolve this equivalence.
        */
        offset=y*image->columns+x;
        ox=offset;
        status=GetMatrixElement(equivalences,ox,0,&obj);
        while (obj != ox)
        {
          ox=obj;
          status=GetMatrixElement(equivalences,ox,0,&obj);
        }
        oy=offset+neighbor_offset;
        status=GetMatrixElement(equivalences,oy,0,&obj);
        while (obj != oy)
        {
          oy=obj;
          status=GetMatrixElement(equivalences,oy,0,&obj);
        }
        if (ox < oy)
          {
            status=SetMatrixElement(equivalences,oy,0,&ox);
            root=ox;
          }
        else
          {
            status=SetMatrixElement(equivalences,ox,0,&oy);
            root=oy;
          }
        ox=offset;
        status=GetMatrixElement(equivalences,ox,0,&obj);
        while (obj != root)
        {
          status=GetMatrixElement(equivalences,ox,0,&obj);
          status=SetMatrixElement(equivalences,ox,0,&root);
        }
        oy=offset+neighbor_offset;
        status=GetMatrixElement(equivalences,oy,0,&obj);
        while (obj != root)
        {
          status=GetMatrixElement(equivalences,oy,0,&obj);
          status=SetMatrixElement(equivalences,oy,0,&root);
        }
        status=SetMatrixElement(equivalences,y*image->columns+x,0,&root);
        p++;
      }
    }
  }
  /*
    Label connected components.
  */
  n=0;
  component_view=AcquireAuthenticCacheView(component_image,exception);
  for (y=0; y < (ssize_t) component_image->rows; y++)
  {
    const IndexPacket
      *magick_restrict indexes;

    const PixelPacket
      *magick_restrict p;

    IndexPacket
      *magick_restrict component_indexes;

    PixelPacket
      *magick_restrict q;

    ssize_t
      x;

    if (status == MagickFalse)
      continue;
    p=GetCacheViewVirtualPixels(image_view,0,y,image->columns,1,exception);
    q=QueueCacheViewAuthenticPixels(component_view,0,y,component_image->columns,
      1,exception);
    if ((p == (const PixelPacket *) NULL) || (q == (PixelPacket *) NULL))
      {
        status=MagickFalse;
        continue;
      }
    indexes=GetCacheViewVirtualIndexQueue(image_view);
    component_indexes=GetCacheViewAuthenticIndexQueue(component_view);
    for (x=0; x < (ssize_t) component_image->columns; x++)
    {
      ssize_t
        id,
        offset;

      offset=y*image->columns+x;
      status=GetMatrixElement(equivalences,offset,0,&id);
      if (id != offset)
        status=GetMatrixElement(equivalences,id,0,&id);
      else
        {
          id=n++;
          if (id >= (ssize_t) MaxColormapSize)
            break;
        }
      status=SetMatrixElement(equivalences,offset,0,&id);
      if (x < object[id].bounding_box.x)
        object[id].bounding_box.x=x;
      if (x >= (ssize_t) object[id].bounding_box.width)
        object[id].bounding_box.width=(size_t) x;
      if (y < object[id].bounding_box.y)
        object[id].bounding_box.y=y;
      if (y >= (ssize_t) object[id].bounding_box.height)
        object[id].bounding_box.height=(size_t) y;
      object[id].color.red+=QuantumScale*(MagickRealType) p->red;
      object[id].color.green+=QuantumScale*(MagickRealType) p->green;
      object[id].color.blue+=QuantumScale*(MagickRealType) p->blue;
      if (image->matte != MagickFalse)
        object[id].color.opacity+=QuantumScale*(MagickRealType) p->opacity;
      if (image->colorspace == CMYKColorspace)
        object[id].color.index+=QuantumScale*(MagickRealType) indexes[x];
      object[id].centroid.x+=x;
      object[id].centroid.y+=y;
      object[id].area++;
      component_indexes[x]=(IndexPacket) id;
      p++;
      q++;
    }
    if (n > (ssize_t) MaxColormapSize)
      break;
    if (SyncCacheViewAuthenticPixels(component_view,exception) == MagickFalse)
      status=MagickFalse;
    if (image->progress_monitor != (MagickProgressMonitor) NULL)
      {
        MagickBooleanType
          proceed;

        progress++;
        proceed=SetImageProgress(image,ConnectedComponentsImageTag,progress,
          image->rows);
        if (proceed == MagickFalse)
          status=MagickFalse;
      }
  }
  component_view=DestroyCacheView(component_view);
  image_view=DestroyCacheView(image_view);
  equivalences=DestroyMatrixInfo(equivalences);
  if (n > (ssize_t) MaxColormapSize)
    {
      object=(CCObjectInfo *) RelinquishMagickMemory(object);
      component_image=DestroyImage(component_image);
      ThrowImageException(ResourceLimitError,"TooManyObjects");
    }
  background_id=0;
  min_threshold=0.0;
  max_threshold=0.0;
  component_image->colors=(size_t) n;
  for (i=0; i < (ssize_t) component_image->colors; i++)
  {
    object[i].bounding_box.width-=(object[i].bounding_box.x-1);
    object[i].bounding_box.height-=(object[i].bounding_box.y-1);
    object[i].color.red/=(QuantumScale*object[i].area);
    object[i].color.green/=(QuantumScale*object[i].area);
    object[i].color.blue/=(QuantumScale*object[i].area);
    if (image->matte != MagickFalse)
      object[i].color.opacity/=(QuantumScale*object[i].area);
    if (image->colorspace == CMYKColorspace)
      object[i].color.index/=(QuantumScale*object[i].area);
    object[i].centroid.x/=object[i].area;
    object[i].centroid.y/=object[i].area;
    max_threshold+=object[i].area;
    if (object[i].area > object[background_id].area)
      background_id=i;
  }
  max_threshold+=MagickEpsilon;
  artifact=GetImageArtifact(image,"connected-components:background-id");
  if (artifact != (const char *) NULL)
    background_id=(ssize_t) StringToLong(artifact);
  artifact=GetImageArtifact(image,"connected-components:area-threshold");
  if (artifact != (const char *) NULL)
    {
      /*
        Merge any object not within the min and max area threshold.
      */
      (void) sscanf(artifact,"%lf%*[ -]%lf",&min_threshold,&max_threshold);
      for (i=0; i < (ssize_t) component_image->colors; i++)
        if (((object[i].area < min_threshold) ||
             (object[i].area >= max_threshold)) && (i != background_id))
          object[i].merge=MagickTrue;
    }
  artifact=GetImageArtifact(image,"connected-components:keep-colors");
  if (artifact != (const char *) NULL)
    {
      const char
        *p;

      /*
        Keep selected objects based on color, merge others.
      */
      for (i=0; i < (ssize_t) component_image->colors; i++)
        object[i].merge=MagickTrue;
      for (p=artifact;  ; )
      {
        char
          color[MagickPathExtent];

        MagickPixelPacket
          pixel;

        const char
          *q;

        for (q=p; *q != '\0'; q++)
          if (*q == ';')
            break;
        (void) CopyMagickString(color,p,(size_t) MagickMin(q-p+1,
          MagickPathExtent));
        (void) QueryMagickColor(color,&pixel,exception);
        for (i=0; i < (ssize_t) component_image->colors; i++)
          if (IsMagickColorSimilar(&object[i].color,&pixel) != MagickFalse)
            object[i].merge=MagickFalse;
        if (*q == '\0')
          break;
        p=q+1;
      }
    }
  artifact=GetImageArtifact(image,"connected-components:keep-ids");
  if (artifact == (const char *) NULL)
    artifact=GetImageArtifact(image,"connected-components:keep");
  if (artifact != (const char *) NULL)
    for (c=(char *) artifact; *c != '\0'; )
    {
      /*
        Keep selected objects based on id, merge others.
      */
      for (i=0; i < (ssize_t) component_image->colors; i++)
        object[i].merge=MagickTrue;
      while ((isspace((int) ((unsigned char) *c)) != 0) || (*c == ','))
        c++;
      first=(ssize_t) strtol(c,&c,10);
      if (first < 0)
        first+=(ssize_t) component_image->colors;
      last=first;
      while (isspace((int) ((unsigned char) *c)) != 0)
        c++;
      if (*c == '-')
        {
          last=(ssize_t) strtol(c+1,&c,10);
          if (last < 0)
            last+=(ssize_t) component_image->colors;
        }
      step=(ssize_t) (first > last ? -1 : 1);
      for ( ; first != (last+step); first+=step)
        object[first].merge=MagickFalse;
    }
  artifact=GetImageArtifact(image,"connected-components:keep-top");
  if (artifact != (const char *) NULL)
    {
      CCObjectInfo
        *top_objects;

      ssize_t
        top_ids;

      /*
        Keep top objects.
      */
      top_ids=(ssize_t) StringToLong(artifact);
      top_objects=(CCObjectInfo *) AcquireQuantumMemory(component_image->colors,
        sizeof(*top_objects));
      if (top_objects == (CCObjectInfo *) NULL)
        {
          object=(CCObjectInfo *) RelinquishMagickMemory(object);
          component_image=DestroyImage(component_image);
          ThrowImageException(ResourceLimitError,"MemoryAllocationFailed");
        }
      (void) memcpy(top_objects,object,component_image->colors*sizeof(*object));
      qsort((void *) top_objects,component_image->colors,sizeof(*top_objects),
        CCObjectInfoCompare);
      for (i=top_ids+1; i < (ssize_t) component_image->colors; i++)
        object[top_objects[i].id].merge=MagickTrue;
      top_objects=(CCObjectInfo *) RelinquishMagickMemory(top_objects);
    }
  artifact=GetImageArtifact(image,"connected-components:remove-colors");
  if (artifact != (const char *) NULL)
    {
      const char
        *p;

      /*
        Remove selected objects based on color, keep others.
      */
      for (p=artifact;  ; )
      {
        char
          color[MagickPathExtent];

        MagickPixelPacket
          pixel;

        const char
          *q;

        for (q=p; *q != '\0'; q++)
          if (*q == ';')
            break;
        (void) CopyMagickString(color,p,(size_t) MagickMin(q-p+1,
          MagickPathExtent));
        (void) QueryMagickColor(color,&pixel,exception);
        for (i=0; i < (ssize_t) component_image->colors; i++)
          if (IsMagickColorSimilar(&object[i].color,&pixel) != MagickFalse)
            object[i].merge=MagickTrue;
        if (*q == '\0')
          break;
        p=q+1;
      }
    }
  artifact=GetImageArtifact(image,"connected-components:remove-ids");
  if (artifact == (const char *) NULL)
    artifact=GetImageArtifact(image,"connected-components:remove");
  if (artifact != (const char *) NULL)
    for (c=(char *) artifact; *c != '\0'; )
    {
      /*
        Remove selected objects based on color, keep others.
      */
      while ((isspace((int) ((unsigned char) *c)) != 0) || (*c == ','))
        c++;
      first=(ssize_t) strtol(c,&c,10);
      if (first < 0)
        first+=(ssize_t) component_image->colors;
      last=first;
      while (isspace((int) ((unsigned char) *c)) != 0)
        c++;
      if (*c == '-')
        {
          last=(ssize_t) strtol(c+1,&c,10);
          if (last < 0)
            last+=(ssize_t) component_image->colors;
        }
      step=(ssize_t) (first > last ? -1 : 1);
      for ( ; first != (last+step); first+=step)
        object[first].merge=MagickTrue;
    }
  /*
    Merge any object not within the min and max area threshold.
  */
  component_view=AcquireAuthenticCacheView(component_image,exception);
  object_view=AcquireVirtualCacheView(component_image,exception);
  for (i=0; i < (ssize_t) component_image->colors; i++)
  {
    RectangleInfo
      bounding_box;

    ssize_t
      j;

    size_t
      id;

    if (status == MagickFalse)
      continue;
    if ((object[i].merge == MagickFalse) || (i == background_id))
      continue;  /* keep object */
    /*
      Merge this object.
    */
    for (j=0; j < (ssize_t) component_image->colors; j++)
      object[j].census=0;
    bounding_box=object[i].bounding_box;
    for (y=0; y < (ssize_t) bounding_box.height; y++)
    {
      const IndexPacket
        *magick_restrict indexes;

      const PixelPacket
        *magick_restrict p;

      ssize_t
        x;

      if (status == MagickFalse)
        continue;
      p=GetCacheViewVirtualPixels(component_view,bounding_box.x,
        bounding_box.y+y,bounding_box.width,1,exception);
      if (p == (const PixelPacket *) NULL)
        {
          status=MagickFalse;
          continue;
        }
      indexes=GetCacheViewVirtualIndexQueue(component_view);
      for (x=0; x < (ssize_t) bounding_box.width; x++)
      {
        size_t
          k;

        if (status == MagickFalse)
          continue;
        j=(ssize_t) indexes[x];
        if (j == i)
          for (k=0; k < (ssize_t) (connectivity > 4 ? 4 : 2); k++)
          {
            const IndexPacket
              *magick_restrict indexes;

            const PixelPacket
              *p;

            /*
              Compute area of adjacent objects.
            */
            if (status == MagickFalse)
              continue;
            dx=connectivity > 4 ? connect8[k][1] : connect4[k][1];
            dy=connectivity > 4 ? connect8[k][0] : connect4[k][0];
            p=GetCacheViewVirtualPixels(object_view,bounding_box.x+x+dx,
              bounding_box.y+y+dy,1,1,exception);
            if (p == (const PixelPacket *) NULL)
              {
                status=MagickFalse;
                break;
              }
            indexes=GetCacheViewVirtualIndexQueue(object_view);
            j=(ssize_t) *indexes;
            if (j != i)
              object[j].census++;
          }
      }
    }
    /*
      Merge with object of greatest adjacent area.
    */
    id=0;
    for (j=1; j < (ssize_t) component_image->colors; j++)
      if (object[j].census > object[id].census)
        id=(size_t) j;
    object[i].area=0.0;
    for (y=0; y < (ssize_t) bounding_box.height; y++)
    {
      IndexPacket
        *magick_restrict component_indexes;

      PixelPacket
        *magick_restrict q;

      ssize_t
        x;

      if (status == MagickFalse)
        continue;
      q=GetCacheViewAuthenticPixels(component_view,bounding_box.x,
        bounding_box.y+y,bounding_box.width,1,exception);
      if (q == (PixelPacket *) NULL)
        {
          status=MagickFalse;
          continue;
        }
      component_indexes=GetCacheViewAuthenticIndexQueue(component_view);
      for (x=0; x < (ssize_t) bounding_box.width; x++)
      {
        if ((ssize_t) component_indexes[x] == i)
          component_indexes[x]=(IndexPacket) id;
      }
      if (SyncCacheViewAuthenticPixels(component_view,exception) == MagickFalse)
        status=MagickFalse;
    }
  }
  object_view=DestroyCacheView(object_view);
  component_view=DestroyCacheView(component_view);
  artifact=GetImageArtifact(image,"connected-components:mean-color");
  if (IsMagickTrue(artifact) != MagickFalse)
    {
      /*
        Replace object with mean color.
      */
      for (i=0; i < (ssize_t) component_image->colors; i++)
      {
        component_image->colormap[i].red=ClampToQuantum(object[i].color.red);
        component_image->colormap[i].green=ClampToQuantum(
          object[i].color.green);
        component_image->colormap[i].blue=ClampToQuantum(object[i].color.blue);
        component_image->colormap[i].opacity=ClampToQuantum(
          object[i].color.opacity);
      }
    }
  (void) SyncImage(component_image);
  artifact=GetImageArtifact(image,"connected-components:verbose");
  if (IsMagickTrue(artifact) != MagickFalse)
    {
      /*
        Report statistics on each unique objects.
      */
      for (i=0; i < (ssize_t) component_image->colors; i++)
      {
        object[i].bounding_box.width=0;
        object[i].bounding_box.height=0;
        object[i].bounding_box.x=(ssize_t) component_image->columns;
        object[i].bounding_box.y=(ssize_t) component_image->rows;
        object[i].centroid.x=0;
        object[i].centroid.y=0;
        object[i].census=object[i].area == 0.0 ? 0.0 : 1.0;
        object[i].area=0;
      }
      component_view=AcquireVirtualCacheView(component_image,exception);
      for (y=0; y < (ssize_t) component_image->rows; y++)
      {
        const IndexPacket
          *indexes;

        const PixelPacket
          *magick_restrict p;

        ssize_t
          x;

        if (status == MagickFalse)
          continue;
        p=GetCacheViewVirtualPixels(component_view,0,y,
          component_image->columns,1,exception);
        if (p == (const PixelPacket *) NULL)
          {
            status=MagickFalse;
            continue;
          }
        indexes=GetCacheViewVirtualIndexQueue(component_view);
        for (x=0; x < (ssize_t) component_image->columns; x++)
        {
          size_t
            id;

          id=(size_t) indexes[x];
          if (x < object[id].bounding_box.x)
            object[id].bounding_box.x=x;
          if (x > (ssize_t) object[id].bounding_box.width)
            object[id].bounding_box.width=(size_t) x;
          if (y < object[id].bounding_box.y)
            object[id].bounding_box.y=y;
          if (y > (ssize_t) object[id].bounding_box.height)
            object[id].bounding_box.height=(size_t) y;
          object[id].centroid.x+=x;
          object[id].centroid.y+=y;
          object[id].area++;
        }
      }
      for (i=0; i < (ssize_t) component_image->colors; i++)
      {
        object[i].bounding_box.width-=(object[i].bounding_box.x-1);
        object[i].bounding_box.height-=(object[i].bounding_box.y-1);
        object[i].centroid.x=object[i].centroid.x/object[i].area;
        object[i].centroid.y=object[i].centroid.y/object[i].area;
      }
      component_view=DestroyCacheView(component_view);
      qsort((void *) object,component_image->colors,sizeof(*object),
        CCObjectInfoCompare);
      artifact=GetImageArtifact(image,"connected-components:exclude-header");
      if (IsStringTrue(artifact) == MagickFalse)
        (void) fprintf(stdout,
          "Objects (id: bounding-box centroid area mean-color):\n");
      for (i=0; i < (ssize_t) component_image->colors; i++)
        if (object[i].census > 0.0)
          {
            char
              mean_color[MaxTextExtent];

            GetColorTuple(&object[i].color,MagickFalse,mean_color);
            (void) fprintf(stdout,
              "  %.20g: %.20gx%.20g%+.20g%+.20g %.1f,%.1f %.20g %s\n",(double)
              object[i].id,(double) object[i].bounding_box.width,(double)
              object[i].bounding_box.height,(double) object[i].bounding_box.x,
              (double) object[i].bounding_box.y,object[i].centroid.x,
              object[i].centroid.y,(double) object[i].area,mean_color);
          }
    }
  object=(CCObjectInfo *) RelinquishMagickMemory(object);
  return(component_image);
}
