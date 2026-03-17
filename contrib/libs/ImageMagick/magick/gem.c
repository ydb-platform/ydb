/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%                              GGGG  EEEEE  M   M                             %
%                             G      E      MM MM                             %
%                             G GG   EEE    M M M                             %
%                             G   G  E      M   M                             %
%                              GGGG  EEEEE  M   M                             %
%                                                                             %
%                                                                             %
%                    Graphic Gems - Graphic Support Methods                   %
%                                                                             %
%                               Software Design                               %
%                                    Cristy                                   %
%                                 August 1996                                 %
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
#include "magick/color-private.h"
#include "magick/draw.h"
#include "magick/gem.h"
#include "magick/gem-private.h"
#include "magick/image.h"
#include "magick/image-private.h"
#include "magick/log.h"
#include "magick/memory_.h"
#include "magick/pixel-private.h"
#include "magick/quantum.h"
#include "magick/random_.h"
#include "magick/resize.h"
#include "magick/transform.h"
#include "magick/signature-private.h"

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t H C L T o R G B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertHCLToRGB() transforms a (hue, chroma, luma) to a (red, green,
%  blue) triple.
%
%  The format of the ConvertHCLToRGBImage method is:
%
%      void ConvertHCLToRGB(const double hue,const double chroma,
%        const double luma,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o hue, chroma, luma: A double value representing a component of the
%      HCL color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/
MagickExport void ConvertHCLToRGB(const double hue,const double chroma,
  const double luma,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    b,
    c,
    g,
    h,
    m,
    r,
    x;

  /*
    Convert HCL to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  h=6.0*hue;
  c=chroma;
  x=c*(1.0-fabs(fmod(h,2.0)-1.0));
  r=0.0;
  g=0.0;
  b=0.0;
  if ((0.0 <= h) && (h < 1.0))
    {
      r=c;
      g=x;
    }
  else
    if ((1.0 <= h) && (h < 2.0))
      {
        r=x;
        g=c;
      }
    else
      if ((2.0 <= h) && (h < 3.0))
        {
          g=c;
          b=x;
        }
      else
        if ((3.0 <= h) && (h < 4.0))
          {
            g=x;
            b=c;
          }
        else
          if ((4.0 <= h) && (h < 5.0))
            {
              r=x;
              b=c;
            }
          else
            if ((5.0 <= h) && (h < 6.0))
              {
                r=c;
                b=x;
              }
  m=luma-(0.298839*r+0.586811*g+0.114350*b);
  *red=ClampToQuantum((MagickRealType) QuantumRange*(r+m));
  *green=ClampToQuantum((MagickRealType) QuantumRange*(g+m));
  *blue=ClampToQuantum((MagickRealType) QuantumRange*(b+m));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t H C L p T o R G B                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertHCLpToRGB() transforms a (hue, chroma, luma) to a (red, green,
%  blue) triple.  Since HCL colorspace is wider than RGB, we instead choose a
%  saturation strategy to project it on the RGB cube.
%
%  The format of the ConvertHCLpToRGBImage method is:
%
%      void ConvertHCLpToRGB(const double hue,const double chroma,
%        const double luma,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o hue, chroma, luma: A double value representing a component of the
%      HCLp color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/
MagickExport void ConvertHCLpToRGB(const double hue,const double chroma,
  const double luma,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    b,
    c,
    g,
    h,
    m,
    r,
    x,
    z;

  /*
    Convert HCLp to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  h=6.0*hue;
  c=chroma;
  x=c*(1.0-fabs(fmod(h,2.0)-1.0));
  r=0.0;
  g=0.0;
  b=0.0;
  if ((0.0 <= h) && (h < 1.0))
    {
      r=c;
      g=x;
    }
  else
    if ((1.0 <= h) && (h < 2.0))
      {
        r=x;
        g=c;
      }
    else
      if ((2.0 <= h) && (h < 3.0))
        {
          g=c;
          b=x;
        }
      else
        if ((3.0 <= h) && (h < 4.0))
          {
            g=x;
            b=c;
          }
        else
          if ((4.0 <= h) && (h < 5.0))
            {
              r=x;
              b=c;
            }
          else
            if ((5.0 <= h) && (h < 6.0))
              {
                r=c;
                b=x;
              }
  m=luma-(0.298839*r+0.586811*g+0.114350*b);
  z=1.0;
  if (m < 0.0)
    {
      z=luma/(luma-m);
      m=0.0;
    }
  else
    if (m+c > 1.0)
      {
        z=(1.0-luma)/(m+c-luma);
        m=1.0-z*c;
      }
  *red=ClampToQuantum((MagickRealType) QuantumRange*(z*r+m));
  *green=ClampToQuantum((MagickRealType) QuantumRange*(z*g+m));
  *blue=ClampToQuantum((MagickRealType) QuantumRange*(z*b+m));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t H S B T o R G B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertHSBToRGB() transforms a (hue, saturation, brightness) to a (red,
%  green, blue) triple.
%
%  The format of the ConvertHSBToRGBImage method is:
%
%      void ConvertHSBToRGB(const double hue,const double saturation,
%        const double brightness,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o hue, saturation, brightness: A double value representing a
%      component of the HSB color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/
MagickExport void ConvertHSBToRGB(const double hue,const double saturation,
  const double brightness,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    f,
    h,
    p,
    q,
    t;

  /*
    Convert HSB to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  if (fabs(saturation) < MagickEpsilon)
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*brightness);
      *green=(*red);
      *blue=(*red);
      return;
    }
  h=6.0*(hue-floor(hue));
  f=h-floor((double) h);
  p=brightness*(1.0-saturation);
  q=brightness*(1.0-saturation*f);
  t=brightness*(1.0-(saturation*(1.0-f)));
  switch ((int) h)
  {
    case 0:
    default:
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*brightness);
      *green=ClampToQuantum((MagickRealType) QuantumRange*t);
      *blue=ClampToQuantum((MagickRealType) QuantumRange*p);
      break;
    }
    case 1:
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*q);
      *green=ClampToQuantum((MagickRealType) QuantumRange*brightness);
      *blue=ClampToQuantum((MagickRealType) QuantumRange*p);
      break;
    }
    case 2:
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*p);
      *green=ClampToQuantum((MagickRealType) QuantumRange*brightness);
      *blue=ClampToQuantum((MagickRealType) QuantumRange*t);
      break;
    }
    case 3:
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*p);
      *green=ClampToQuantum((MagickRealType) QuantumRange*q);
      *blue=ClampToQuantum((MagickRealType) QuantumRange*brightness);
      break;
    }
    case 4:
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*t);
      *green=ClampToQuantum((MagickRealType) QuantumRange*p);
      *blue=ClampToQuantum((MagickRealType) QuantumRange*brightness);
      break;
    }
    case 5:
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*brightness);
      *green=ClampToQuantum((MagickRealType) QuantumRange*p);
      *blue=ClampToQuantum((MagickRealType) QuantumRange*q);
      break;
    }
  }
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t H S I T o R G B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertHSIToRGB() transforms a (hue, saturation, intensity) to a (red,
%  green, blue) triple.
%
%  The format of the ConvertHSIToRGBImage method is:
%
%      void ConvertHSIToRGB(const double hue,const double saturation,
%        const double intensity,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o hue, saturation, intensity: A double value representing a
%      component of the HSI color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/
MagickExport void ConvertHSIToRGB(const double hue,const double saturation,
  const double intensity,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    b,
    g,
    h,
    r;

  /*
    Convert HSI to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  h=360.0*hue;
  h-=360.0*floor(h/360.0);
  if (h < 120.0)
    {
      b=intensity*(1.0-saturation);
      r=intensity*(1.0+saturation*cos(h*(MagickPI/180.0))/cos((60.0-h)*
        (MagickPI/180.0)));
      g=3.0*intensity-r-b;
    }
  else
    if (h < 240.0)
      {
        h-=120.0;
        r=intensity*(1.0-saturation);
        g=intensity*(1.0+saturation*cos(h*(MagickPI/180.0))/cos((60.0-h)*
          (MagickPI/180.0)));
        b=3.0*intensity-r-g;
      }
    else
      {
        h-=240.0;
        g=intensity*(1.0-saturation);
        b=intensity*(1.0+saturation*cos(h*(MagickPI/180.0))/cos((60.0-h)*
          (MagickPI/180.0)));
        r=3.0*intensity-g-b;
      }
  *red=ClampToQuantum((MagickRealType) QuantumRange*r);
  *green=ClampToQuantum((MagickRealType) QuantumRange*g);
  *blue=ClampToQuantum((MagickRealType) QuantumRange*b);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t H S L T o R G B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertHSLToRGB() transforms a (hue, saturation, lightness) to a (red,
%  green, blue) triple.
%
%  The format of the ConvertHSLToRGBImage method is:
%
%      void ConvertHSLToRGB(const double hue,const double saturation,
%        const double lightness,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o hue, saturation, lightness: A double value representing a
%      component of the HSL color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/
MagickExport void ConvertHSLToRGB(const double hue,const double saturation,
  const double lightness,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    b,
    c,
    g,
    h,
    min,
    r,
    x;

  /*
    Convert HSL to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  h=hue*360.0;
  if (lightness <= 0.5)
    c=2.0*lightness*saturation;
  else
    c=(2.0-2.0*lightness)*saturation;
  min=lightness-0.5*c;
  h-=360.0*floor(h/360.0);
  h/=60.0;
  x=c*(1.0-fabs(h-2.0*floor(h/2.0)-1.0));
  switch ((int) floor(h))
  {
    case 0:
    default:
    {
      r=min+c;
      g=min+x;
      b=min;
      break;
    }
    case 1:
    {
      r=min+x;
      g=min+c;
      b=min;
      break;
    }
    case 2:
    {
      r=min;
      g=min+c;
      b=min+x;
      break;
    }
    case 3:
    {
      r=min;
      g=min+x;
      b=min+c;
      break;
    }
    case 4:
    {
      r=min+x;
      g=min;
      b=min+c;
      break;
    }
    case 5:
    {
      r=min+c;
      g=min;
      b=min+x;
      break;
    }
  }
  *red=ClampToQuantum((MagickRealType) QuantumRange*r);
  *green=ClampToQuantum((MagickRealType) QuantumRange*g);
  *blue=ClampToQuantum((MagickRealType) QuantumRange*b);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t H S V T o R G B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertHSVToRGB() transforms a (hue, saturation, value) to a (red,
%  green, blue) triple.
%
%  The format of the ConvertHSVToRGBImage method is:
%
%      void ConvertHSVToRGB(const double hue,const double saturation,
%        const double value,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o hue, saturation, value: A double value representing a
%      component of the HSV color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/
MagickExport void ConvertHSVToRGB(const double hue,const double saturation,
  const double value,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    b,
    c,
    g,
    h,
    min,
    r,
    x;

  /*
    Convert HSV to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  h=hue*360.0;
  c=value*saturation;
  min=value-c;
  h-=360.0*floor(h/360.0);
  h/=60.0;
  x=c*(1.0-fabs(h-2.0*floor(h/2.0)-1.0));
  switch ((int) floor(h))
  {
    case 0:
    default:
    {
      r=min+c;
      g=min+x;
      b=min;
      break;
    }
    case 1:
    {
      r=min+x;
      g=min+c;
      b=min;
      break;
    }
    case 2:
    {
      r=min;
      g=min+c;
      b=min+x;
      break;
    }
    case 3:
    {
      r=min;
      g=min+x;
      b=min+c;
      break;
    }
    case 4:
    {
      r=min+x;
      g=min;
      b=min+c;
      break;
    }
    case 5:
    {
      r=min+c;
      g=min;
      b=min+x;
      break;
    }
  }
  *red=ClampToQuantum((MagickRealType) QuantumRange*r);
  *green=ClampToQuantum((MagickRealType) QuantumRange*g);
  *blue=ClampToQuantum((MagickRealType) QuantumRange*b);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t H W B T o R G B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertHWBToRGB() transforms a (hue, whiteness, blackness) to a (red, green,
%  blue) triple.
%
%  The format of the ConvertHWBToRGBImage method is:
%
%      void ConvertHWBToRGB(const double hue,const double whiteness,
%        const double blackness,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o hue, whiteness, blackness: A double value representing a
%      component of the HWB color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/
MagickExport void ConvertHWBToRGB(const double hue,const double whiteness,
  const double blackness,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    b,
    f,
    g,
    n,
    r,
    v;

  ssize_t
    i;

  /*
    Convert HWB to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  v=1.0-blackness;
  if (fabs(hue-(-1.0)) < MagickEpsilon)
    {
      *red=ClampToQuantum((MagickRealType) QuantumRange*v);
      *green=ClampToQuantum((MagickRealType) QuantumRange*v);
      *blue=ClampToQuantum((MagickRealType) QuantumRange*v);
      return;
    }
  i=CastDoubleToLong(floor(6.0*hue));
  f=6.0*hue-i;
  if ((i & 0x01) != 0)
    f=1.0-f;
  n=whiteness+f*(v-whiteness);  /* linear interpolation */
  switch (i)
  {
    case 0:
    default: r=v; g=n; b=whiteness; break;
    case 1: r=n; g=v; b=whiteness; break;
    case 2: r=whiteness; g=v; b=n; break;
    case 3: r=whiteness; g=n; b=v; break;
    case 4: r=n; g=whiteness; b=v; break;
    case 5: r=v; g=whiteness; b=n; break;
  }
  *red=ClampToQuantum((MagickRealType) QuantumRange*r);
  *green=ClampToQuantum((MagickRealType) QuantumRange*g);
  *blue=ClampToQuantum((MagickRealType) QuantumRange*b);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t L C H a b T o R G B                                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertLCHabToRGB() transforms a (luma, chroma, hue) to a (red, green,
%  blue) triple.
%
%  The format of the ConvertLCHabToRGBImage method is:
%
%      void ConvertLCHabToRGB(const double luma,const double chroma,
%        const double hue,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o luma, chroma, hue: A double value representing a component of the LCHab
%      color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/

static inline void ConvertLCHabToXYZ(const double luma,const double chroma,
  const double hue,double *X,double *Y,double *Z)
{
  ConvertLabToXYZ(luma,chroma*cos(DegreesToRadians(hue)),chroma*
    sin(DegreesToRadians(hue)),X,Y,Z);
}

MagickExport void ConvertLCHabToRGB(const double luma,const double chroma,
  const double hue,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    X,
    Y,
    Z;

  /*
    Convert LCHab to RGB colorspace.
  */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  ConvertLCHabToXYZ(100.0*luma,255.0*(chroma-0.5),360.0*hue,&X,&Y,&Z);
  ConvertXYZToRGB(X,Y,Z,red,green,blue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t L C H u v T o R G B                                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertLCHuvToRGB() transforms a (luma, chroma, hue) to a (red, green,
%  blue) triple.
%
%  The format of the ConvertLCHuvToRGBImage method is:
%
%      void ConvertLCHuvToRGB(const double luma,const double chroma,
%        const double hue,Quantum *red,Quantum *green,Quantum *blue)
%
%  A description of each parameter follows:
%
%    o luma, chroma, hue: A double value representing a component of the LCHuv
%      color space.
%
%    o red, green, blue: A pointer to a pixel component of type Quantum.
%
*/

static inline void ConvertLCHuvToXYZ(const double luma,const double chroma,
  const double hue,double *X,double *Y,double *Z)
{
  ConvertLuvToXYZ(luma,chroma*cos(DegreesToRadians(hue)),chroma*
    sin(DegreesToRadians(hue)),X,Y,Z);
}

MagickExport void ConvertLCHuvToRGB(const double luma,const double chroma,
  const double hue,Quantum *red,Quantum *green,Quantum *blue)
{
  double
    X,
    Y,
    Z;

  /*
   Convert LCHuv to RGB colorspace.
 */
  assert(red != (Quantum *) NULL);
  assert(green != (Quantum *) NULL);
  assert(blue != (Quantum *) NULL);
  ConvertLCHuvToXYZ(100.0*luma,255.0*(chroma-0.5),360.0*hue,&X,&Y,&Z);
  ConvertXYZToRGB(X,Y,Z,red,green,blue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o H C L                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToHCL() transforms a (red, green, blue) to a (hue, chroma,
%  luma) triple.
%
%  The format of the ConvertRGBToHCL method is:
%
%      void ConvertRGBToHCL(const Quantum red,const Quantum green,
%        const Quantum blue,double *hue,double *chroma,double *luma)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel.
%
%    o hue, chroma, luma: A pointer to a double value representing a
%      component of the HCL color space.
%
*/
MagickExport void ConvertRGBToHCL(const Quantum red,const Quantum green,
  const Quantum blue,double *hue,double *chroma,double *luma)
{
  double
    b,
    c,
    g,
    h,
    max,
    r;

  /*
    Convert RGB to HCL colorspace.
  */
  assert(hue != (double *) NULL);
  assert(chroma != (double *) NULL);
  assert(luma != (double *) NULL);
  r=(double) red;
  g=(double) green;
  b=(double) blue;
  max=MagickMax(r,MagickMax(g,b));
  c=max-(double) MagickMin(r,MagickMin(g,b));
  h=0.0;
  if (fabs(c) < MagickEpsilon)
    h=0.0;
  else
    if (red == (Quantum) max)
      h=fmod((g-b)/c+6.0,6.0);
    else
      if (green == (Quantum) max)
        h=((b-r)/c)+2.0;
      else
        if (blue == (Quantum) max)
          h=((r-g)/c)+4.0;
  *hue=(h/6.0);
  *chroma=QuantumScale*c;
  *luma=QuantumScale*(0.298839*r+0.586811*g+0.114350*b);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o H C L p                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToHCLp() transforms a (red, green, blue) to a (hue, chroma,
%  luma) triple.
%
%  The format of the ConvertRGBToHCLp method is:
%
%      void ConvertRGBToHCLp(const Quantum red,const Quantum green,
%        const Quantum blue,double *hue,double *chroma,double *luma)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel.
%
%    o hue, chroma, luma: A pointer to a double value representing a
%      component of the HCLp color space.
%
*/
MagickExport void ConvertRGBToHCLp(const Quantum red,const Quantum green,
  const Quantum blue,double *hue,double *chroma,double *luma)
{
  double
    b,
    c,
    g,
    h,
    max,
    r;

  /*
    Convert RGB to HCLp colorspace.
  */
  assert(hue != (double *) NULL);
  assert(chroma != (double *) NULL);
  assert(luma != (double *) NULL);
  r=(double) red;
  g=(double) green;
  b=(double) blue;
  max=MagickMax(r,MagickMax(g,b));
  c=max-(double) MagickMin(r,MagickMin(g,b));
  h=0.0;
  if (fabs(c) < MagickEpsilon)
    h=0.0;
  else
    if (red == (Quantum) max)
      h=fmod((g-b)/c+6.0,6.0);
    else
      if (green == (Quantum) max)
        h=((b-r)/c)+2.0;
      else
        if (blue == (Quantum) max)
          h=((r-g)/c)+4.0;
  *hue=(h/6.0);
  *chroma=QuantumScale*c;
  *luma=QuantumScale*(0.298839*r+0.586811*g+0.114350*b);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o H S B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToHSB() transforms a (red, green, blue) to a (hue, saturation,
%  brightness) triple.
%
%  The format of the ConvertRGBToHSB method is:
%
%      void ConvertRGBToHSB(const Quantum red,const Quantum green,
%        const Quantum blue,double *hue,double *saturation,double *brightness)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel..
%
%    o hue, saturation, brightness: A pointer to a double value representing a
%      component of the HSB color space.
%
*/
MagickExport void ConvertRGBToHSB(const Quantum red,const Quantum green,
  const Quantum blue,double *hue,double *saturation,double *brightness)
{
  double
    b,
    delta,
    g,
    max,
    min,
    r;

  /*
    Convert RGB to HSB colorspace.
  */
  assert(hue != (double *) NULL);
  assert(saturation != (double *) NULL);
  assert(brightness != (double *) NULL);
  *hue=0.0;
  *saturation=0.0;
  *brightness=0.0;
  r=(double) red;
  g=(double) green;
  b=(double) blue;
  min=r < g ? r : g;
  if (b < min)
    min=b;
  max=r > g ? r : g;
  if (b > max)
    max=b;
  if (fabs(max) < MagickEpsilon)
    return;
  delta=max-min;
  *saturation=delta/max;
  *brightness=QuantumScale*max;
  if (fabs(delta) < MagickEpsilon)
    return;
  if (fabs(r-max) < MagickEpsilon)
    *hue=(g-b)/delta;
  else
    if (fabs(g-max) < MagickEpsilon)
      *hue=2.0+(b-r)/delta;
    else
      *hue=4.0+(r-g)/delta;
  *hue/=6.0;
  if (*hue < 0.0)
    *hue+=1.0;
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o H S I                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToHSI() transforms a (red, green, blue) to a (hue, saturation,
%  intensity) triple.
%
%  The format of the ConvertRGBToHSI method is:
%
%      void ConvertRGBToHSI(const Quantum red,const Quantum green,
%        const Quantum blue,double *hue,double *saturation,double *intensity)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel..
%
%    o hue, saturation, intensity: A pointer to a double value representing a
%      component of the HSI color space.
%
*/
MagickExport void ConvertRGBToHSI(const Quantum red,const Quantum green,
  const Quantum blue,double *hue,double *saturation,double *intensity)
{
  double
    alpha,
    beta;

  /*
    Convert RGB to HSI colorspace.
  */
  assert(hue != (double *) NULL);
  assert(saturation != (double *) NULL);
  assert(intensity != (double *) NULL);
  *intensity=(QuantumScale*(double) red+QuantumScale*(double) green+
    QuantumScale*(double) blue)/3.0;
  if (*intensity <= 0.0)
    {
      *hue=0.0;
      *saturation=0.0;
      return;
    }
  *saturation=1.0-MagickMin(QuantumScale*(double) red,MagickMin(QuantumScale*
    (double) green,QuantumScale*(double) blue))/(*intensity);
  alpha=0.5*(2.0*QuantumScale*(double) red-QuantumScale*(double) green-
    QuantumScale*(double) blue);
  beta=0.8660254037844385*(QuantumScale*(double) green-QuantumScale*(double)
    blue);
  *hue=atan2(beta,alpha)*(180.0/MagickPI)/360.0;
  if (*hue < 0.0)
    *hue+=1.0;
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o H S L                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToHSL() transforms a (red, green, blue) to a (hue, saturation,
%  lightness) triple.
%
%  The format of the ConvertRGBToHSL method is:
%
%      void ConvertRGBToHSL(const Quantum red,const Quantum green,
%        const Quantum blue,double *hue,double *saturation,double *lightness)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel..
%
%    o hue, saturation, lightness: A pointer to a double value representing a
%      component of the HSL color space.
%
*/
MagickExport void ConvertRGBToHSL(const Quantum red,const Quantum green,
  const Quantum blue,double *hue,double *saturation,double *lightness)
{
  double
    c,
    max,
    min;

  /*
    Convert RGB to HSL colorspace.
  */
  assert(hue != (double *) NULL);
  assert(saturation != (double *) NULL);
  assert(lightness != (double *) NULL);
  max=MagickMax(QuantumScale*(double) red,MagickMax(QuantumScale*(double) green,
    QuantumScale*(double) blue));
  min=MagickMin(QuantumScale*(double) red,MagickMin(QuantumScale*(double) green,
    QuantumScale*(double) blue));
  c=max-min;
  *lightness=(max+min)/2.0;
  if (c <= 0.0)
    {
      *hue=0.0;
      *saturation=0.0;
      return;
    }
  if (fabs(max-QuantumScale*(double) red) < MagickEpsilon)
    {
      *hue=(QuantumScale*(double) green-QuantumScale*(double) blue)/c;
      if ((QuantumScale*(double) green) < (QuantumScale*(double) blue))
        *hue+=6.0;
    }
  else
    if (fabs(max-QuantumScale*(double) green) < MagickEpsilon)
      *hue=2.0+(QuantumScale*(double) blue-QuantumScale*(double) red)/c;
    else
      *hue=4.0+(QuantumScale*(double) red-QuantumScale*(double) green)/c;
  *hue*=60.0/360.0;
  if (*lightness <= 0.5)
    *saturation=c*MagickSafeReciprocal(2.0*(*lightness));
  else
    *saturation=c*MagickSafeReciprocal(2.0-2.0*(*lightness));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o H S V                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToHSV() transforms a (red, green, blue) to a (hue, saturation,
%  value) triple.
%
%  The format of the ConvertRGBToHSV method is:
%
%      void ConvertRGBToHSV(const Quantum red,const Quantum green,
%        const Quantum blue,double *hue,double *saturation,double *value)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel..
%
%    o hue, saturation, value: A pointer to a double value representing a
%      component of the HSV color space.
%
*/
MagickExport void ConvertRGBToHSV(const Quantum red,const Quantum green,
  const Quantum blue,double *hue,double *saturation,double *value)
{
  double
    c,
    max,
    min;

  /*
    Convert RGB to HSV colorspace.
  */
  assert(hue != (double *) NULL);
  assert(saturation != (double *) NULL);
  assert(value != (double *) NULL);
  max=MagickMax(QuantumScale*(double) red,MagickMax(QuantumScale*(double) green,
    QuantumScale*(double) blue));
  min=MagickMin(QuantumScale*(double) red,MagickMin(QuantumScale*(double) green,
    QuantumScale*(double) blue));
  c=max-min;
  *value=max;
  if (c <= 0.0)
    {
      *hue=0.0;
      *saturation=0.0;
      return;
    }
  if (fabs(max-QuantumScale*(double) red) < MagickEpsilon)
    {
      *hue=(QuantumScale*(double) green-QuantumScale*(double) blue)/c;
      if ((QuantumScale*(double) green) < (QuantumScale*(double) blue))
        *hue+=6.0;
    }
  else
    if (fabs(max-QuantumScale*(double) green) < MagickEpsilon)
      *hue=2.0+(QuantumScale*(double) blue-QuantumScale*(double) red)/c;
    else
      *hue=4.0+(QuantumScale*(double) red-QuantumScale*(double) green)/c;
  *hue*=60.0/360.0;
  *saturation=c*MagickSafeReciprocal(max);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o H W B                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToHWB() transforms a (red, green, blue) to a (hue, whiteness,
%  blackness) triple.
%
%  The format of the ConvertRGBToHWB method is:
%
%      void ConvertRGBToHWB(const Quantum red,const Quantum green,
%        const Quantum blue,double *hue,double *whiteness,double *blackness)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel.
%
%    o hue, whiteness, blackness: A pointer to a double value representing a
%      component of the HWB color space.
%
*/
MagickExport void ConvertRGBToHWB(const Quantum red,const Quantum green,
  const Quantum blue,double *hue,double *whiteness,double *blackness)
{
  double
    b,
    f,
    g,
    p,
    r,
    v,
    w;

  /*
    Convert RGB to HWB colorspace.
  */
  assert(hue != (double *) NULL);
  assert(whiteness != (double *) NULL);
  assert(blackness != (double *) NULL);
  r=(double) red;
  g=(double) green;
  b=(double) blue;
  w=MagickMin(r,MagickMin(g,b));
  v=MagickMax(r,MagickMax(g,b));
  *blackness=1.0-QuantumScale*v;
  *whiteness=QuantumScale*w;
  if (fabs(v-w) < MagickEpsilon)
    {
      *hue=(-1.0);
      return;
    }
  f=(fabs(r-w) < MagickEpsilon) ? g-b : ((fabs(g-w) < MagickEpsilon) ? b-r :
    r-g);
  p=(fabs(r-w) < MagickEpsilon) ? 3.0 : ((fabs(g-w) < MagickEpsilon) ? 5.0 :
    1.0);
  *hue=(p-f/(v-1.0*w))/6.0;
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o L C H a b                                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToLCHab() transforms a (red, green, blue) to a (luma, chroma,
%  hue) triple.
%
%  The format of the ConvertRGBToLCHab method is:
%
%      void ConvertRGBToLCHab(const Quantum red,const Quantum green,
%        const Quantum blue,double *luma,double *chroma,double *hue)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel.
%
%    o hue, chroma, luma: A pointer to a double value representing a
%      component of the LCHab color space.
%
*/
static inline void ConvertXYZToLCHab(const double X,const double Y,
  const double Z,double *luma,double *chroma,double *hue)
{
  double
    a,
    b;

  ConvertXYZToLab(X,Y,Z,luma,&a,&b);
  *chroma=hypot(a-0.5,b-0.5)/255.0+0.5;
  *hue=180.0*atan2(b-0.5,a-0.5)/MagickPI/360.0;
  if (*hue < 0.0)
    *hue+=1.0;
}

MagickExport void ConvertRGBToLCHab(const Quantum red,const Quantum green,
  const Quantum blue,double *luma,double *chroma,double *hue)
{
  double
    X,
    Y,
    Z;

  /*
   Convert RGB to LCHab colorspace.
  */
  assert(luma != (double *) NULL);
  assert(chroma != (double *) NULL);
  assert(hue != (double *) NULL);
  ConvertRGBToXYZ(red,green,blue,&X,&Y,&Z);
  ConvertXYZToLCHab(X,Y,Z,luma,chroma,hue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o n v e r t R G B T o L C H u v                                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ConvertRGBToLCHuv() transforms a (red, green, blue) to a (luma, chroma,
%  hue) triple.
%
%  The format of the ConvertRGBToLCHuv method is:
%
%      void ConvertRGBToLCHuv(const Quantum red,const Quantum green,
%        const Quantum blue,double *luma,double *chroma,double *hue)
%
%  A description of each parameter follows:
%
%    o red, green, blue: A Quantum value representing the red, green, and
%      blue component of a pixel.
%
%    o hue, chroma, luma: A pointer to a double value representing a
%      component of the LCHuv color space.
%
*/

static inline void ConvertXYZToLCHuv(const double X,const double Y,
  const double Z,double *luma,double *chroma,double *hue)
{
  double
    u,
    v;

  ConvertXYZToLuv(X,Y,Z,luma,&u,&v);
  *chroma=hypot(354.0*u-134.0,262.0*v-140.0)/255.0+0.5;
  *hue=180.0*atan2(262.0*v-140.0,354.0*u-134.0)/MagickPI/360.0;
  if (*hue < 0.0)
    *hue+=1.0;
}

MagickExport void ConvertRGBToLCHuv(const Quantum red,const Quantum green,
  const Quantum blue,double *luma,double *chroma,double *hue)
{
  double
    X,
    Y,
    Z;

  /*
    Convert RGB to LCHuv colorspace.
  */
  assert(luma != (double *) NULL);
  assert(chroma != (double *) NULL);
  assert(hue != (double *) NULL);
  ConvertRGBToXYZ(red,green,blue,&X,&Y,&Z);
  ConvertXYZToLCHuv(X,Y,Z,luma,chroma,hue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   E x p a n d A f f i n e                                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ExpandAffine() computes the affine's expansion factor, i.e. the square root
%  of the factor by which the affine transform affects area. In an affine
%  transform composed of scaling, rotation, shearing, and translation, returns
%  the amount of scaling.
%
%  The format of the ExpandAffine method is:
%
%      double ExpandAffine(const AffineMatrix *affine)
%
%  A description of each parameter follows:
%
%    o expansion: ExpandAffine returns the affine's expansion factor.
%
%    o affine: A pointer the affine transform of type AffineMatrix.
%
*/
MagickExport double ExpandAffine(const AffineMatrix *affine)
{
  assert(affine != (const AffineMatrix *) NULL);
  return(sqrt(fabs(affine->sx*affine->sy-affine->rx*affine->ry)));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e n e r a t e D i f f e r e n t i a l N o i s e                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GenerateDifferentialNoise() generates differential noise.
%
%  The format of the GenerateDifferentialNoise method is:
%
%      double GenerateDifferentialNoise(RandomInfo *random_info,
%        const Quantum pixel,const NoiseType noise_type,
%        const MagickRealType attenuate)
%
%  A description of each parameter follows:
%
%    o random_info: the random info.
%
%    o pixel: noise is relative to this pixel value.
%
%    o noise_type: the type of noise.
%
%    o attenuate:  attenuate the noise.
%
*/
MagickExport double GenerateDifferentialNoise(RandomInfo *random_info,
  const Quantum pixel,const NoiseType noise_type,const MagickRealType attenuate)
{
#define SigmaUniform  (attenuate*0.015625)
#define SigmaGaussian  (attenuate*0.015625)
#define SigmaImpulse  (attenuate*0.1)
#define SigmaLaplacian (attenuate*0.0390625)
#define SigmaMultiplicativeGaussian  (attenuate*0.5)
#define SigmaPoisson  (attenuate*12.5)
#define SigmaRandom  (attenuate)
#define TauGaussian  (attenuate*0.078125)

  double
    alpha,
    beta,
    noise,
    sigma;

  alpha=GetPseudoRandomValue(random_info);
  switch (noise_type)
  {
    case UniformNoise:
    default:
    {
      noise=(double) pixel+(double) QuantumRange*SigmaUniform*(alpha-0.5);
      break;
    }
    case GaussianNoise:
    {
      double
        gamma,
        tau;

      if (fabs(alpha) < MagickEpsilon)
        alpha=1.0;
      beta=GetPseudoRandomValue(random_info);
      gamma=sqrt(-2.0*log(alpha));
      sigma=gamma*cos((double) (2.0*MagickPI*beta));
      tau=gamma*sin((double) (2.0*MagickPI*beta));
      noise=(double) pixel+sqrt((double) pixel)*SigmaGaussian*sigma+
        (double) QuantumRange*TauGaussian*tau;
      break;
    }
    case ImpulseNoise:
    {
      if (alpha < (SigmaImpulse/2.0))
        noise=0.0;
      else
        if (alpha >= (1.0-(SigmaImpulse/2.0)))
          noise=(double) QuantumRange;
        else
          noise=(double) pixel;
      break;
    }
    case LaplacianNoise:
    {
      if (alpha <= 0.5)
        {
          if (alpha <= MagickEpsilon)
            noise=(double) (pixel-QuantumRange);
          else
            noise=(double) pixel+(double) QuantumRange*SigmaLaplacian*
              log(2.0*alpha)+0.5;
          break;
        }
      beta=1.0-alpha;
      if (beta <= (0.5*MagickEpsilon))
        noise=(double) (pixel+QuantumRange);
      else
        noise=(double) pixel-(double) QuantumRange*SigmaLaplacian*
          log(2.0*beta)+0.5;
      break;
    }
    case MultiplicativeGaussianNoise:
    {
      sigma=1.0;
      if (alpha > MagickEpsilon)
        sigma=sqrt(-2.0*log(alpha));
      beta=GetPseudoRandomValue(random_info);
      noise=(double) pixel+(double) pixel*SigmaMultiplicativeGaussian*sigma*
        cos((double) (2.0*MagickPI*beta))/2.0;
      break;
    }
    case PoissonNoise:
    {
      double
        poisson;

      ssize_t
        i;

      poisson=exp(-SigmaPoisson*QuantumScale*(double) pixel);
      for (i=0; alpha > poisson; i++)
      {
        beta=GetPseudoRandomValue(random_info);
        alpha*=beta;
      }
      noise=(double) QuantumRange*i*MagickSafeReciprocal(SigmaPoisson);
      break;
    }
    case RandomNoise:
    {
      noise=(double) QuantumRange*SigmaRandom*alpha;
      break;
    }
  }
  return(noise);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t O p t i m a l K e r n e l W i d t h                                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetOptimalKernelWidth() computes the optimal kernel radius for a convolution
%  filter.  Start with the minimum value of 3 pixels and walk out until we drop
%  below the threshold of one pixel numerical accuracy.
%
%  The format of the GetOptimalKernelWidth method is:
%
%      size_t GetOptimalKernelWidth(const double radius,const double sigma)
%
%  A description of each parameter follows:
%
%    o radius: the radius of the Gaussian, in pixels, not counting the center
%      pixel.
%
%    o sigma: the standard deviation of the Gaussian, in pixels.
%
*/
MagickExport size_t GetOptimalKernelWidth1D(const double radius,
  const double sigma)
{
  double
    alpha,
    beta,
    gamma,
    normalize,
    value;

  size_t
    width;

  ssize_t
    i,
    j;

  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"...");
  if (radius > MagickEpsilon)
    return((size_t) (2.0*ceil(radius)+1.0));
  gamma=fabs(sigma);
  if (gamma <= MagickEpsilon)
    return(3UL);
  alpha=MagickSafeReciprocal(2.0*gamma*gamma);
  beta=(double) MagickSafeReciprocal((double) MagickSQ2PI*gamma);
  for (width=5; ; )
  {
    normalize=0.0;
    j=(ssize_t) (width-1)/2;
    for (i=(-j); i <= j; i++)
      normalize+=exp(-((double) (i*i))*alpha)*beta;
    value=exp(-((double) (j*j))*alpha)*beta/normalize;
    if ((value < QuantumScale) || (value < MagickEpsilon))
      break;
    width+=2;
  }
  return((size_t) (width-2));
}

MagickExport size_t GetOptimalKernelWidth2D(const double radius,
  const double sigma)
{
  double
    alpha,
    beta,
    gamma,
    normalize,
    value;

  size_t
    width;

  ssize_t
    j,
    u,
    v;

  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"...");
  if (radius > MagickEpsilon)
    return((size_t) (2.0*ceil(radius)+1.0));
  gamma=fabs(sigma);
  if (gamma <= MagickEpsilon)
    return(3UL);
  alpha=MagickSafeReciprocal(2.0*gamma*gamma);
  beta=(double) MagickSafeReciprocal((double) Magick2PI*gamma*gamma);
  for (width=5; ; )
  {
    normalize=0.0;
    j=(ssize_t) (width-1)/2;
    for (v=(-j); v <= j; v++)
      for (u=(-j); u <= j; u++)
        normalize+=exp(-((double) (u*u+v*v))*alpha)*beta;
    value=exp(-((double) (j*j))*alpha)*beta/normalize;
    if ((value < QuantumScale) || (value < MagickEpsilon))
      break;
    width+=2;
  }
  return((size_t) (width-2));
}

MagickExport size_t  GetOptimalKernelWidth(const double radius,
  const double sigma)
{
  return(GetOptimalKernelWidth1D(radius,sigma));
}
