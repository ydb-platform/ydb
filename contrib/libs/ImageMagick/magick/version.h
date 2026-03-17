/*
  Copyright 1999 ImageMagick Studio LLC, a non-profit organization
  dedicated to making software imaging solutions freely available.
  
  You may not use this file except in compliance with the License.
  obtain a copy of the License at
  
    https://imagemagick.org/license/
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  MagickCore version methods.
*/
#ifndef _MAGICKCORE_VERSION_H
#define _MAGICKCORE_VERSION_H

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

/*
  Define declarations.
*/
#define MagickPackageName "ImageMagick"
#define MagickCopyright  "(C) 1999 ImageMagick Studio LLC"
#define MagickLibVersion  0x69D
#define MagickLibVersionText  "6.9.13"
#define MagickLibVersionNumber  7,0,0
#define MagickLibAddendum  "-40"
#define MagickLibInterface  7
#define MagickLibMinInterface  7
#define MagickPlatform  "x86_64"
#define MagickppLibVersionText  "6.9.13"
#define MagickppLibVersionNumber  9:0:0
#define MagickppLibAddendum  "-40"
#define MagickppLibInterface  9
#define MagickppLibMinInterface  9
#define MagickGitRevision  "f4b7844b1:20260222"
#define MagickReleaseDate "2019-06-08"
#define MagickHomeURL  "file:///var/empty/imagemagick-6.9.13-40/share/doc/ImageMagick-6/index.html"
#define MagickAuthoritativeLicense  \
  "https://imagemagick.org/license/"
#define MagickAuthoritativeURL  "https://legacy.imagemagick.org"
#if (MAGICKCORE_QUANTUM_DEPTH == 8)
#define MagickQuantumDepth  "Q8"
#define MagickQuantumRange  "255"
#elif (MAGICKCORE_QUANTUM_DEPTH == 16)
#define MagickQuantumDepth  "Q16"
#define MagickQuantumRange  "65535"
#elif (MAGICKCORE_QUANTUM_DEPTH == 32)
#define MagickQuantumDepth  "Q32"
#define MagickQuantumRange  "4294967295"
#elif (MAGICKCORE_QUANTUM_DEPTH == 64)
#define MagickQuantumDepth  "Q64"
#define MagickQuantumRange  "18446744073709551615.0"
#else
#define MagickQuantumDepth  "Q?"
#define MagickQuantumRange  "?"
#endif
#if defined(MAGICKCORE_HDRI_SUPPORT)
#define MagickHDRISupport  "-HDRI"
#else
#define MagickHDRISupport  ""
#endif
#define MagickVersion  \
  MagickPackageName " " MagickLibVersionText MagickLibAddendum " " \
  MagickQuantumDepth MagickHDRISupport " " MagickPlatform " " \
  MagickGitRevision " " MagickAuthoritativeURL

extern MagickExport char
  *GetMagickHomeURL(void);

extern MagickExport const char
  *GetMagickCopyright(void) magick_attribute((__const__)),
  *GetMagickDelegates(void) magick_attribute((__const__)),
  *GetMagickFeatures(void) magick_attribute((__const__)),
  *GetMagickLicense(void) magick_attribute((__const__)),
  *GetMagickPackageName(void) magick_attribute((__const__)),
  *GetMagickQuantumDepth(size_t *),
  *GetMagickQuantumRange(size_t *),
  *GetMagickReleaseDate(void) magick_attribute((__const__)),
  *GetMagickVersion(size_t *);

extern MagickExport void
  ListMagickVersion(FILE *);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif
