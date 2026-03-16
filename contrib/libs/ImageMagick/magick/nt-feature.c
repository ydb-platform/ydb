/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%                                 N   N  TTTTT                                %
%                                 NN  N    T                                  %
%                                 N N N    T                                  %
%                                 N  NN    T                                  %
%                                 N   N    T                                  %
%                                                                             %
%                                                                             %
%                   Windows NT Feature Methods for MagickCore                 %
%                                                                             %
%                               Software Design                               %
%                                    Cristy                                   %
%                                December 1996                                %
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
#if defined(MAGICKCORE_WINDOWS_SUPPORT) || defined(__CYGWIN__)
#include "magick/cache.h"
#include "magick/colorspace.h"
#include "magick/colorspace-private.h"
#include "magick/draw.h"
#include "magick/exception.h"
#include "magick/exception-private.h"
#include "magick/image-private.h"
#include "magick/locale-private.h"
#include "magick/memory_.h"
#include "magick/monitor.h"
#include "magick/monitor-private.h"
#include "magick/nt-base.h"
#include "magick/nt-base-private.h"
#include "magick/nt-feature.h"
#include "magick/pixel-accessor.h"
#include "magick/quantum.h"
#include "magick/string_.h"
#include "magick/token.h"
#include "magick/splay-tree.h"
#include "magick/utility.h"
#if defined(__CYGWIN__)
#define WIN32_LEAN_AND_MEAN
#define VC_EXTRALEAN
#include <windows.h>
#endif

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   I s M a g i c k C o n f l i c t                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  IsMagickConflict() returns true if the image format conflicts with a logical
%  drive (.e.g. X:).
%
%  The format of the IsMagickConflict method is:
%
%      MagickBooleanType IsMagickConflict(const char *magick)
%
%  A description of each parameter follows:
%
%    o magick: Specifies the image format.
%
*/
MagickExport MagickBooleanType NTIsMagickConflict(const char *magick)
{
  MagickBooleanType
    status;

  assert(magick != (char *) NULL);
  if (strlen(magick) > 1)
    return(MagickFalse);
  status=(GetLogicalDrives() & (1 <<
    ((LocaleToUppercase((int) (*magick)))-'A'))) != 0 ? MagickTrue : MagickFalse;
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T A c q u i r e T y p e C a c h e                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTAcquireTypeCache() loads a Windows TrueType fonts.
%
%  The format of the NTAcquireTypeCache method is:
%
%      MagickBooleanType NTAcquireTypeCache(SplayTreeInfo *type_cache)
%
%  A description of each parameter follows:
%
%    o type_cache: A linked list of fonts.
%
*/
MagickExport MagickBooleanType NTAcquireTypeCache(SplayTreeInfo *type_cache,
  ExceptionInfo *exception)
{
  HKEY
    reg_key = (HKEY) INVALID_HANDLE_VALUE;

  LONG
    res;

  int
    list_entries = 0;

  char
    buffer[MaxTextExtent],
    system_root[MaxTextExtent],
    font_root[MaxTextExtent];

  DWORD
    type,
    system_root_length;

  MagickBooleanType
    status;

  /*
    Try to find the right Windows*\CurrentVersion key, the SystemRoot and
    then the Fonts key
  */
  res = RegOpenKeyExA (HKEY_LOCAL_MACHINE,
    "SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion", 0, KEY_READ, &reg_key);
  if (res == ERROR_SUCCESS) {
    system_root_length=sizeof(system_root)-1;
    res = RegQueryValueExA(reg_key,"SystemRoot",NULL, &type,
      (BYTE*) system_root, &system_root_length);
  }
  if (res != ERROR_SUCCESS) {
    res = RegOpenKeyExA (HKEY_LOCAL_MACHINE,
      "SOFTWARE\\Microsoft\\Windows\\CurrentVersion", 0, KEY_READ, &reg_key);
    if (res == ERROR_SUCCESS) {
      system_root_length=sizeof(system_root)-1;
      res = RegQueryValueExA(reg_key,"SystemRoot",NULL, &type,
        (BYTE*)system_root, &system_root_length);
    }
  }
  if (res == ERROR_SUCCESS)
    res = RegOpenKeyExA (reg_key, "Fonts",0, KEY_READ, &reg_key);
  if (res != ERROR_SUCCESS)
    return(MagickFalse);
  *font_root='\0';
  (void) CopyMagickString(buffer,system_root,MaxTextExtent);
  (void) ConcatenateMagickString(buffer,"\\fonts\\arial.ttf",MaxTextExtent);
  if (IsPathAccessible(buffer) != MagickFalse)
    {
      (void) CopyMagickString(font_root,system_root,MaxTextExtent);
      (void) ConcatenateMagickString(font_root,"\\fonts\\",MaxTextExtent);
    }
  else
    {
      (void) CopyMagickString(font_root,system_root,MaxTextExtent);
      (void) ConcatenateMagickString(font_root,"\\",MaxTextExtent);
    }

  {
    TypeInfo
      *type_info;

    DWORD
      registry_index = 0,
      type,
      value_data_size,
      value_name_length;

    char
      value_data[MaxTextExtent],
      value_name[MaxTextExtent];

    res = ERROR_SUCCESS;

    while (res != ERROR_NO_MORE_ITEMS)
      {
        char
          *family_extent,
          token[MaxTextExtent],
          *pos,
          *q;

        value_name_length = sizeof(value_name) - 1;
        value_data_size = sizeof(value_data) - 1;
        res=RegEnumValueA(reg_key,registry_index,value_name,&value_name_length,
          0,&type,(BYTE *) value_data,&value_data_size);
        registry_index++;
        if (res != ERROR_SUCCESS)
          continue;
        if ((pos=strstr(value_name," (TrueType)")) == (char*) NULL)
          continue;
        *pos='\0';  /* Remove (TrueType) from string */
        type_info=(TypeInfo *) AcquireMagickMemory(sizeof(*type_info));
        if (type_info == (TypeInfo *) NULL)
          ThrowFatalException(ResourceLimitFatalError,"MemoryAllocationFailed");
        (void) memset(type_info,0,sizeof(TypeInfo));
        type_info->path=ConstantString("Windows Fonts");
        type_info->signature=MagickCoreSignature;
        (void) CopyMagickString(buffer,value_name,MaxTextExtent);  /* name */
        for (pos=buffer; *pos != 0; pos++)
          if (*pos == ' ')
            *pos='-';
        type_info->name=ConstantString(buffer);
        type_info->description=ConstantString(value_name);  /* fullname */
        type_info->format=ConstantString("truetype");  /* format */
        if (strchr(value_data,'\\') != (char *) NULL)  /* glyphs */
          (void) CopyMagickString(buffer,value_data,MaxTextExtent);
        else
          {
            (void) CopyMagickString(buffer,font_root,MaxTextExtent);
            (void) ConcatenateMagickString(buffer,value_data,MaxTextExtent);
          }
        LocaleLower(buffer);
        type_info->glyphs=ConstantString(buffer);
        type_info->stretch=NormalStretch;
        type_info->style=NormalStyle;
        type_info->weight=400;
        /*
          Some fonts are known to require special encodings.
        */
        if ( (LocaleCompare(type_info->name, "Symbol") == 0 ) ||
             (LocaleCompare(type_info->name, "Wingdings") == 0 ) ||
             (LocaleCompare(type_info->name, "Wingdings-2") == 0 ) ||
             (LocaleCompare(type_info->name, "Wingdings-3") == 0 ) )
          type_info->encoding=ConstantString("AppleRoman");
        family_extent=value_name;
        for (q=value_name; *q != '\0'; )
        {
            (void) GetNextToken(q,(const char **) &q,MaxTextExtent,token);
            if (*token == '\0')
              break;

            if (LocaleCompare(token,"Italic") == 0)
              {
                type_info->style=ItalicStyle;
              }

            else if (LocaleCompare(token,"Oblique") == 0)
              {
                type_info->style=ObliqueStyle;
              }

            else if (LocaleCompare(token,"Bold") == 0)
              {
                type_info->weight=700;
              }

            else if (LocaleCompare(token,"Thin") == 0)
              {
                type_info->weight=100;
              }

            else if ( (LocaleCompare(token,"ExtraLight") == 0) ||
                      (LocaleCompare(token,"UltraLight") == 0) )
              {
                type_info->weight=200;
              }

            else if (LocaleCompare(token,"Light") == 0)
              {
                type_info->weight=300;
              }

            else if ( (LocaleCompare(token,"Normal") == 0) ||
                      (LocaleCompare(token,"Regular") == 0) )
              {
                type_info->weight=400;
              }

            else if (LocaleCompare(token,"Medium") == 0)
              {
                type_info->weight=500;
              }

            else if ( (LocaleCompare(token,"SemiBold") == 0) ||
                      (LocaleCompare(token,"DemiBold") == 0) )
              {
                type_info->weight=600;
              }

            else if ( (LocaleCompare(token,"ExtraBold") == 0) ||
                      (LocaleCompare(token,"UltraBold") == 0) )
              {
                type_info->weight=800;
              }

            else if ( (LocaleCompare(token,"Heavy") == 0) ||
                      (LocaleCompare(token,"Black") == 0) )
              {
                type_info->weight=900;
              }

            else if (LocaleCompare(token,"Condensed") == 0)
              {
                type_info->stretch = CondensedStretch;
              }

            else if (LocaleCompare(token,"Expanded") == 0)
              {
                type_info->stretch = ExpandedStretch;
              }

            else if (LocaleCompare(token,"ExtraCondensed") == 0)
              {
                type_info->stretch = ExtraCondensedStretch;
              }

            else if (LocaleCompare(token,"ExtraExpanded") == 0)
              {
                type_info->stretch = ExtraExpandedStretch;
              }

            else if (LocaleCompare(token,"SemiCondensed") == 0)
              {
                type_info->stretch = SemiCondensedStretch;
              }

            else if (LocaleCompare(token,"SemiExpanded") == 0)
              {
                type_info->stretch = SemiExpandedStretch;
              }

            else if (LocaleCompare(token,"UltraCondensed") == 0)
              {
                type_info->stretch = UltraCondensedStretch;
              }

            else if (LocaleCompare(token,"UltraExpanded") == 0)
              {
                type_info->stretch = UltraExpandedStretch;
              }

            else
              {
                family_extent=q;
              }
          }

        (void) CopyMagickString(buffer,value_name,family_extent-value_name+1);
        StripString(buffer);
        type_info->family=ConstantString(buffer);

        list_entries++;
        status=AddValueToSplayTree(type_cache,type_info->name,type_info);
        if (status == MagickFalse)
          (void) ThrowMagickException(exception,GetMagickModule(),
            ResourceLimitError,"MemoryAllocationFailed","`%s'",type_info->name);
      }
  }
  RegCloseKey(reg_key);
  return(MagickTrue);
}

#endif
