/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%                 SSSSS  TTTTT  RRRR   EEEEE   AAA   M   M                    %
%                 SS       T    R   R  E      A   A  MM MM                    %
%                  SSS     T    RRRR   EEE    AAAAA  M M M                    %
%                    SS    T    R R    E      A   A  M   M                    %
%                 SSSSS    T    R  R   EEEEE  A   A  M   M                    %
%                                                                             %
%                                                                             %
%                     Stream image to a raw image format.                     %
%                                                                             %
%                           Software Design                                   %
%                                Cristy                                       %
%                              July 1992                                      %
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
%  Stream is a lightweight utility designed to extract pixels from large image
%  files to a raw format using a minimum of system resources.  The entire
%  image or any regular portion of the image can be extracted.
%
%
*/

/*
  Include declarations.
*/
#include "wand/studio.h"
#include "wand/MagickWand.h"

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%  M a i n                                                                    %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%
*/

static int StreamMain(int argc,char **argv)
{
  ExceptionInfo
    *exception;

  ImageInfo
    *image_info;

  MagickBooleanType
    status;

  MagickCoreGenesis(*argv,MagickTrue);
  MagickWandGenesis();
  exception=AcquireExceptionInfo();
  image_info=AcquireImageInfo();
  status=MagickCommandGenesis(image_info,StreamImageCommand,argc,argv,
    (char **) NULL,exception);
  image_info=DestroyImageInfo(image_info);
  exception=DestroyExceptionInfo(exception);
  MagickWandTerminus();
  return(status != MagickFalse ? 0 : 1);
}

#if !defined(MAGICKCORE_WINDOWS_SUPPORT) || defined(__CYGWIN__)
int main(int argc,char **argv)
{
  return(StreamMain(argc,argv));
}
#else
static LONG WINAPI NTUncaughtException(EXCEPTION_POINTERS *info)
{ 
  magick_unreferenced(info);
  AsynchronousResourceComponentTerminus();
  return(EXCEPTION_CONTINUE_SEARCH);
}

int wmain(int argc,wchar_t *argv[])
{
  char
    **utf8;

  int
    i,
    status;

  SetUnhandledExceptionFilter(NTUncaughtException);
  SetConsoleOutputCP(CP_UTF8);
  utf8=NTArgvToUTF8(argc,argv);
  status=StreamMain(argc,utf8);
  for (i=0; i < argc; i++)
    utf8[i]=DestroyString(utf8[i]);
  utf8=(char **) RelinquishMagickMemory(utf8);
  return(status);
}
#endif
