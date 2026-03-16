/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%           DDDD   EEEEE  L      EEEEE   GGGG   AAA   TTTTT  EEEEE            %
%           D   D  E      L      E      G      A   A    T    E                %
%           D   D  EEE    L      EEE    G  GG  AAAAA    T    EEE              %
%           D   D  E      L      E      G   G  A   A    T    E                %
%           DDDD   EEEEE  LLLLL  EEEEE   GGG   A   A    T    EEEEE            %
%                                                                             %
%                                                                             %
%             MagickCore Methods to Read/Write/Invoke Delegates               %
%                                                                             %
%                             Software Design                                 %
%                                  Cristy                                     %
%                               October 1998                                  %
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
%  The Delegates methods associate a set of commands with a particular
%  image format.  ImageMagick uses delegates for formats it does not handle
%  directly.
%
%  Thanks to Bob Friesenhahn for the initial inspiration and design of the
%  delegates methods.
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
#include "magick/client.h"
#include "magick/configure.h"
#include "magick/constitute.h"
#include "magick/delegate.h"
#include "magick/exception.h"
#include "magick/exception-private.h"
#include "magick/hashmap.h"
#include "magick/image-private.h"
#include "magick/list.h"
#include "magick/memory_.h"
#include "magick/nt-base-private.h"
#include "magick/option.h"
#include "magick/policy.h"
#include "magick/property.h"
#include "magick/resource_.h"
#include "magick/semaphore.h"
#include "magick/signature.h"
#include "magick/string_.h"
#include "magick/token.h"
#include "magick/token-private.h"
#include "magick/utility.h"
#include "magick/utility-private.h"
#include "magick/xml-tree.h"
#include "magick/xml-tree-private.h"

/*
  Define declarations.
*/
#if defined(__APPLE__)
  #include "TargetConditionals.h"
  #if TARGET_OS_IOS || TARGET_OS_WATCH || TARGET_OS_TV
    #define system(s) ((s)==NULL ? 0 : -1)
  #endif // end iOS
#elif defined(__ANDROID__)
  #define system(s) ((s)==NULL ? 0 : -1)
#endif
#define DelegateFilename  "delegates.xml"
#if defined(MAGICKCORE_WINDOWS_SUPPORT)
  #define DELEGATE_ESC "&quot;"
#else
  #define DELEGATE_ESC "&apos;"
#endif

/*
  Declare delegate map.
*/
static const char
  *DelegateMap = (const char *)
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<delegatemap>"
    "  <delegate decode=\"bpg\" command=\"" DELEGATE_ESC "bpgdec" DELEGATE_ESC " -b 16 -o " DELEGATE_ESC "%o.png" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.png" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"png\" encode=\"bpg\" command=\"" DELEGATE_ESC "bpgenc" DELEGATE_ESC " -b 12 -q %~ -o " DELEGATE_ESC "%o" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"browse\" stealth=\"True\" spawn=\"True\" command=\"" DELEGATE_ESC "xdg-open" DELEGATE_ESC " https://imagemagick.org/; rm " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"cdr\" command=\"" DELEGATE_ESC "uniconvertor" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%o.svg" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.svg" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"cgm\" command=\"" DELEGATE_ESC "uniconvertor" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%o.svg" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.svg" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"https\" command=\"" DELEGATE_ESC "curl" DELEGATE_ESC " -s -k -L -o " DELEGATE_ESC "%o" DELEGATE_ESC " " DELEGATE_ESC "https:%M" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"doc\" command=\"" DELEGATE_ESC "soffice" DELEGATE_ESC " --convert-to pdf -outdir `dirname " DELEGATE_ESC "%i" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC " 2&gt; " DELEGATE_ESC "%u" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.pdf" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"docx\" command=\"" DELEGATE_ESC "soffice" DELEGATE_ESC " --convert-to pdf -outdir `dirname " DELEGATE_ESC "%i" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC " 2&gt; " DELEGATE_ESC "%u" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.pdf" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"dng:decode\" command=\"" DELEGATE_ESC "ufraw-batch" DELEGATE_ESC " --silent --create-id=also --out-type=png --out-depth=16 " DELEGATE_ESC "--output=%u.png" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"dot\" command=\"" DELEGATE_ESC "dot" DELEGATE_ESC " -Tsvg " DELEGATE_ESC "%i" DELEGATE_ESC " -o " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"dvi\" command=\"" DELEGATE_ESC "dvips" DELEGATE_ESC " -sstdout=%%stderr -o " DELEGATE_ESC "%o" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"dxf\" command=\"" DELEGATE_ESC "uniconvertor" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%o.svg" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.svg" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"edit\" stealth=\"True\" command=\"" DELEGATE_ESC "xterm" DELEGATE_ESC " -title " DELEGATE_ESC "Edit Image Comment" DELEGATE_ESC " -e vi " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"eps\" encode=\"pdf\" mode=\"bi\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 " DELEGATE_ESC "-sDEVICE=pdfwrite" DELEGATE_ESC " " DELEGATE_ESC "-sOutputFile=%o" DELEGATE_ESC " " DELEGATE_ESC "-f%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"eps\" encode=\"ps\" mode=\"bi\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=ps2write" DELEGATE_ESC " " DELEGATE_ESC "-sOutputFile=%o" DELEGATE_ESC " " DELEGATE_ESC "-f%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"fig\" command=\"" DELEGATE_ESC "uniconvertor" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%o.svg" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.svg" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"hpg\" command=\"" DELEGATE_ESC "hp2xx" DELEGATE_ESC " -sstdout=%%stderr -m eps -f `basename " DELEGATE_ESC "%o" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC ";     mv -f `basename " DELEGATE_ESC "%o" DELEGATE_ESC "` " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"hpgl\" command=\"" DELEGATE_ESC "hp2xx" DELEGATE_ESC " -sstdout=%%stderr -m eps -f `basename " DELEGATE_ESC "%o" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC ";     mv -f `basename " DELEGATE_ESC "%o" DELEGATE_ESC "` " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"htm\" command=\"" DELEGATE_ESC "html2ps" DELEGATE_ESC " -U -o " DELEGATE_ESC "%o" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"html\" command=\"" DELEGATE_ESC "html2ps" DELEGATE_ESC " -U -o " DELEGATE_ESC "%o" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ilbm\" command=\"" DELEGATE_ESC "ilbmtoppm" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC " &gt; " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"jpg\" encode=\"lep\" mode=\"encode\" command=\"" DELEGATE_ESC "lepton" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"jxr\" command=\"mv " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%i.jxr" DELEGATE_ESC "; " DELEGATE_ESC "JxrDecApp" DELEGATE_ESC " -i " DELEGATE_ESC "%i.jxr" DELEGATE_ESC " -o " DELEGATE_ESC "%o.tiff" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.jxr" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.tiff" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"lep\" mode=\"decode\" command=\"" DELEGATE_ESC "lepton" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"odt\" command=\"" DELEGATE_ESC "soffice" DELEGATE_ESC " --convert-to pdf -outdir `dirname " DELEGATE_ESC "%i" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC " 2&gt; " DELEGATE_ESC "%u" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.pdf" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"pcl:cmyk\" stealth=\"True\" command=\"" DELEGATE_ESC "pcl6" DELEGATE_ESC " -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pamcmyk32" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"pcl:color\" stealth=\"True\" command=\"" DELEGATE_ESC "pcl6" DELEGATE_ESC " -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=ppmraw" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"pcl:mono\" stealth=\"True\" command=\"" DELEGATE_ESC "pcl6" DELEGATE_ESC " -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pbmraw" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"pdf\" encode=\"eps\" mode=\"bi\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 -sPDFPassword=" DELEGATE_ESC "%a" DELEGATE_ESC " " DELEGATE_ESC "-sDEVICE=eps2write" DELEGATE_ESC " " DELEGATE_ESC "-sOutputFile=%o" DELEGATE_ESC " " DELEGATE_ESC "-f%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"pdf\" encode=\"ps\" mode=\"bi\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=ps2write" DELEGATE_ESC " -sPDFPassword=" DELEGATE_ESC "%a" DELEGATE_ESC " " DELEGATE_ESC "-sOutputFile=%o" DELEGATE_ESC " " DELEGATE_ESC "-f%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"png\" encode=\"clipboard\" command=\"" DELEGATE_ESC "xclip" DELEGATE_ESC " -selection clipboard -t image/png " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"clipboard\" command=\"" DELEGATE_ESC "xclip" DELEGATE_ESC " -selection clipboard -o &gt; " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"png\" encode=\"webp\" command=\"" DELEGATE_ESC "cwebp" DELEGATE_ESC " -quiet -q %Q " DELEGATE_ESC "%i" DELEGATE_ESC " -o " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"pnm\" encode=\"ilbm\" mode=\"encode\" command=\"" DELEGATE_ESC "ppmtoilbm" DELEGATE_ESC " -24if " DELEGATE_ESC "%i" DELEGATE_ESC " &gt; " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"tiff\" encode=\"jxr\" command=\"mv " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%i.tiff" DELEGATE_ESC "; " DELEGATE_ESC "JxrEncApp" DELEGATE_ESC " -i " DELEGATE_ESC "%i.tiff" DELEGATE_ESC " -o " DELEGATE_ESC "%o.jxr" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.tiff" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.jxr" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"tiff\" encode=\"wdp\" command=\"mv " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%i.tiff" DELEGATE_ESC "; " DELEGATE_ESC "JxrEncApp" DELEGATE_ESC " -i " DELEGATE_ESC "%i.tiff" DELEGATE_ESC " -o " DELEGATE_ESC "%o.jxr" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.tiff" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.jxr" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ppt\" command=\"" DELEGATE_ESC "soffice" DELEGATE_ESC " --convert-to pdf -outdir `dirname " DELEGATE_ESC "%i" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC " 2&gt; " DELEGATE_ESC "%u" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.pdf" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"pptx\" command=\"" DELEGATE_ESC "soffice" DELEGATE_ESC " --convert-to pdf -outdir `dirname " DELEGATE_ESC "%i" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC " 2&gt; " DELEGATE_ESC "%u" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.pdf" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps\" encode=\"prt\" command=\"" DELEGATE_ESC "lpr" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps:alpha\" stealth=\"True\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pngalpha" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps:cmyk\" stealth=\"True\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pamcmyk32" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps:color\" stealth=\"True\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pnmraw" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps\" encode=\"eps\" mode=\"bi\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=eps2write" DELEGATE_ESC " " DELEGATE_ESC "-sOutputFile=%o" DELEGATE_ESC " " DELEGATE_ESC "-f%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps\" encode=\"pdf\" mode=\"bi\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pdfwrite" DELEGATE_ESC " " DELEGATE_ESC "-sOutputFile=%o" DELEGATE_ESC " " DELEGATE_ESC "-f%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps\" encode=\"print\" mode=\"encode\" command=\"lpr " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"ps:mono\" stealth=\"True\" command=\"" DELEGATE_ESC "gs" DELEGATE_ESC " -sstdout=%%stderr -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pbmraw" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC " " DELEGATE_ESC "-f%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"shtml\" command=\"" DELEGATE_ESC "html2ps" DELEGATE_ESC " -U -o " DELEGATE_ESC "%o" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"sid\" command=\"" DELEGATE_ESC "mrsidgeodecode" DELEGATE_ESC " -if sid -i " DELEGATE_ESC "%i" DELEGATE_ESC " -of tif -o " DELEGATE_ESC "%o" DELEGATE_ESC " &gt; " DELEGATE_ESC "%u" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"svg\" command=\"" DELEGATE_ESC "rsvg-convert" DELEGATE_ESC " --dpi-x %x --dpi-y %y -o " DELEGATE_ESC "%o" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
#ifndef MAGICKCORE_RSVG_DELEGATE
    "  <delegate decode=\"svg:decode\" stealth=\"True\" command=\"" DELEGATE_ESC "inkscape" DELEGATE_ESC " " DELEGATE_ESC "%s" DELEGATE_ESC " --export-png=" DELEGATE_ESC "%s" DELEGATE_ESC " --export-dpi=" DELEGATE_ESC "%s" DELEGATE_ESC " --export-background=" DELEGATE_ESC "%s" DELEGATE_ESC " --export-background-opacity=" DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
#endif
    "  <delegate decode=\"tiff\" encode=\"launch\" mode=\"encode\" command=\"" DELEGATE_ESC "gimp" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"wdp\" command=\"mv " DELEGATE_ESC "%i" DELEGATE_ESC " " DELEGATE_ESC "%i.jxr" DELEGATE_ESC "; " DELEGATE_ESC "JxrDecApp" DELEGATE_ESC " -i " DELEGATE_ESC "%i.jxr" DELEGATE_ESC " -o " DELEGATE_ESC "%o.tiff" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.jxr" DELEGATE_ESC " " DELEGATE_ESC "%i" DELEGATE_ESC "; mv " DELEGATE_ESC "%o.tiff" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"webp\" command=\"" DELEGATE_ESC "dwebp" DELEGATE_ESC " -pam " DELEGATE_ESC "%i" DELEGATE_ESC " -o " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"xls\" command=\"" DELEGATE_ESC "soffice" DELEGATE_ESC " --convert-to pdf -outdir `dirname " DELEGATE_ESC "%i" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC " 2&gt; " DELEGATE_ESC "%u" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.pdf" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"xlsx\" command=\"" DELEGATE_ESC "soffice" DELEGATE_ESC " --convert-to pdf -outdir `dirname " DELEGATE_ESC "%i" DELEGATE_ESC "` " DELEGATE_ESC "%i" DELEGATE_ESC " 2&gt; " DELEGATE_ESC "%u" DELEGATE_ESC "; mv " DELEGATE_ESC "%i.pdf" DELEGATE_ESC " " DELEGATE_ESC "%o" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"xps:cmyk\" stealth=\"True\" command=\"" DELEGATE_ESC "gxps" DELEGATE_ESC " -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=bmpsep8" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"xps:color\" stealth=\"True\" command=\"" DELEGATE_ESC "gxps" DELEGATE_ESC " -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=ppmraw" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"xps:mono\" stealth=\"True\" command=\"" DELEGATE_ESC "gxps" DELEGATE_ESC " -dQUIET -dSAFER -dBATCH -dNOPAUSE -dNOPROMPT -dMaxBitmap=500000000 -dAlignToPixels=0 -dGridFitTT=2 " DELEGATE_ESC "-sDEVICE=pbmraw" DELEGATE_ESC " -dTextAlphaBits=%u -dGraphicsAlphaBits=%u " DELEGATE_ESC "-r%s" DELEGATE_ESC " %s " DELEGATE_ESC "-sOutputFile=%s" DELEGATE_ESC " " DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
    "  <delegate decode=\"video:decode\" command=\"" DELEGATE_ESC "ffmpeg" DELEGATE_ESC " -nostdin -loglevel error -i " DELEGATE_ESC "%s" DELEGATE_ESC " -an -f rawvideo -y %s " DELEGATE_ESC "%s" DELEGATE_ESC "\"/>"
    "  <delegate encode=\"video:encode\" stealth=\"True\" command=\"" DELEGATE_ESC "ffmpeg" DELEGATE_ESC " -nostdin -loglevel error -i " DELEGATE_ESC "%s%%d.%s" DELEGATE_ESC " %s " DELEGATE_ESC "%s.%s" DELEGATE_ESC "\"/>"
    "</delegatemap>";

#undef DELEGATE_ESC

/*
  Global declarations.
*/
static LinkedListInfo
  *delegate_cache = (LinkedListInfo *) NULL;

static SemaphoreInfo
  *delegate_semaphore = (SemaphoreInfo *) NULL;

/*
  Forward declarations.
*/
static MagickBooleanType
  IsDelegateCacheInstantiated(ExceptionInfo *),
  LoadDelegateCache(LinkedListInfo *,const char *,const char *,const size_t,
    ExceptionInfo *);

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%  A c q u i r e D e l e g a t e C a c h e                                    %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  AcquireDelegateCache() caches one or more delegate configurations which
%  provides a mapping between delegate attributes and a delegate name.
%
%  The format of the AcquireDelegateCache method is:
%
%      LinkedListInfo *AcquireDelegateCache(const char *filename,
%        ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o filename: the font file name.
%
%    o exception: return any errors or warnings in this structure.
%
*/
static LinkedListInfo *AcquireDelegateCache(const char *filename,
  ExceptionInfo *exception)
{
  LinkedListInfo
    *cache;

  cache=NewLinkedList(0);
#if !MAGICKCORE_ZERO_CONFIGURATION_SUPPORT
  {
    const StringInfo
      *option;

    LinkedListInfo
      *options;

    options=GetConfigureOptions(filename,exception);
    option=(const StringInfo *) GetNextValueInLinkedList(options);
    while (option != (const StringInfo *) NULL)
    {
      (void) LoadDelegateCache(cache,(const char *) GetStringInfoDatum(option),
        GetStringInfoPath(option),0,exception);
      option=(const StringInfo *) GetNextValueInLinkedList(options);
    }
    options=DestroyConfigureOptions(options);
  }
#endif
  if (IsLinkedListEmpty(cache) != MagickFalse)
    (void) LoadDelegateCache(cache,DelegateMap,"built-in",0,exception);
  return(cache);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+   D e l e g a t e C o m p o n e n t G e n e s i s                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  DelegateComponentGenesis() instantiates the delegate component.
%
%  The format of the DelegateComponentGenesis method is:
%
%      MagickBooleanType DelegateComponentGenesis(void)
%
*/
MagickExport MagickBooleanType DelegateComponentGenesis(void)
{
  if (delegate_semaphore == (SemaphoreInfo *) NULL)
    delegate_semaphore=AllocateSemaphoreInfo();
  return(MagickTrue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   D e l e g a t e C o m p o n e n t T e r m i n u s                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  DelegateComponentTerminus() destroys the delegate component.
%
%  The format of the DelegateComponentTerminus method is:
%
%      DelegateComponentTerminus(void)
%
*/

static void *DestroyDelegate(void *delegate_info)
{
  DelegateInfo
    *p;

  p=(DelegateInfo *) delegate_info;
  if (p->path != (char *) NULL)
    p->path=DestroyString(p->path);
  if (p->decode != (char *) NULL)
    p->decode=DestroyString(p->decode);
  if (p->encode != (char *) NULL)
    p->encode=DestroyString(p->encode);
  if (p->commands != (char *) NULL)
    p->commands=DestroyString(p->commands);
  if (p->semaphore != (SemaphoreInfo *) NULL)
    DestroySemaphoreInfo(&p->semaphore);
  p=(DelegateInfo *) RelinquishMagickMemory(p);
  return((void *) NULL);
}

MagickExport void DelegateComponentTerminus(void)
{
  if (delegate_semaphore == (SemaphoreInfo *) NULL)
    ActivateSemaphoreInfo(&delegate_semaphore);
  LockSemaphoreInfo(delegate_semaphore);
  if (delegate_cache != (LinkedListInfo *) NULL)
    delegate_cache=DestroyLinkedList(delegate_cache,DestroyDelegate);
  UnlockSemaphoreInfo(delegate_semaphore);
  DestroySemaphoreInfo(&delegate_semaphore);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+   E x t e r n a l D e l e g a t e C o m m a n d                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ExternalDelegateCommand() executes the specified command and waits until it
%  terminates.  The returned value is the exit status of the command.
%
%  The format of the ExternalDelegateCommand method is:
%
%      int ExternalDelegateCommand(const MagickBooleanType asynchronous,
%        const MagickBooleanType verbose,const char *command,
%        char *message,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o asynchronous: a value other than 0 executes the parent program
%      concurrently with the new child process.
%
%    o verbose: a value other than 0 prints the executed command before it is
%      invoked.
%
%    o command: this string is the command to execute.
%
%    o message: an option buffer to receive any message posted to stdout or
%      stderr.
%
%    o exception: return any errors here.
%
*/
MagickExport int ExternalDelegateCommand(const MagickBooleanType asynchronous,
  const MagickBooleanType verbose,const char *command,char *message,
  ExceptionInfo *exception)
{
  char
    **arguments,
    *sanitize_command;

  int
    number_arguments,
    status;

  PolicyDomain
    domain;

  PolicyRights
    rights;

  ssize_t
    i;

  status=(-1);
  arguments=StringToArgv(command,&number_arguments);
  if (arguments == (char **) NULL)
    return(status);
  if (*arguments[1] == '\0')
    {
      for (i=0; i < (ssize_t) number_arguments; i++)
        arguments[i]=DestroyString(arguments[i]);
      arguments=(char **) RelinquishMagickMemory(arguments);
      return(-1);
    }
  rights=ExecutePolicyRights;
  domain=DelegatePolicyDomain;
  if (IsRightsAuthorized(domain,rights,arguments[1]) == MagickFalse)
    {
      errno=EPERM;
      (void) ThrowMagickException(exception,GetMagickModule(),PolicyError,
        "NotAuthorized","`%s'",arguments[1]);
      for (i=0; i < (ssize_t) number_arguments; i++)
        arguments[i]=DestroyString(arguments[i]);
      arguments=(char **) RelinquishMagickMemory(arguments);
      return(-1);
    }
  if (verbose != MagickFalse)
    {
      (void) FormatLocaleFile(stderr,"%s\n",command);
      (void) fflush(stderr);
    }
  sanitize_command=SanitizeString(command);
  if (asynchronous != MagickFalse)
    (void) ConcatenateMagickString(sanitize_command,"&",MaxTextExtent);
  if (message != (char *) NULL)
    *message='\0';
#if defined(MAGICKCORE_POSIX_SUPPORT)
#if defined(MAGICKCORE_HAVE_POPEN)
  if ((asynchronous == MagickFalse) && (message !=  (char *) NULL))
    {
      char
        buffer[MagickPathExtent];

      FILE
        *file;

      size_t
        offset;

      offset=0;
      file=popen_utf8(sanitize_command,"r");
      if (file == (FILE *) NULL)
        status=system(sanitize_command);
      else
        {
          while (fgets(buffer,(int) sizeof(buffer),file) != NULL)
          {
            size_t
              length;

            length=MagickMin(MagickPathExtent-offset,strlen(buffer)+1);
            if (length > 0)
              {
                (void) CopyMagickString(message+offset,buffer,length);
                offset+=length-1;
              }
          }
          status=pclose(file);
        }
    }
  else
#endif
    {
#if !defined(MAGICKCORE_HAVE_EXECVP)
      status=system(sanitize_command);
#else
      if ((asynchronous != MagickFalse) ||
          (strpbrk(sanitize_command,"&;<>|") != (char *) NULL))
        status=system(sanitize_command);
      else
        {
          pid_t
            child_pid;

          /*
            Call application directly rather than from a shell.
          */
          child_pid=(pid_t) fork();
          if (child_pid == (pid_t) -1)
            status=system(sanitize_command);
          else
            if (child_pid == 0)
              {
                status=execvp(arguments[1],arguments+1);
                _exit(1);
              }
            else
              {
                int
                  child_status;

                pid_t
                  pid;

                child_status=0;
                pid=(pid_t) waitpid(child_pid,&child_status,0);
                if (pid == -1)
                  status=(-1);
                else
                  {
                    if (WIFEXITED(child_status) != 0)
                      status=WEXITSTATUS(child_status);
                    else
                      if (WIFSIGNALED(child_status))
                        status=(-1);
                  }
              }
        }
#endif
    }
#elif defined(MAGICKCORE_WINDOWS_SUPPORT)
  {
    char
      *p;

    /*
      If a command shell is executed we need to change the forward slashes in
      files to a backslash. We need to do this to keep Windows happy when we
      want to 'move' a file.

      TODO: This won't work if one of the delegate parameters has a forward
            slash as a parameter.
    */
    p=strstr(sanitize_command,"cmd.exe /c");
    if (p != (char*) NULL)
      {
        p+=(ptrdiff_t) 10;
        for ( ; *p != '\0'; p++)
          if (*p == '/')
            *p=(*DirectorySeparator);
      }
  }
  status=NTSystemCommand(sanitize_command,message);
#elif defined(vms)
  status=system(sanitize_command);
#else
#  error No suitable system() method.
#endif
  if (status < 0)
    {
      if ((message != (char *) NULL) && (*message != '\0'))
        (void) ThrowMagickException(exception,GetMagickModule(),DelegateError,
          "FailedToExecuteCommand","`%s' (%s)",sanitize_command,message);
      else
        (void) ThrowMagickException(exception,GetMagickModule(),DelegateError,
          "FailedToExecuteCommand","`%s' (%d)",sanitize_command,status);
    }
  sanitize_command=DestroyString(sanitize_command);
  for (i=0; i < (ssize_t) number_arguments; i++)
    arguments[i]=DestroyString(arguments[i]);
  arguments=(char **) RelinquishMagickMemory(arguments);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t D e l e g a t e C o m m a n d                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetDelegateCommand() replaces any embedded formatting characters with the
%  appropriate image attribute and returns the resulting command.
%
%  The format of the GetDelegateCommand method is:
%
%      char *GetDelegateCommand(const ImageInfo *image_info,Image *image,
%        const char *decode,const char *encode,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o command: Method GetDelegateCommand returns the command associated
%      with specified delegate tag.
%
%    o image_info: the image info.
%
%    o image: the image.
%
%    o decode: Specifies the decode delegate we are searching for as a
%      character string.
%
%    o encode: Specifies the encode delegate we are searching for as a
%      character string.
%
%    o exception: return any errors or warnings in this structure.
%
*/

static char *GetMagickPropertyLetter(const ImageInfo *image_info,Image *image,
  const char letter)
{
  char
    value[MaxTextExtent];

  const char
    *string;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  *value='\0';
  string=(const char *) value;
  switch (letter)
  {
    case 'a':
    {
      /*
        Authentication passphrase.
      */
      if (image_info->authenticate != (char *) NULL)
        string=image_info->authenticate;
      break;
    }
    case 'b':
    {
      /*
        Image size read in - in bytes.
      */
      (void) FormatMagickSize(image->extent,MagickFalse,value);
      if (image->extent == 0)
        (void) FormatMagickSize(GetBlobSize(image),MagickFalse,value);
      break;
    }
    case 'd':
    {
      /*
        Directory component of filename.
      */
      GetPathComponent(image->magick_filename,HeadPath,value);
      break;
    }
    case 'e':
    {
      /*
        Filename extension (suffix) of image file.
      */
      GetPathComponent(image->magick_filename,ExtensionPath,value);
      break;
    }
    case 'f':
    {
      /*
        Filename without directory component.
      */
      GetPathComponent(image->magick_filename,TailPath,value);
      break;
    }
    case 'g':
    {
      /*
        Image geometry, canvas and offset  %Wx%H+%X+%Y.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20gx%.20g%+.20g%+.20g",
        (double) image->page.width,(double) image->page.height,
        (double) image->page.x,(double) image->page.y);
      break;
    }
    case 'h':
    {
      /*
        Image height (current).
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        (image->rows != 0 ? image->rows : image->magick_rows));
      break;
    }
    case 'i':
    {
      /*
        Filename last used for image (read or write).
      */
      string=image->filename;
      break;
    }
    case 'm':
    {
      /*
        Image format (file magick).
      */
      string=image->magick;
      break;
    }
    case 'n':
    {
      /*
        Number of images in the list.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        GetImageListLength(image));
      break;
    }
    case 'o':
    {
      /*
        Output Filename - for delegate use only
      */
      string=image_info->filename;
      break;
    }
    case 'p':
    {
      /*
        Image index in current image list -- As 'n' OBSOLETE.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        GetImageIndexInList(image));
      break;
    }
    case 'q':
    {
      /*
        Quantum depth of image in memory.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        MAGICKCORE_QUANTUM_DEPTH);
      break;
    }
    case 'r':
    {
      ColorspaceType
        colorspace;

      /*
        Image storage class and colorspace.
      */
      colorspace=image->colorspace;
      if (SetImageGray(image,&image->exception) != MagickFalse)
        colorspace=GRAYColorspace;
      (void) FormatLocaleString(value,MaxTextExtent,"%s %s %s",
        CommandOptionToMnemonic(MagickClassOptions,(ssize_t)
        image->storage_class),CommandOptionToMnemonic(MagickColorspaceOptions,
        (ssize_t) colorspace),image->matte != MagickFalse ? "Matte" : "" );
      break;
    }
    case 's':
    {
      /*
        Image scene number.
      */
      if (image_info->number_scenes != 0)
        (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
          image_info->scene);
      else
        (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
          image->scene);
      break;
    }
    case 't':
    {
      /*
        Base filename without directory or extension.
      */
      GetPathComponent(image->magick_filename,BasePath,value);
      break;
    }
    case 'u':
    {
      /*
        Unique filename.
      */
      string=image_info->unique;
      break;
    }
    case 'w':
    {
      /*
        Image width (current).
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        (image->columns != 0 ? image->columns : image->magick_columns));
      break;
    }
    case 'x':
    {
      /*
        Image horizontal resolution.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",
        fabs(image->x_resolution) > MagickEpsilon ? image->x_resolution :
        image->units == PixelsPerCentimeterResolution ? DefaultResolution/2.54 :
        DefaultResolution);
      break;
    }
    case 'y':
    {
      /*
        Image vertical resolution.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",
        fabs(image->y_resolution) > MagickEpsilon ? image->y_resolution :
        image->units == PixelsPerCentimeterResolution ? DefaultResolution/2.54 :
        DefaultResolution);
      break;
    }
    case 'z':
    {
      /*
        Image depth.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        image->depth);
      break;
    }
    case 'A':
    {
      /*
        Image alpha channel.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%s",
        CommandOptionToMnemonic(MagickBooleanOptions,(ssize_t) image->matte));
      break;
    }
    case 'C':
    {
      /*
        Image compression method.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%s",
        CommandOptionToMnemonic(MagickCompressOptions,(ssize_t)
          image->compression));
      break;
    }
    case 'D':
    {
      /*
        Image dispose method.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%s",
        CommandOptionToMnemonic(MagickDisposeOptions,(ssize_t) image->dispose));
      break;
    }
    case 'F':
    {

      /*
        Magick filename - filename given incl. coder & read mods.
      */
      (void) CopyMagickString(value,image->magick_filename,MaxTextExtent);
      break;
    }
    case 'G':
    {
      /*
        Image size as geometry = "%wx%h".
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20gx%.20g",(double)
        image->magick_columns,(double) image->magick_rows);
      break;
    }
    case 'H':
    {
      /*
        Layer canvas height.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        image->page.height);
      break;
    }
    case 'I':
    {
      /*
        Image iterations for animations.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        image->iterations);
      break;
    }
    case 'M':
    {
      /*
        Magick filename - filename given incl. coder & read mods.
      */
      string=image->magick_filename;
      break;
    }
    case 'O':
    {
      /*
        Layer canvas offset with sign = "+%X+%Y".
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%+ld%+ld",(long)
        image->page.x,(long) image->page.y);
      break;
    }
    case 'P':
    {
      /*
        Layer canvas page size = "%Wx%H".
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20gx%.20g",(double)
        image->page.width,(double) image->page.height);
      break;
    }
    case '~':
    {
      /*
        BPG Image compression quality.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        (100-(image->quality == 0 ? 42 : image->quality))/2);
      break;
    }
    case 'Q':
    {
      /*
        Image compression quality.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        (image->quality == 0 ? 92 : image->quality));
      break;
    }
    case 'S':
    {
      /*
        Image scenes.
      */
      if (image_info->number_scenes == 0)
        string="2147483647";
      else
        {
          (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
            image_info->scene+image_info->number_scenes);
        }
      break;
    }
    case 'T':
    {
      /*
        Image time delay for animations.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        image->delay);
      break;
    }
    case 'U':
    {
      /*
        Image resolution units.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%s",
        CommandOptionToMnemonic(MagickResolutionOptions,(ssize_t)
          image->units));
      break;
    }
    case 'W':
    {
      /*
        Layer canvas width.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%.20g",(double)
        image->page.width);
      break;
    }
    case 'X':
    {
      /*
        Layer canvas X offset.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%+.20g",(double)
        image->page.x);
      break;
    }
    case 'Y':
    {
      /*
        Layer canvas Y offset.
      */
      (void) FormatLocaleString(value,MaxTextExtent,"%+.20g",(double)
        image->page.y);
      break;
    }
    case 'Z':
    {
      /*
        Zero filename.
      */
      string=image_info->zero;
      break;
    }
    case '@':
    {
      RectangleInfo
        page;

      /*
        Image bounding box.
      */
      page=GetImageBoundingBox(image,&image->exception);
      (void) FormatLocaleString(value,MaxTextExtent,"%.20gx%.20g%+.20g%+.20g",
        (double) page.width,(double) page.height,(double) page.x,(double)
        page.y);
      break;
    }
    case '#':
    {
      /*
        Image signature.
      */
      (void) SignatureImage(image);
      string=GetImageProperty(image,"signature");
      break;
    }
    case '%':
    {
      /*
        Percent escaped.
      */
      string="%";
      break;
    }
  }
  return(SanitizeDelegateString(string));
}

static char *InterpretDelegateProperties(const ImageInfo *image_info,
  Image *image,const char *embed_text)
{
#define ExtendInterpretText(string_length) \
{ \
  size_t length=(string_length); \
  if ((size_t) (q-interpret_text+length+1) >= extent) \
    { \
      extent+=length; \
      interpret_text=(char *) ResizeQuantumMemory(interpret_text,extent+ \
        MaxTextExtent,sizeof(*interpret_text)); \
      if (interpret_text == (char *) NULL) \
        return((char *) NULL); \
      q=interpret_text+strlen(interpret_text); \
   } \
}

#define AppendKeyValue2Text(key,value)\
{ \
  size_t length=strlen(key)+strlen(value)+2; \
  if ((size_t) (q-interpret_text+length+1) >= extent) \
    { \
      extent+=length; \
      interpret_text=(char *) ResizeQuantumMemory(interpret_text,extent+ \
        MaxTextExtent,sizeof(*interpret_text)); \
      if (interpret_text == (char *) NULL) \
        return((char *) NULL); \
      q=interpret_text+strlen(interpret_text); \
     } \
   q+=(ptrdiff_t) FormatLocaleString(q,extent,"%s=%s\n",(key),(value)); \
}

#define AppendString2Text(string) \
{ \
  size_t length=strlen((string)); \
  if ((size_t) (q-interpret_text+length+1) >= extent) \
    { \
      extent+=length; \
      interpret_text=(char *) ResizeQuantumMemory(interpret_text,extent+ \
        MaxTextExtent,sizeof(*interpret_text)); \
      if (interpret_text == (char *) NULL) \
        return((char *) NULL); \
      q=interpret_text+strlen(interpret_text); \
    } \
  (void) CopyMagickString(q,(string),extent); \
  q+=(ptrdiff_t) length; \
}

  char
    *interpret_text,
    *property;

  char
    *q;  /* current position in interpret_text */

  const char
    *p;  /* position in embed_text string being expanded */

  size_t
    extent;  /* allocated length of interpret_text */

  MagickBooleanType
    number;

  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  if (embed_text == (const char *) NULL)
    return(ConstantString(""));
  p=embed_text;
  while ((isspace((int) ((unsigned char) *p)) != 0) && (*p != '\0'))
    p++;
  if (*p == '\0')
    return(ConstantString(""));
  /*
    Translate any embedded format characters.
  */
  interpret_text=AcquireString(embed_text);  /* new string with extra space */
  extent=MaxTextExtent;  /* how many extra space */
  number=MagickFalse;  /* is last char a number? */
  for (q=interpret_text; *p!='\0';
    number=(isdigit((int) ((unsigned char) *p))) ? MagickTrue : MagickFalse,p++)
  {
    /*
      Interpret escape characters (e.g. Filename: %M).
    */
    *q='\0';
    ExtendInterpretText(MaxTextExtent);
    switch (*p)
    {
      case '\\':
      {
        switch (*(p+1))
        {
          case '\0':
            continue;
          case 'r':  /* convert to RETURN */
          {
            *q++='\r';
            p++;
            continue;
          }
          case 'n':  /* convert to NEWLINE */
          {
            *q++='\n';
            p++;
            continue;
          }
          case '\n':  /* EOL removal UNIX,MacOSX */
          {
            p++;
            continue;
          }
          case '\r':  /* EOL removal DOS,Windows */
          {
            p++;
            if (*p == '\n') /* return-newline EOL */
              p++;
            continue;
          }
          default:
          {
            p++;
            *q++=(*p);
          }
        }
        continue;
      }
      case '&':
      {
        if (LocaleNCompare("&lt;",p,4) == 0)
          {
            *q++='<';
             p+=(ptrdiff_t) 3;
          }
        else
          if (LocaleNCompare("&gt;",p,4) == 0)
            {
              *q++='>';
               p+=(ptrdiff_t) 3;
            }
          else
            if (LocaleNCompare("&amp;",p,5) == 0)
              {
                *q++='&';
                p+=(ptrdiff_t) 4;
              }
            else
              *q++=(*p);
        continue;
      }
      case '%':
        break;  /* continue to next set of handlers */
      default:
      {
        *q++=(*p);  /* any thing else is 'as normal' */
        continue;
      }
    }
    p++; /* advance beyond the percent */
    /*
      Doubled percent - or percent at end of string.
    */
    if ((*p == '\0') || (*p == '\'') || (*p == '"'))
      p--;
    if (*p == '%')
      {
        *q++='%';
        continue;
      }
    /*
      Single letter escapes %c.
    */
    if (number != MagickFalse)
      {
        *q++='%';  /* do NOT substitute the percent */
        p--;  /* back up one */
        continue;
      }
    property=GetMagickPropertyLetter(image_info,image,*p);
    if (property != (char *) NULL)
      {
        AppendString2Text(property);
        property=DestroyString(property);
        continue;
      }
    (void) ThrowMagickException(&image->exception,GetMagickModule(),
      OptionWarning,"UnknownImageProperty","\"%%%c\"",*p);
  }
  *q='\0';
  return(interpret_text);
}

MagickExport char *GetDelegateCommand(const ImageInfo *image_info,Image *image,
  const char *decode,const char *encode,ExceptionInfo *exception)
{
  char
    *command,
    **commands;

  const DelegateInfo
    *delegate_info;

  ssize_t
    i;

  assert(image_info != (ImageInfo *) NULL);
  assert(image_info->signature == MagickCoreSignature);
  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  delegate_info=GetDelegateInfo(decode,encode,exception);
  if (delegate_info == (const DelegateInfo *) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),DelegateError,
        "NoTagFound","`%s'",decode ? decode : encode);
      return((char *) NULL);
    }
  commands=StringToList(delegate_info->commands);
  if (commands == (char **) NULL)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",
        decode ? decode : encode);
      return((char *) NULL);
    }
  command=InterpretDelegateProperties(image_info,image,commands[0]);
  if (command == (char *) NULL)
    (void) ThrowMagickException(exception,GetMagickModule(),ResourceLimitError,
      "MemoryAllocationFailed","`%s'",commands[0]);
  /*
    Relinquish resources.
  */
  for (i=0; commands[i] != (char *) NULL; i++)
    commands[i]=DestroyString(commands[i]);
  commands=(char **) RelinquishMagickMemory(commands);
  return(command);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t D e l e g a t e C o m m a n d s                                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetDelegateCommands() returns the commands associated with a delegate.
%
%  The format of the GetDelegateCommands method is:
%
%      const char *GetDelegateCommands(const DelegateInfo *delegate_info)
%
%  A description of each parameter follows:
%
%    o delegate_info:  The delegate info.
%
*/
MagickExport const char *GetDelegateCommands(const DelegateInfo *delegate_info)
{
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"...");
  assert(delegate_info != (DelegateInfo *) NULL);
  assert(delegate_info->signature == MagickCoreSignature);
  return(delegate_info->commands);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t D e l e g a t e I n f o                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetDelegateInfo() returns any delegates associated with the specified tag.
%
%  The format of the GetDelegateInfo method is:
%
%      const DelegateInfo *GetDelegateInfo(const char *decode,
%        const char *encode,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o decode: Specifies the decode delegate we are searching for as a
%      character string.
%
%    o encode: Specifies the encode delegate we are searching for as a
%      character string.
%
%    o exception: return any errors or warnings in this structure.
%
*/
MagickExport const DelegateInfo *GetDelegateInfo(const char *decode,
  const char *encode,ExceptionInfo *exception)
{
  const DelegateInfo
    *p;

  assert(exception != (ExceptionInfo *) NULL);
  if (IsDelegateCacheInstantiated(exception) == MagickFalse)
    return((const DelegateInfo *) NULL);
  /*
    Search for named delegate.
  */
  LockSemaphoreInfo(delegate_semaphore);
  ResetLinkedListIterator(delegate_cache);
  p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
  if ((LocaleCompare(decode,"*") == 0) && (LocaleCompare(encode,"*") == 0))
    {
      UnlockSemaphoreInfo(delegate_semaphore);
      return(p);
    }
  while (p != (const DelegateInfo *) NULL)
  {
    if (p->mode > 0)
      {
        if (LocaleCompare(p->decode,decode) == 0)
          break;
        p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
        continue;
      }
    if (p->mode < 0)
      {
        if (LocaleCompare(p->encode,encode) == 0)
          break;
        p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
        continue;
      }
    if (LocaleCompare(decode,p->decode) == 0)
      if (LocaleCompare(encode,p->encode) == 0)
        break;
    if (LocaleCompare(decode,"*") == 0)
      if (LocaleCompare(encode,p->encode) == 0)
        break;
    if (LocaleCompare(decode,p->decode) == 0)
      if (LocaleCompare(encode,"*") == 0)
        break;
    p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
  }
  if (p != (const DelegateInfo *) NULL)
    (void) InsertValueInLinkedList(delegate_cache,0,
      RemoveElementByValueFromLinkedList(delegate_cache,p));
  UnlockSemaphoreInfo(delegate_semaphore);
  return(p);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t D e l e g a t e I n f o L i s t                                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetDelegateInfoList() returns any delegates that match the specified pattern.
%
%  The delegate of the GetDelegateInfoList function is:
%
%      const DelegateInfo **GetDelegateInfoList(const char *pattern,
%        size_t *number_delegates,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o pattern: Specifies a pointer to a text string containing a pattern.
%
%    o number_delegates:  This integer returns the number of delegates in the
%      list.
%
%    o exception: return any errors or warnings in this structure.
%
*/

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

static int DelegateInfoCompare(const void *x,const void *y)
{
  const DelegateInfo
    **p,
    **q;

  int
    cmp;

  p=(const DelegateInfo **) x,
  q=(const DelegateInfo **) y;
  cmp=LocaleCompare((*p)->path,(*q)->path);
  if (cmp == 0)
    {
      if ((*p)->decode == (char *) NULL)
        if (((*p)->encode != (char *) NULL) &&
            ((*q)->encode != (char *) NULL))
          return(strcmp((*p)->encode,(*q)->encode));
      if (((*p)->decode != (char *) NULL) &&
          ((*q)->decode != (char *) NULL))
        return(strcmp((*p)->decode,(*q)->decode));
    }
  return(cmp);
}

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

MagickExport const DelegateInfo **GetDelegateInfoList(const char *pattern,
  size_t *number_delegates,ExceptionInfo *exception)
{
  const DelegateInfo
    **delegates;

  const DelegateInfo
    *p;

  ssize_t
    i;

  /*
    Allocate delegate list.
  */
  assert(pattern != (char *) NULL);
  assert(number_delegates != (size_t *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",pattern);
  *number_delegates=0;
  p=GetDelegateInfo("*","*",exception);
  if (p == (const DelegateInfo *) NULL)
    return((const DelegateInfo **) NULL);
  delegates=(const DelegateInfo **) AcquireQuantumMemory((size_t)
    GetNumberOfElementsInLinkedList(delegate_cache)+1UL,sizeof(*delegates));
  if (delegates == (const DelegateInfo **) NULL)
    return((const DelegateInfo **) NULL);
  /*
    Generate delegate list.
  */
  LockSemaphoreInfo(delegate_semaphore);
  ResetLinkedListIterator(delegate_cache);
  p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
  for (i=0; p != (const DelegateInfo *) NULL; )
  {
    if ((p->stealth == MagickFalse) &&
        ((GlobExpression(p->decode,pattern,MagickFalse) != MagickFalse) ||
         (GlobExpression(p->encode,pattern,MagickFalse) != MagickFalse)))
      delegates[i++]=p;
    p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
  }
  UnlockSemaphoreInfo(delegate_semaphore);
  qsort((void *) delegates,(size_t) i,sizeof(*delegates),DelegateInfoCompare);
  delegates[i]=(DelegateInfo *) NULL;
  *number_delegates=(size_t) i;
  return(delegates);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t D e l e g a t e L i s t                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetDelegateList() returns any image format delegates that match the
%  specified  pattern.
%
%  The format of the GetDelegateList function is:
%
%      char **GetDelegateList(const char *pattern,
%        size_t *number_delegates,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o pattern: Specifies a pointer to a text string containing a pattern.
%
%    o number_delegates:  This integer returns the number of delegates
%      in the list.
%
%    o exception: return any errors or warnings in this structure.
%
*/

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

static int DelegateCompare(const void *x,const void *y)
{
  const char
    **p,
    **q;

  p=(const char **) x;
  q=(const char **) y;
  return(LocaleCompare(*p,*q));
}

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

MagickExport char **GetDelegateList(const char *pattern,
  size_t *number_delegates,ExceptionInfo *exception)
{
  char
    **delegates;

  const DelegateInfo
    *p;

  ssize_t
    i;

  /*
    Allocate delegate list.
  */
  assert(pattern != (char *) NULL);
  assert(number_delegates != (size_t *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",pattern);
  *number_delegates=0;
  p=GetDelegateInfo("*","*",exception);
  if (p == (const DelegateInfo *) NULL)
    return((char **) NULL);
  delegates=(char **) AcquireQuantumMemory((size_t)
    GetNumberOfElementsInLinkedList(delegate_cache)+1UL,sizeof(*delegates));
  if (delegates == (char **) NULL)
    return((char **) NULL);
  LockSemaphoreInfo(delegate_semaphore);
  ResetLinkedListIterator(delegate_cache);
  p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
  for (i=0; p != (const DelegateInfo *) NULL; )
  {
    if ((p->stealth == MagickFalse) &&
        (GlobExpression(p->decode,pattern,MagickFalse) != MagickFalse))
      delegates[i++]=ConstantString(p->decode);
    if ((p->stealth == MagickFalse) &&
        (GlobExpression(p->encode,pattern,MagickFalse) != MagickFalse))
      delegates[i++]=ConstantString(p->encode);
    p=(const DelegateInfo *) GetNextValueInLinkedList(delegate_cache);
  }
  UnlockSemaphoreInfo(delegate_semaphore);
  qsort((void *) delegates,(size_t) i,sizeof(*delegates),DelegateCompare);
  delegates[i]=(char *) NULL;
  *number_delegates=(size_t) i;
  return(delegates);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   G e t D e l e g a t e M o d e                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetDelegateMode() returns the mode of the delegate.
%
%  The format of the GetDelegateMode method is:
%
%      ssize_t GetDelegateMode(const DelegateInfo *delegate_info)
%
%  A description of each parameter follows:
%
%    o delegate_info:  The delegate info.
%
*/
MagickExport ssize_t GetDelegateMode(const DelegateInfo *delegate_info)
{
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"...");
  assert(delegate_info != (DelegateInfo *) NULL);
  assert(delegate_info->signature == MagickCoreSignature);
  return(delegate_info->mode);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+   G e t D e l e g a t e T h r e a d S u p p o r t                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  GetDelegateThreadSupport() returns MagickTrue if the delegate supports
%  threads.
%
%  The format of the GetDelegateThreadSupport method is:
%
%      MagickBooleanType GetDelegateThreadSupport(
%        const DelegateInfo *delegate_info)
%
%  A description of each parameter follows:
%
%    o delegate_info:  The delegate info.
%
*/
MagickExport MagickBooleanType GetDelegateThreadSupport(
  const DelegateInfo *delegate_info)
{
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"...");
  assert(delegate_info != (DelegateInfo *) NULL);
  assert(delegate_info->signature == MagickCoreSignature);
  return(delegate_info->thread_support);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+   I s D e l e g a t e C a c h e I n s t a n t i a t e d                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  IsDelegateCacheInstantiated() determines if the delegate cache is
%  instantiated.  If not, it instantiates the cache and returns it.
%
%  The format of the IsDelegateInstantiated method is:
%
%      MagickBooleanType IsDelegateCacheInstantiated(ExceptionInfo *exception)
%
%  A description of each parameter follows.
%
%    o exception: return any errors or warnings in this structure.
%
*/
static MagickBooleanType IsDelegateCacheInstantiated(ExceptionInfo *exception)
{
  if (delegate_cache == (LinkedListInfo *) NULL)
    {
      if (delegate_semaphore == (SemaphoreInfo *) NULL)
        ActivateSemaphoreInfo(&delegate_semaphore);
      LockSemaphoreInfo(delegate_semaphore);
      if (delegate_cache == (LinkedListInfo *) NULL)
        delegate_cache=AcquireDelegateCache(DelegateFilename,exception);
      UnlockSemaphoreInfo(delegate_semaphore);
    }
  return(delegate_cache != (LinkedListInfo *) NULL ? MagickTrue : MagickFalse);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   I n v o k e D e l e g a t e                                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  InvokeDelegate replaces any embedded formatting characters with the
%  appropriate image attribute and executes the resulting command.  MagickFalse
%  is returned if the commands execute with success otherwise MagickTrue.
%
%  The format of the InvokeDelegate method is:
%
%      MagickBooleanType InvokeDelegate(ImageInfo *image_info,Image *image,
%        const char *decode,const char *encode,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image_info: the imageInfo.
%
%    o image: the image.
%
%    o exception: return any errors or warnings in this structure.
%
*/

static MagickBooleanType CopyDelegateFile(const char *source,
  const char *destination,const MagickBooleanType overwrite)
{
  int
    destination_file,
    source_file;

  MagickBooleanType
    status;

  size_t
    i;

  size_t
    length,
    quantum;

  ssize_t
    count;

  struct stat
    attributes;

  unsigned char
    *buffer;

  /*
    Copy source file to destination.
  */
  assert(source != (const char *) NULL);
  assert(destination != (char *) NULL);
  if (overwrite == MagickFalse)
    {
      status=GetPathAttributes(destination,&attributes);
      if (status != MagickFalse)
        return(MagickTrue);
    }
  destination_file=open_utf8(destination,O_WRONLY | O_BINARY | O_CREAT,S_MODE);
  if (destination_file == -1)
    return(MagickFalse);
  source_file=open_utf8(source,O_RDONLY | O_BINARY,0);
  if (source_file == -1)
    {
      (void) close(destination_file);
      return(MagickFalse);
    }
  quantum=(size_t) MagickMaxBufferExtent;
  if ((fstat(source_file,&attributes) == 0) && (attributes.st_size > 0))
    quantum=MagickMin((size_t) attributes.st_size,MagickMaxBufferExtent);
  buffer=(unsigned char *) AcquireQuantumMemory(quantum,sizeof(*buffer));
  if (buffer == (unsigned char *) NULL)
    {
      (void) close(source_file);
      (void) close(destination_file);
      return(MagickFalse);
    }
  length=0;
  for (i=0; ; i+=count)
  {
    count=(ssize_t) read(source_file,buffer,quantum);
    if (count <= 0)
      break;
    length=(size_t) count;
    count=(ssize_t) write(destination_file,buffer,length);
    if ((size_t) count != length)
      break;
  }
  (void) close(destination_file);
  (void) close(source_file);
  buffer=(unsigned char *) RelinquishMagickMemory(buffer);
  return(i != 0 ? MagickTrue : MagickFalse);
}

MagickExport MagickBooleanType InvokeDelegate(ImageInfo *image_info,
  Image *image,const char *decode,const char *encode,ExceptionInfo *exception)
{
  char
    *command,
    **commands,
    input_filename[MaxTextExtent],
    output_filename[MaxTextExtent];

  const DelegateInfo
    *delegate_info;

  MagickBooleanType
    status,
    temporary;

  PolicyRights
    rights;

  ssize_t
    i;

  /*
    Get delegate.
  */
  assert(image_info != (ImageInfo *) NULL);
  assert(image_info->signature == MagickCoreSignature);
  assert(image != (Image *) NULL);
  assert(image->signature == MagickCoreSignature);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",image->filename);
  rights=ExecutePolicyRights;
  if ((decode != (const char *) NULL) &&
      (IsRightsAuthorized(DelegatePolicyDomain,rights,decode) == MagickFalse))
    {
      errno=EPERM;
      (void) ThrowMagickException(exception,GetMagickModule(),PolicyError,
        "NotAuthorized","`%s'",decode);
      return(MagickFalse);
    }
  if ((encode != (const char *) NULL) &&
      (IsRightsAuthorized(DelegatePolicyDomain,rights,encode) == MagickFalse))
    {
      errno=EPERM;
      (void) ThrowMagickException(exception,GetMagickModule(),PolicyError,
        "NotAuthorized","`%s'",encode);
      return(MagickFalse);
    }
  temporary=(*image->filename == '\0') ? MagickTrue : MagickFalse;
  if (temporary != MagickFalse)
    if (AcquireUniqueFilename(image->filename) == MagickFalse)
      {
        ThrowFileException(exception,FileOpenError,
          "UnableToCreateTemporaryFile",image->filename);
        return(MagickFalse);
      }
  delegate_info=GetDelegateInfo(decode,encode,exception);
  if (delegate_info == (DelegateInfo *) NULL)
    {
      if (temporary != MagickFalse)
        (void) RelinquishUniqueFileResource(image->filename);
      (void) ThrowMagickException(exception,GetMagickModule(),DelegateError,
        "NoTagFound","`%s'",decode ? decode : encode);
      return(MagickFalse);
    }
  if (*image_info->filename == '\0')
    {
      if (AcquireUniqueFilename(image_info->filename) == MagickFalse)
        {
          if (temporary != MagickFalse)
            (void) RelinquishUniqueFileResource(image->filename);
          ThrowFileException(exception,FileOpenError,
            "UnableToCreateTemporaryFile",image_info->filename);
          return(MagickFalse);
        }
      image_info->temporary=MagickTrue;
    }
  if ((delegate_info->mode != 0) && (((decode != (const char *) NULL) &&
        (delegate_info->encode != (char *) NULL)) ||
       ((encode != (const char *) NULL) &&
        (delegate_info->decode != (char *) NULL))))
    {
      char
        *magick;

      ImageInfo
        *clone_info;

      Image
        *p;

      /*
        Delegate requires a particular image format.
      */
      if (AcquireUniqueFilename(image_info->unique) == MagickFalse)
        {
          ThrowFileException(exception,FileOpenError,
            "UnableToCreateTemporaryFile",image_info->unique);
          return(MagickFalse);
        }
      if (AcquireUniqueFilename(image_info->zero) == MagickFalse)
        {
          (void) RelinquishUniqueFileResource(image_info->unique);
          ThrowFileException(exception,FileOpenError,
            "UnableToCreateTemporaryFile",image_info->zero);
          return(MagickFalse);
        }
      magick=InterpretDelegateProperties(image_info,image,
        decode != (char *) NULL ? delegate_info->encode :
        delegate_info->decode);
      if (magick == (char *) NULL)
        {
          (void) RelinquishUniqueFileResource(image_info->unique);
          (void) RelinquishUniqueFileResource(image_info->zero);
          if (temporary != MagickFalse)
            (void) RelinquishUniqueFileResource(image->filename);
          (void) ThrowMagickException(exception,GetMagickModule(),
            DelegateError,"DelegateFailed","`%s'",decode ? decode : encode);
          return(MagickFalse);
        }
      LocaleUpper(magick);
      clone_info=CloneImageInfo(image_info);
      (void) CopyMagickString((char *) clone_info->magick,magick,MaxTextExtent);
      if (LocaleCompare(magick,"NULL") != 0)
        (void) CopyMagickString(image->magick,magick,MaxTextExtent);
      magick=DestroyString(magick);
      (void) FormatLocaleString(clone_info->filename,MaxTextExtent,"%s:",
        delegate_info->decode);
      (void) SetImageInfo(clone_info,(unsigned int) GetImageListLength(image),
        exception);
      (void) CopyMagickString(clone_info->filename,image_info->filename,
        MaxTextExtent);
      (void) CopyMagickString(image_info->filename,image->filename,
        MaxTextExtent);
      for (p=image; p != (Image *) NULL; p=GetNextImageInList(p))
      {
        (void) FormatLocaleString(p->filename,MaxTextExtent,"%s:%s",
          delegate_info->decode,clone_info->filename);
        status=WriteImage(clone_info,p);
        if (status == MagickFalse)
          {
            (void) RelinquishUniqueFileResource(image_info->unique);
            (void) RelinquishUniqueFileResource(image_info->zero);
            if (temporary != MagickFalse)
              (void) RelinquishUniqueFileResource(image->filename);
            clone_info=DestroyImageInfo(clone_info);
            (void) ThrowMagickException(exception,GetMagickModule(),
              DelegateError,"DelegateFailed","`%s'",decode ? decode : encode);
            return(MagickFalse);
          }
        if (clone_info->adjoin != MagickFalse)
          break;
      }
      (void) RelinquishUniqueFileResource(image_info->unique);
      (void) RelinquishUniqueFileResource(image_info->zero);
      clone_info=DestroyImageInfo(clone_info);
    }
  /*
    Invoke delegate.
  */
  commands=StringToList(delegate_info->commands);
  if (commands == (char **) NULL)
    {
      if (temporary != MagickFalse)
        (void) RelinquishUniqueFileResource(image->filename);
      (void) ThrowMagickException(exception,GetMagickModule(),
        ResourceLimitError,"MemoryAllocationFailed","`%s'",
        decode ? decode : encode);
      return(MagickFalse);
    }
  command=(char *) NULL;
  status=MagickFalse;
  (void) CopyMagickString(output_filename,image_info->filename,MaxTextExtent);
  (void) CopyMagickString(input_filename,image->filename,MaxTextExtent);
  for (i=0; commands[i] != (char *) NULL; i++)
  {
    status=AcquireUniqueSymbolicLink(output_filename,image_info->filename);
    if (AcquireUniqueFilename(image_info->unique) == MagickFalse)
      {
        ThrowFileException(exception,FileOpenError,
          "UnableToCreateTemporaryFile",image_info->unique);
        break;
      }
    if (AcquireUniqueFilename(image_info->zero) == MagickFalse)
      {
        (void) RelinquishUniqueFileResource(image_info->unique);
        ThrowFileException(exception,FileOpenError,
          "UnableToCreateTemporaryFile",image_info->zero);
        break;
      }
    if (LocaleCompare(decode,"SCAN") != 0)
      {
        status=AcquireUniqueSymbolicLink(input_filename,image->filename);
        if (status == MagickFalse)
          {
            ThrowFileException(exception,FileOpenError,
              "UnableToCreateTemporaryFile",input_filename);
            break;
          }
      }
    status=MagickFalse;
    command=InterpretDelegateProperties(image_info,image,commands[i]);
    if (command != (char *) NULL)
      {
        /*
          Execute delegate.
        */
        status=ExternalDelegateCommand(delegate_info->spawn,image_info->verbose,
          command,(char *) NULL,exception) != 0 ? MagickTrue : MagickFalse;
        if (delegate_info->spawn != MagickFalse)
          {
            ssize_t
              count;

            /*
              Wait for input file to 'disappear', or maximum 2 seconds.
            */
            count=20;
            while ((count-- > 0) && (access_utf8(image->filename,F_OK) == 0))
              (void) MagickDelay(100);  /* sleep 0.1 seconds */
          }
        command=DestroyString(command);
      }
    if (LocaleCompare(decode,"SCAN") != 0)
      {
        if (CopyDelegateFile(image->filename,input_filename,MagickFalse) == MagickFalse)
          (void) RelinquishUniqueFileResource(input_filename);
      }
    if ((strcmp(input_filename,output_filename) != 0) &&
        (CopyDelegateFile(image_info->filename,output_filename,MagickTrue) == MagickFalse))
      (void) RelinquishUniqueFileResource(output_filename);
    if (image_info->temporary != MagickFalse)
      (void) RelinquishUniqueFileResource(image_info->filename);
    (void) RelinquishUniqueFileResource(image_info->unique);
    (void) RelinquishUniqueFileResource(image_info->zero);
    (void) RelinquishUniqueFileResource(image_info->filename);
    (void) RelinquishUniqueFileResource(image->filename);
    if (status != MagickFalse)
      {
        (void) ThrowMagickException(exception,GetMagickModule(),DelegateError,
          "DelegateFailed","`%s'",commands[i]);
        break;
      }
    commands[i]=DestroyString(commands[i]);
  }
  (void) CopyMagickString(image_info->filename,output_filename,MaxTextExtent);
  (void) CopyMagickString(image->filename,input_filename,MaxTextExtent);
  /*
    Relinquish resources.
  */
  for ( ; commands[i] != (char *) NULL; i++)
    commands[i]=DestroyString(commands[i]);
  commands=(char **) RelinquishMagickMemory(commands);
  if (temporary != MagickFalse)
    (void) RelinquishUniqueFileResource(image->filename);
  return(status == MagickFalse ? MagickTrue : MagickFalse);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%  L i s t D e l e g a t e I n f o                                            %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  ListDelegateInfo() lists the image formats to a file.
%
%  The format of the ListDelegateInfo method is:
%
%      MagickBooleanType ListDelegateInfo(FILE *file,ExceptionInfo *exception)
%
%  A description of each parameter follows.
%
%    o file:  An pointer to a FILE.
%
%    o exception: return any errors or warnings in this structure.
%
*/
MagickExport MagickBooleanType ListDelegateInfo(FILE *file,
  ExceptionInfo *exception)
{
  const DelegateInfo
    **delegate_info;

  char
    **commands,
    delegate[MaxTextExtent];

  const char
    *path;

  ssize_t
    i;

  size_t
    number_delegates;

  ssize_t
    j;

  if (file == (const FILE *) NULL)
    file=stdout;
  delegate_info=GetDelegateInfoList("*",&number_delegates,exception);
  if (delegate_info == (const DelegateInfo **) NULL)
    return(MagickFalse);
  path=(const char *) NULL;
  for (i=0; i < (ssize_t) number_delegates; i++)
  {
    if (delegate_info[i]->stealth != MagickFalse)
      continue;
    if ((path == (const char *) NULL) ||
        (LocaleCompare(path,delegate_info[i]->path) != 0))
      {
        if (delegate_info[i]->path != (char *) NULL)
          (void) FormatLocaleFile(file,"\nPath: %s\n\n",delegate_info[i]->path);
        (void) FormatLocaleFile(file,"Delegate                Command\n");
        (void) FormatLocaleFile(file,
          "-------------------------------------------------"
          "------------------------------\n");
      }
    path=delegate_info[i]->path;
    *delegate='\0';
    if (delegate_info[i]->encode != (char *) NULL)
      (void) CopyMagickString(delegate,delegate_info[i]->encode,MaxTextExtent);
    (void) ConcatenateMagickString(delegate,"        ",MaxTextExtent);
    delegate[9]='\0';
    commands=StringToList(delegate_info[i]->commands);
    if (commands == (char **) NULL)
      continue;
    (void) FormatLocaleFile(file,"%11s%c=%c%s  ",delegate_info[i]->decode ?
      delegate_info[i]->decode : "",delegate_info[i]->mode <= 0 ? '<' : ' ',
      delegate_info[i]->mode >= 0 ? '>' : ' ',delegate);
    StripString(commands[0]);
    (void) FormatLocaleFile(file,"\"%s\"\n",commands[0]);
    for (j=1; commands[j] != (char *) NULL; j++)
    {
      StripString(commands[j]);
      (void) FormatLocaleFile(file,"                     \"%s\"\n",commands[j]);
    }
    for (j=0; commands[j] != (char *) NULL; j++)
      commands[j]=DestroyString(commands[j]);
    commands=(char **) RelinquishMagickMemory(commands);
  }
  (void) fflush(file);
  delegate_info=(const DelegateInfo **)
    RelinquishMagickMemory((void *) delegate_info);
  return(MagickTrue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+   L o a d D e l e g a t e L i s t                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  LoadDelegateCache() loads the delegate configurations which provides a
%  mapping between delegate attributes and a delegate name.
%
%  The format of the LoadDelegateCache method is:
%
%      MagickBooleanType LoadDelegateCache(LinkedListInfo *cache,
%        const char *xml,const char *filename,const size_t depth,
%        ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o xml:  The delegate list in XML format.
%
%    o filename:  The delegate list filename.
%
%    o depth: depth of <include /> statements.
%
%    o exception: return any errors or warnings in this structure.
%
*/
static MagickBooleanType LoadDelegateCache(LinkedListInfo *cache,
  const char *xml,const char *filename,const size_t depth,
  ExceptionInfo *exception)
{
  char
    keyword[MaxTextExtent],
    *token;

  const char
    *q;

  DelegateInfo
    *delegate_info;

  MagickStatusType
    status;

  size_t
    extent;

  /*
    Load the delegate map file.
  */
  (void) LogMagickEvent(ConfigureEvent,GetMagickModule(),
    "Loading delegate configuration file \"%s\" ...",filename);
  if (xml == (const char *) NULL)
    return(MagickFalse);
  status=MagickTrue;
  delegate_info=(DelegateInfo *) NULL;
  token=AcquireString(xml);
  extent=strlen(token)+MaxTextExtent;
  for (q=(const char *) xml; *q != '\0'; )
  {
    /*
      Interpret XML.
    */
    (void) GetNextToken(q,&q,extent,token);
    if (*token == '\0')
      break;
    (void) CopyMagickString(keyword,token,MaxTextExtent);
    if (LocaleNCompare(keyword,"<!DOCTYPE",9) == 0)
      {
        /*
          Doctype element.
        */
        while ((LocaleNCompare(q,"]>",2) != 0) && (*q != '\0'))
          (void) GetNextToken(q,&q,extent,token);
        continue;
      }
    if (LocaleNCompare(keyword,"<!--",4) == 0)
      {
        /*
          Comment element.
        */
        while ((LocaleNCompare(q,"->",2) != 0) && (*q != '\0'))
          (void) GetNextToken(q,&q,extent,token);
        continue;
      }
    if (LocaleCompare(keyword,"<include") == 0)
      {
        /*
          Include element.
        */
        while (((*token != '/') && (*(token+1) != '>')) && (*q != '\0'))
        {
          (void) CopyMagickString(keyword,token,MaxTextExtent);
          (void) GetNextToken(q,&q,extent,token);
          if (*token != '=')
            continue;
          (void) GetNextToken(q,&q,extent,token);
          if (LocaleCompare(keyword,"file") == 0)
            {
              if (depth > MagickMaxRecursionDepth)
                (void) ThrowMagickException(exception,GetMagickModule(),
                  ConfigureError,"IncludeElementNestedTooDeeply","`%s'",token);
              else
                {
                  char
                    path[MaxTextExtent],
                    *xml;

                  GetPathComponent(filename,HeadPath,path);
                  if (*path != '\0')
                    (void) ConcatenateMagickString(path,DirectorySeparator,
                      MaxTextExtent);
                  if (*token == *DirectorySeparator)
                    (void) CopyMagickString(path,token,MaxTextExtent);
                  else
                    (void) ConcatenateMagickString(path,token,MaxTextExtent);
                  xml=FileToXML(path,~0UL);
                  if (xml != (char *) NULL)
                    {
                      status&=LoadDelegateCache(cache,xml,path,depth+1,
                        exception);
                      xml=(char *) RelinquishMagickMemory(xml);
                    }
                }
            }
        }
        continue;
      }
    if (LocaleCompare(keyword,"<delegate") == 0)
      {
        /*
          Delegate element.
        */
        delegate_info=(DelegateInfo *) AcquireQuantumMemory(1,
          sizeof(*delegate_info));
        if (delegate_info == (DelegateInfo *) NULL)
          ThrowFatalException(ResourceLimitFatalError,"MemoryAllocationFailed");
        (void) memset(delegate_info,0,sizeof(*delegate_info));
        delegate_info->path=ConstantString(filename);
        delegate_info->thread_support=MagickTrue;
        delegate_info->signature=MagickCoreSignature;
        continue;
      }
    if (delegate_info == (DelegateInfo *) NULL)
      continue;
    if ((LocaleCompare(keyword,"/>") == 0) ||
        (LocaleCompare(keyword,"</policy>") == 0))
      {
        status=AppendValueToLinkedList(cache,delegate_info);
        if (status == MagickFalse)
          (void) ThrowMagickException(exception,GetMagickModule(),
            ResourceLimitError,"MemoryAllocationFailed","`%s'",
            delegate_info->commands);
        delegate_info=(DelegateInfo *) NULL;
        continue;
      }
    (void) GetNextToken(q,(const char **) NULL,extent,token);
    if (*token != '=')
      continue;
    (void) GetNextToken(q,&q,extent,token);
    (void) GetNextToken(q,&q,extent,token);
    switch (*keyword)
    {
      case 'C':
      case 'c':
      {
        if (LocaleCompare((char *) keyword,"command") == 0)
          {
            char
              *commands;

            commands=AcquireString(token);
#if defined(MAGICKCORE_WINDOWS_SUPPORT)
            if (strchr(commands,'@') != (char *) NULL)
              {
                char
                  path[MaxTextExtent];

                NTGhostscriptEXE(path,MaxTextExtent);
                (void) SubstituteString((char **) &commands,"@PSDelegate@",
                  path);
                (void) SubstituteString((char **) &commands,"\\","/");
              }
#endif
            (void) SubstituteString((char **) &commands,"&quot;","\"");
            (void) SubstituteString((char **) &commands,"&apos;","'");
            (void) SubstituteString((char **) &commands,"&amp;","&");
            (void) SubstituteString((char **) &commands,"&gt;",">");
            (void) SubstituteString((char **) &commands,"&lt;","<");
            if (delegate_info->commands != (char *) NULL)
              delegate_info->commands=DestroyString(delegate_info->commands);
            delegate_info->commands=commands;
            break;
          }
        break;
      }
      case 'D':
      case 'd':
      {
        if (LocaleCompare((char *) keyword,"decode") == 0)
          {
            delegate_info->decode=ConstantString(token);
            delegate_info->mode=1;
            break;
          }
        break;
      }
      case 'E':
      case 'e':
      {
        if (LocaleCompare((char *) keyword,"encode") == 0)
          {
            delegate_info->encode=ConstantString(token);
            delegate_info->mode=(-1);
            break;
          }
        break;
      }
      case 'M':
      case 'm':
      {
        if (LocaleCompare((char *) keyword,"mode") == 0)
          {
            delegate_info->mode=1;
            if (LocaleCompare(token,"bi") == 0)
              delegate_info->mode=0;
            else
              if (LocaleCompare(token,"encode") == 0)
                delegate_info->mode=(-1);
            break;
          }
        break;
      }
      case 'S':
      case 's':
      {
        if (LocaleCompare((char *) keyword,"spawn") == 0)
          {
            delegate_info->spawn=IsMagickTrue(token);
            break;
          }
        if (LocaleCompare((char *) keyword,"stealth") == 0)
          {
            delegate_info->stealth=IsMagickTrue(token);
            break;
          }
        break;
      }
      case 'T':
      case 't':
      {
        if (LocaleCompare((char *) keyword,"thread-support") == 0)
          {
            delegate_info->thread_support=IsMagickTrue(token);
            if (delegate_info->thread_support == MagickFalse)
              delegate_info->semaphore=AllocateSemaphoreInfo();
            break;
          }
        break;
      }
      default:
        break;
    }
  }
  token=(char *) RelinquishMagickMemory(token);
  return(status != 0 ? MagickTrue : MagickFalse);
}
