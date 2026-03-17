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
%                         Image Comparison Methods                            %
%                                                                             %
%                              Software Design                                %
%                                   Cristy                                    %
%                               December 2003                                 %
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
%  Use the compare program to mathematically and visually annotate the
%  difference between an image and its reconstruction.
%
*/

/*
  Include declarations.
*/
#include "wand/studio.h"
#include "wand/MagickWand.h"
#include "wand/mogrify-private.h"
#include "magick/compare-private.h"
#include "magick/image-private.h"
#include "magick/string-private.h"

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   C o m p a r e I m a g e C o m m a n d                                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  CompareImageCommand() compares two images and returns the difference between
%  them as a distortion metric and as a new image visually annotating their
%  differences.
%
%  The format of the CompareImageCommand method is:
%
%      MagickBooleanType CompareImageCommand(ImageInfo *image_info,int argc,
%        char **argv,char **metadata,ExceptionInfo *exception)
%
%  A description of each parameter follows:
%
%    o image_info: the image info.
%
%    o argc: the number of elements in the argument vector.
%
%    o argv: A text array containing the command line arguments.
%
%    o metadata: any metadata is returned here.
%
%    o exception: return any errors or warnings in this structure.
%
*/

static MagickBooleanType CompareUsage(void)
{
  static const char
    miscellaneous[] =
      "  -debug events        display copious debugging information\n"
      "  -help                print program options\n"
      "  -list type           print a list of supported option arguments\n"
      "  -log format          format of debugging information",
    operators[] =
      "  -brightness-contrast geometry\n"
      "                       improve brightness / contrast of the image\n"
      "  -distort method args\n"
      "                       distort images according to given method and args\n"
      "  -level value         adjust the level of image contrast\n"
      "  -resize geometry     resize the image\n"
      "  -rotate degrees      apply Paeth rotation to the image\n"
      "  -sigmoidal-contrast geometry\n"
      "                       increase the contrast without saturating highlights or\n"
      "  -trim                trim image edges",
    sequence_operators[] =
      "  -crop geometry       cut out a rectangular region of the image\n"
      "  -separate            separate an image channel into a grayscale image\n"
      "  -write filename      write images to this file",
    settings[] =
      "  -adjoin              join images into a single multi-image file\n"
      "  -alpha option        on, activate, off, deactivate, set, opaque, copy\n"
      "                       transparent, extract, background, or shape\n"
      "  -authenticate password\n"
      "                       decipher image with this password\n"
      "  -background color    background color\n"
      "  -channel type        apply option to select image channels\n"
      "  -colorspace type     alternate image colorspace\n"
      "  -compose operator    set image composite operator\n"
      "  -compress type       type of pixel compression when writing the image\n"
      "  -decipher filename   convert cipher pixels to plain pixels\n"
      "  -define format:option\n"
      "                       define one or more image format options\n"
      "  -density geometry    horizontal and vertical density of the image\n"
      "  -depth value         image depth\n"
      "  -dissimilarity-threshold value\n"
      "                       maximum distortion for (sub)image match\n"
      "  -encipher filename   convert plain pixels to cipher pixels\n"
      "  -extract geometry    extract area from image\n"
      "  -format \"string\"     output formatted image characteristics\n"
      "  -fuzz distance       colors within this distance are considered equal\n"
      "  -gravity type        horizontal and vertical text placement\n"
      "  -highlight-color color\n"
      "                       emphasize pixel differences with this color\n"
      "  -identify            identify the format and characteristics of the image\n"
      "  -interlace type      type of image interlacing scheme\n"
      "  -limit type value    pixel cache resource limit\n"
      "  -lowlight-color color\n"
      "                       de-emphasize pixel differences with this color\n"
      "  -mask filename       associate a mask with the image\n"
      "  -metric type         measure differences between images with this metric\n"
      "  -monitor             monitor progress\n"
      "  -passphrase filename get the passphrase from this file\n"
      "  -precision value     maximum number of significant digits to print\n"
      "  -profile filename    add, delete, or apply an image profile\n"
      "  -quality value       JPEG/MIFF/PNG compression level\n"
      "  -quiet               suppress all warning messages\n"
      "  -quantize colorspace reduce colors in this colorspace\n"
      "  -regard-warnings     pay attention to warning messages\n"
      "  -repage geometry     size and location of an image canvas\n"
      "  -respect-parentheses settings remain in effect until parenthesis boundary\n"
      "  -sampling-factor geometry\n"
      "                       horizontal and vertical sampling factor\n"
      "  -seed value          seed a new sequence of pseudo-random numbers\n"
      "  -set attribute value set an image attribute\n"
      "  -quality value       JPEG/MIFF/PNG compression level\n"
      "  -similarity-threshold value\n"
      "                       minimum distortion for (sub)image match\n"
      "  -size geometry       width and height of image\n"
      "  -subimage-search     search for subimage\n"
      "  -synchronize         synchronize image to storage device\n"
      "  -taint               declare the image as modified\n"
      "  -transparent-color color\n"
      "                       transparent color\n"
      "  -type type           image type\n"
      "  -verbose             print detailed information about the image\n"
      "  -version             print version information\n"
      "  -virtual-pixel method\n"
      "                       virtual pixel access method",
    stack_operators[] =
      "  -delete indexes      delete the image from the image sequence";

  ListMagickVersion(stdout);
  (void) printf("Usage: %s [options ...] image reconstruct difference\n",
    GetClientName());
  (void) printf("\nImage Settings:\n");
  (void) puts(settings);
  (void) printf("\nImage Operators:\n");
  (void) puts(operators);
  (void) printf("\nImage Sequence Operators:\n");
  (void) puts(sequence_operators);
  (void) printf("\nImage Stack Operators:\n");
  (void) puts(stack_operators);
  (void) printf("\nMiscellaneous Options:\n");
  (void) puts(miscellaneous);
  (void) printf(
    "\nBy default, the image format of `file' is determined by its magic\n");
  (void) printf(
    "number.  To specify a particular image format, precede the filename\n");
  (void) printf(
    "with an image format name and a colon (i.e. ps:image) or specify the\n");
  (void) printf(
    "image type as the filename suffix (i.e. image.ps).  Specify 'file' as\n");
  (void) printf("'-' for standard input or output.\n");
  return(MagickTrue);
}

WandExport MagickBooleanType CompareImageCommand(ImageInfo *image_info,
  int argc,char **argv,char **metadata,ExceptionInfo *exception)
{
#define CompareEpsilon  (1.0e-06)
#define CompareConstantColorException \
  "search metric is unreliable for constant-color images"
#define CompareEqualSizedException \
  "subimage search metric is unreliable for equal-sized images"
#define DefaultDissimilarityThreshold  (1.0/MagickPI)
#define DestroyCompare() \
{ \
  if (similarity_image != (Image *) NULL) \
    similarity_image=DestroyImageList(similarity_image); \
  if (difference_image != (Image *) NULL) \
    difference_image=DestroyImageList(difference_image); \
  DestroyImageStack(); \
  for (i=0; i < (ssize_t) argc; i++) \
    argv[i]=DestroyString(argv[i]); \
  argv=(char **) RelinquishMagickMemory(argv); \
}
#define ThrowCompareException(asperity,tag,option) \
{ \
  if (exception->severity < (asperity)) \
    (void) ThrowMagickException(exception,GetMagickModule(),asperity,tag, \
      "`%s'",option); \
  DestroyCompare(); \
  return(MagickFalse); \
}
#define ThrowCompareInvalidArgumentException(option,argument) \
{ \
  (void) ThrowMagickException(exception,GetMagickModule(),OptionError, \
    "InvalidArgument","`%s': %s",option,argument); \
  DestroyCompare(); \
  return(MagickFalse); \
}

  ChannelType
    channels = DefaultChannels;

  char
    *filename,
    *option;

  const char
    *format;

  double
    dissimilarity_threshold = DefaultDissimilarityThreshold,
    distortion = 0.0,
    scale = (double) QuantumRange,
    similarity_metric = 0.0,
    similarity_threshold = DefaultSimilarityThreshold;

  Image
    *difference_image,
    *image = (Image *) NULL,
    *reconstruct_image,
    *similarity_image;

  ImageInfo
    *restore_info;

  ImageStack
    image_stack[MaxImageStackDepth+1];

  MagickBooleanType
    fire,
    pend,
    respect_parenthesis,
    similar = MagickTrue,
    subimage_search;

  MagickStatusType
    status;

  MetricType
    metric = UndefinedErrorMetric;

  RectangleInfo
    offset;

  ssize_t
    i;

  ssize_t
    j,
    k;

  /*
    Set defaults.
  */
  assert(image_info != (ImageInfo *) NULL);
  assert(image_info->signature == MagickCoreSignature);
  assert(exception != (ExceptionInfo *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"...");
  if (argc == 2)
    {
      option=argv[1];
      if ((LocaleCompare("version",option+1) == 0) ||
          (LocaleCompare("-version",option+1) == 0))
        {
          ListMagickVersion(stdout);
          return(MagickTrue);
        }
    }
  if (argc < 3)
    {
      (void) ThrowMagickException(exception,GetMagickModule(),OptionError,
        "MissingArgument","%s","");
      (void) CompareUsage();
      return(MagickFalse);
    }
  restore_info=image_info;
  difference_image=NewImageList();
  similarity_image=NewImageList();
  format=(char *) NULL;
  j=1;
  k=0;
  NewImageStack();
  option=(char *) NULL;
  pend=MagickFalse;
  reconstruct_image=NewImageList();
  respect_parenthesis=MagickFalse;
  status=MagickTrue;
  subimage_search=MagickFalse;
  /*
    Compare an image.
  */
  ReadCommandlLine(argc,&argv);
  status=ExpandFilenames(&argc,&argv);
  if (status == MagickFalse)
    ThrowCompareException(ResourceLimitError,"MemoryAllocationFailed",
      GetExceptionMessage(errno));
  for (i=1; i < ((ssize_t) argc-1); i++)
  {
    option=argv[i];
    if (LocaleCompare(option,"(") == 0)
      {
        FireImageStack(MagickTrue,MagickTrue,pend);
        if (k == MaxImageStackDepth)
          ThrowCompareException(OptionError,"ParenthesisNestedTooDeeply",
            option);
        PushImageStack();
        continue;
      }
    if (LocaleCompare(option,")") == 0)
      {
        FireImageStack(MagickTrue,MagickTrue,MagickTrue);
        if (k == 0)
          ThrowCompareException(OptionError,"UnableToParseExpression",option);
        PopImageStack();
        continue;
      }
    if (IsCommandOption(option) == MagickFalse)
      {
        Image
          *images;

        /*
          Read input image.
        */
        FireImageStack(MagickFalse,MagickFalse,pend);
        filename=argv[i];
        if ((LocaleCompare(filename,"--") == 0) && (i < ((ssize_t) argc-1)))
          filename=argv[++i];
        (void) SetImageOption(image_info,"filename",filename);
        (void) CopyMagickString(image_info->filename,filename,MaxTextExtent);
        images=ReadImages(image_info,exception);
        status&=(images != (Image *) NULL) &&
          (exception->severity < ErrorException);
        if (images == (Image *) NULL)
          continue;
        AppendImageStack(images);
        continue;
      }
    pend=image != (Image *) NULL ? MagickTrue : MagickFalse;
    switch (*(option+1))
    {
      case 'a':
      {
        if (LocaleCompare("adjoin",option+1) == 0)
          break;
        if (LocaleCompare("alpha",option+1) == 0)
          {
            ssize_t
              type;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            type=ParseCommandOption(MagickAlphaOptions,MagickFalse,argv[i]);
            if (type < 0)
              ThrowCompareException(OptionError,"UnrecognizedAlphaChannelType",
                argv[i]);
            break;
          }
        if (LocaleCompare("authenticate",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option);
      }
      case 'b':
      {
        if (LocaleCompare("background",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("brightness-contrast",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option);
      }
      case 'c':
      {
        if (LocaleCompare("cache",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("channel",option+1) == 0)
          {
            ssize_t
              channel;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            channel=ParseChannelOption(argv[i]);
            if (channel < 0)
              ThrowCompareException(OptionError,"UnrecognizedChannelType",
                argv[i]);
            channels=(ChannelType) channel;
            break;
          }
        if (LocaleCompare("colorspace",option+1) == 0)
          {
            ssize_t
              colorspace;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            colorspace=ParseCommandOption(MagickColorspaceOptions,MagickFalse,
              argv[i]);
            if (colorspace < 0)
              ThrowCompareException(OptionError,"UnrecognizedColorspace",
                argv[i]);
            break;
          }
        if (LocaleCompare("compose",option+1) == 0)
          {
            ssize_t
              compose;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            compose=ParseCommandOption(MagickComposeOptions,MagickFalse,
              argv[i]);
            if (compose < 0)
              ThrowCompareException(OptionError,"UnrecognizedComposeOperator",
                argv[i]);
            break;
          }
        if (LocaleCompare("compress",option+1) == 0)
          {
            ssize_t
              compress;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            compress=ParseCommandOption(MagickCompressOptions,MagickFalse,
              argv[i]);
            if (compress < 0)
              ThrowCompareException(OptionError,"UnrecognizedImageCompression",
                argv[i]);
            break;
          }
        if (LocaleCompare("concurrent",option+1) == 0)
          break;
        if (LocaleCompare("crop",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'd':
      {
        if (LocaleCompare("debug",option+1) == 0)
          {
            LogEventType
              event_mask;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            event_mask=SetLogEventMask(argv[i]);
            if (event_mask == UndefinedEvents)
              ThrowCompareException(OptionError,"UnrecognizedEventType",
                argv[i]);
            break;
          }
        if (LocaleCompare("decipher",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("define",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (*option == '+')
              {
                const char
                  *define;

                define=GetImageOption(image_info,argv[i]);
                if (define == (const char *) NULL)
                  ThrowCompareException(OptionError,"NoSuchOption",argv[i]);
                break;
              }
            break;
          }
        if (LocaleCompare("delete",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsSceneGeometry(argv[i],MagickFalse) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("density",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("depth",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("dissimilarity-threshold",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            if (*option == '+')
              dissimilarity_threshold=DefaultDissimilarityThreshold;
            else
              dissimilarity_threshold=StringToDouble(argv[i],(char **) NULL);
            break;
          }
        if (LocaleCompare("distort",option+1) == 0)
          {
            ssize_t
              op;

            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            op=ParseCommandOption(MagickDistortOptions,MagickFalse,argv[i]);
            if (op < 0)
              ThrowCompareException(OptionError,"UnrecognizedDistortMethod",
                argv[i]);
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("duration",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'e':
      {
        if (LocaleCompare("encipher",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("extract",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'f':
      {
        if (LocaleCompare("format",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            format=argv[i];
            break;
          }
        if (LocaleCompare("fuzz",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'g':
      {
        if (LocaleCompare("gravity",option+1) == 0)
          {
            ssize_t
              gravity;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            gravity=ParseCommandOption(MagickGravityOptions,MagickFalse,
              argv[i]);
            if (gravity < 0)
              ThrowCompareException(OptionError,"UnrecognizedGravityType",
                argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'h':
      {
        if ((LocaleCompare("help",option+1) == 0) ||
            (LocaleCompare("-help",option+1) == 0))
          {
            DestroyCompare();
            return(CompareUsage());
          }
        if (LocaleCompare("highlight-color",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'i':
      {
        if (LocaleCompare("identify",option+1) == 0)
          break;
        if (LocaleCompare("interlace",option+1) == 0)
          {
            ssize_t
              interlace;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            interlace=ParseCommandOption(MagickInterlaceOptions,MagickFalse,
              argv[i]);
            if (interlace < 0)
              ThrowCompareException(OptionError,"UnrecognizedInterlaceType",
                argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'l':
      {
        if (LocaleCompare("level",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("limit",option+1) == 0)
          {
            char
              *p;

            double
              value;

            ssize_t
              resource;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            resource=ParseCommandOption(MagickResourceOptions,MagickFalse,
              argv[i]);
            if (resource < 0)
              ThrowCompareException(OptionError,"UnrecognizedResourceType",
                argv[i]);
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            value=StringToDouble(argv[i],&p);
            (void) value;
            if ((p == argv[i]) && (LocaleCompare("unlimited",argv[i]) != 0))
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("list",option+1) == 0)
          {
            ssize_t
              list;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            list=ParseCommandOption(MagickListOptions,MagickFalse,argv[i]);
            if (list < 0)
              ThrowCompareException(OptionError,"UnrecognizedListType",argv[i]);
            status=MogrifyImageInfo(image_info,(int) (i-j+1),(const char **)
              argv+j,exception);
            DestroyCompare();
            return(status == 0 ? MagickFalse : MagickTrue);
          }
        if (LocaleCompare("log",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if ((i == (ssize_t) argc) || (strchr(argv[i],'%') == (char *) NULL))
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("lowlight-color",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'm':
      {
        if (LocaleCompare("mask",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("matte",option+1) == 0)
          break;
        if (LocaleCompare("metric",option+1) == 0)
          {
            ssize_t
              type;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            type=ParseCommandOption(MagickMetricOptions,MagickTrue,argv[i]);
            if (type < 0)
              ThrowCompareException(OptionError,"UnrecognizedMetricType",
                argv[i]);
            metric=(MetricType) type;
            break;
          }
        if (LocaleCompare("monitor",option+1) == 0)
          break;
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'p':
      {
        if (LocaleCompare("precision",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("passphrase",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("profile",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'q':
      {
        if (LocaleCompare("quality",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("quantize",option+1) == 0)
          {
            ssize_t
              colorspace;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            colorspace=ParseCommandOption(MagickColorspaceOptions,
              MagickFalse,argv[i]);
            if (colorspace < 0)
              ThrowCompareException(OptionError,"UnrecognizedColorspace",
                argv[i]);
            break;
          }
        if (LocaleCompare("quiet",option+1) == 0)
          break;
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'r':
      {
        if (LocaleCompare("regard-warnings",option+1) == 0)
          break;
        if (LocaleCompare("repage",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("resize",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("rotate",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleNCompare("respect-parentheses",option+1,17) == 0)
          {
            respect_parenthesis=(*option == '-') ? MagickTrue : MagickFalse;
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 's':
      {
        if (LocaleCompare("sampling-factor",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("seed",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("separate",option+1) == 0)
          break;
        if (LocaleCompare("set",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("sigmoidal-contrast",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("similarity-threshold",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            if (*option == '+')
              similarity_threshold=DefaultSimilarityThreshold;
            else
              similarity_threshold=StringToDouble(argv[i],(char **) NULL);
            break;
          }
        if (LocaleCompare("size",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            if (IsGeometry(argv[i]) == MagickFalse)
              ThrowCompareInvalidArgumentException(option,argv[i]);
            break;
          }
        if (LocaleCompare("subimage-search",option+1) == 0)
          {
            if (*option == '+')
              {
                subimage_search=MagickFalse;
                break;
              }
            subimage_search=MagickTrue;
            break;
          }
        if (LocaleCompare("synchronize",option+1) == 0)
          break;
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 't':
      {
        if (LocaleCompare("taint",option+1) == 0)
          break;
        if (LocaleCompare("transparent-color",option+1) == 0)
          {
            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        if (LocaleCompare("trim",option+1) == 0)
          break;
        if (LocaleCompare("type",option+1) == 0)
          {
            ssize_t
              type;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            type=ParseCommandOption(MagickTypeOptions,MagickFalse,argv[i]);
            if (type < 0)
              ThrowCompareException(OptionError,"UnrecognizedImageType",
                argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'v':
      {
        if (LocaleCompare("verbose",option+1) == 0)
          break;
        if ((LocaleCompare("version",option+1) == 0) ||
            (LocaleCompare("-version",option+1) == 0))
          {
            ListMagickVersion(stdout);
            break;
          }
        if (LocaleCompare("virtual-pixel",option+1) == 0)
          {
            ssize_t
              method;

            if (*option == '+')
              break;
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            method=ParseCommandOption(MagickVirtualPixelOptions,MagickFalse,
              argv[i]);
            if (method < 0)
              ThrowCompareException(OptionError,
                "UnrecognizedVirtualPixelMethod",argv[i]);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case 'w':
      {
        if (LocaleCompare("write",option+1) == 0)
          {
            i++;
            if (i == (ssize_t) argc)
              ThrowCompareException(OptionError,"MissingArgument",option);
            break;
          }
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
      }
      case '?':
        break;
      default:
        ThrowCompareException(OptionError,"UnrecognizedOption",option)
    }
    fire=(GetCommandOptionFlags(MagickCommandOptions,MagickFalse,option) &
      FireOptionFlag) == 0 ?  MagickFalse : MagickTrue;
    if (fire != MagickFalse)
      FireImageStack(MagickTrue,MagickTrue,MagickTrue);
  }
  if (k != 0)
    ThrowCompareException(OptionError,"UnbalancedParenthesis",argv[i]);
  if (i-- != ((ssize_t) argc-1))
    ThrowCompareException(OptionError,"MissingAnImageFilename",argv[i]);
  if ((image == (Image *) NULL) || (GetImageListLength(image) < 2))
    ThrowCompareException(OptionError,"MissingAnImageFilename",argv[i]);
  FinalizeImageSettings(image_info,image,MagickTrue);
  if ((image == (Image *) NULL) || (GetImageListLength(image) < 2))
    ThrowCompareException(OptionError,"MissingAnImageFilename",argv[i]);
  image=GetImageFromList(image,0);
  reconstruct_image=GetImageFromList(image,1);
  offset.x=0;
  offset.y=0;
  if (subimage_search != MagickFalse)
    {
      char
        artifact[MaxTextExtent];

      (void) FormatLocaleString(artifact,MaxTextExtent,"%g",
        similarity_threshold);
      (void) SetImageArtifact(image,"compare:similarity-threshold",artifact);
      similarity_image=SimilarityMetricImage(image,reconstruct_image,metric,
        &offset,&similarity_metric,exception);
      if (similarity_metric >= dissimilarity_threshold)
        (void) ThrowMagickException(exception,GetMagickModule(),ImageWarning,
          "ImagesTooDissimilar","`%s'",image->filename);
    }
  if (similarity_image == (Image *) NULL)
    difference_image=CompareImageChannels(image,reconstruct_image,channels,metric,
      &distortion,exception);
  else
    {
      Image
        *composite_image;

      /*
        Determine if reconstructed image is a subimage of the image.
      */
      composite_image=CloneImage(image,0,0,MagickTrue,exception);
      if (composite_image == (Image *) NULL)
        difference_image=CompareImageChannels(image,reconstruct_image,channels,
          metric,&distortion,exception);
      else
        {
          Image
            *distort_image;

          RectangleInfo
            page;

          (void) CompositeImage(composite_image,CopyCompositeOp,
            reconstruct_image,offset.x,offset.y);
          difference_image=CompareImageChannels(image,composite_image,channels,
            metric,&distortion,exception);
          if (difference_image != (Image *) NULL)
            {
              difference_image->page.x=offset.x;
              difference_image->page.y=offset.y;
            }
          composite_image=DestroyImage(composite_image);
          page.width=reconstruct_image->columns;
          page.height=reconstruct_image->rows;
          page.x=offset.x;
          page.y=offset.y;
          distort_image=CropImage(image,&page,exception);
          if (distort_image != (Image *) NULL)
            {
              Image
                *sans_image;

              (void) SetImageArtifact(distort_image,"compare:virtual-pixels",
                "false");
              sans_image=CompareImages(distort_image,reconstruct_image,metric,
                &distortion,exception);
              if (sans_image != (Image *) NULL)
                sans_image=DestroyImage(sans_image);
              distort_image=DestroyImage(distort_image);
            }
        }
      if (difference_image != (Image *) NULL)
        {
          AppendImageToList(&difference_image,similarity_image);
          similarity_image=(Image *) NULL;
        }
    }
  switch (metric)
  {
    case AbsoluteErrorMetric:
    {
      size_t
        columns,
        rows;

      SetImageCompareBounds(image,reconstruct_image,&columns,&rows);
      scale=(double) columns*rows;
      break;
    }
    case NormalizedCrossCorrelationErrorMetric:
    {
      double
        maxima = 0.0,
        minima = 0.0;

      (void) GetImageRange(reconstruct_image,&minima,&maxima,exception);
      if (fabs(maxima-minima) < MagickEpsilon)
        (void) ThrowMagickException(exception,GetMagickModule(),ImageWarning,
          CompareConstantColorException,"(%s)",CommandOptionToMnemonic(
          MagickMetricOptions,(ssize_t) metric));
      break;
    } 
    case PeakAbsoluteErrorMetric:
    {
      if ((subimage_search != MagickFalse) &&
          (image->columns == reconstruct_image->columns) &&
          (image->rows == reconstruct_image->rows))
        (void) ThrowMagickException(exception,GetMagickModule(),ImageWarning,
          CompareEqualSizedException,"(%s)",CommandOptionToMnemonic(
          MagickMetricOptions,(ssize_t) metric));
      break;
    }
    case PerceptualHashErrorMetric:
    {
      if (subimage_search == MagickFalse)
        {
          double
            maxima = 0.0,
            minima = 0.0;

          (void) GetImageRange(reconstruct_image,&minima,&maxima,exception);
          if (fabs(maxima-minima) < MagickEpsilon)
            (void) ThrowMagickException(exception,GetMagickModule(),
              ImageWarning,CompareConstantColorException,"(%s)",
              CommandOptionToMnemonic(MagickMetricOptions,(ssize_t) metric));
        }
      if ((subimage_search != MagickFalse) &&
          (image->columns == reconstruct_image->columns) &&
          (image->rows == reconstruct_image->rows))
        (void) ThrowMagickException(exception,GetMagickModule(),ImageWarning,
          CompareEqualSizedException,"(%s)",CommandOptionToMnemonic(
          MagickMetricOptions,(ssize_t) metric));
      break;
    }
    case PeakSignalToNoiseRatioMetric:
    {
      scale=MagickSafePSNRRecipicol(10.0);
      break;
    }
    default:
      break;
  }
  if (fabs(distortion) > CompareEpsilon)
    similar=MagickFalse;
  if (difference_image == (Image *) NULL)
    status=0;
  else
    {
      if (image_info->verbose != MagickFalse)
        (void) IsImagesEqual(image,reconstruct_image);
      if (*difference_image->magick == '\0')
        (void) CopyMagickString(difference_image->magick,image->magick,
          MaxTextExtent);
      if (image_info->verbose == MagickFalse)
        {
          switch (metric)
          {
            case AbsoluteErrorMetric:
            {
              (void) FormatLocaleFile(stderr,"%.*g (%.*g)",GetMagickPrecision(),
                ceil(scale*distortion),GetMagickPrecision(),distortion);
              break;
            }
            case MeanErrorPerPixelMetric:
            {
              if (subimage_search == MagickFalse)
                {
                  (void) FormatLocaleFile(stderr,"%.*g (%.*g, %.*g)",
                    GetMagickPrecision(),scale*distortion,
                    GetMagickPrecision(),distortion,GetMagickPrecision(),
                    image->error.normalized_maximum_error);
                  break;
                }
              magick_fallthrough;
            }
            default:
            {
              (void) FormatLocaleFile(stderr,"%.*g (%.*g)",GetMagickPrecision(),
                scale*distortion,GetMagickPrecision(),distortion);
              break;
            }
          }
          if (subimage_search != MagickFalse)
            (void) FormatLocaleFile(stderr," @ %.20g,%.20g [%.*g]",
              (double) offset.x,(double) offset.y,GetMagickPrecision(),
              similarity_metric);
        }
      else
        {
          double
            *channel_distortion;

          channel_distortion=GetImageChannelDistortions(image,reconstruct_image,
            metric,&image->exception);
          (void) FormatLocaleFile(stderr,"Image: %s\n",image->filename);
          if ((reconstruct_image->columns != image->columns) ||
              (reconstruct_image->rows != image->rows))
            (void) FormatLocaleFile(stderr,"Offset: %.20g,%.20g\n",(double)
              difference_image->page.x,(double) difference_image->page.y);
          (void) FormatLocaleFile(stderr,"  Channel distortion: %s\n",
            CommandOptionToMnemonic(MagickMetricOptions,(ssize_t) metric));
          switch (metric)
          {
            case FuzzErrorMetric:
            case MeanAbsoluteErrorMetric:
            case MeanSquaredErrorMetric:
            case PeakAbsoluteErrorMetric:
            case RootMeanSquaredErrorMetric:
            case UndefinedErrorMetric:
            {
              switch (image->colorspace)
              {
                case RGBColorspace:
                default:
                {
                  (void) FormatLocaleFile(stderr,"    red: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*channel_distortion[RedChannel],
                    GetMagickPrecision(),channel_distortion[RedChannel]);
                  (void) FormatLocaleFile(stderr,"    green: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*channel_distortion[GreenChannel],
                    GetMagickPrecision(),channel_distortion[GreenChannel]);
                  (void) FormatLocaleFile(stderr,"    blue: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*channel_distortion[BlueChannel],
                    GetMagickPrecision(),channel_distortion[BlueChannel]);
                  if (image->matte != MagickFalse)
                    (void) FormatLocaleFile(stderr,"    alpha: %.*g (%.*g)\n",
                      GetMagickPrecision(),scale*
                      channel_distortion[OpacityChannel],GetMagickPrecision(),
                      channel_distortion[OpacityChannel]);
                  break;
                }
                case CMYKColorspace:
                {
                  (void) FormatLocaleFile(stderr,"    cyan: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*channel_distortion[CyanChannel],
                    GetMagickPrecision(),channel_distortion[CyanChannel]);
                  (void) FormatLocaleFile(stderr,"    magenta: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*
                    channel_distortion[MagentaChannel],GetMagickPrecision(),
                    channel_distortion[MagentaChannel]);
                  (void) FormatLocaleFile(stderr,"    yellow: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*
                    channel_distortion[YellowChannel],GetMagickPrecision(),
                    channel_distortion[YellowChannel]);
                  (void) FormatLocaleFile(stderr,"    black: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*channel_distortion[BlackChannel],
                    GetMagickPrecision(),channel_distortion[BlackChannel]);
                  if (image->matte != MagickFalse)
                    (void) FormatLocaleFile(stderr,"    alpha: %.*g (%.*g)\n",
                      GetMagickPrecision(),scale*
                      channel_distortion[OpacityChannel],GetMagickPrecision(),
                      channel_distortion[OpacityChannel]);
                  break;
                }
                case LinearGRAYColorspace:
                case GRAYColorspace:
                {
                  (void) FormatLocaleFile(stderr,"    gray: %.*g (%.*g)\n",
                    GetMagickPrecision(),scale*channel_distortion[GrayChannel],
                    GetMagickPrecision(),channel_distortion[GrayChannel]);
                  if (image->matte != MagickFalse)
                    (void) FormatLocaleFile(stderr,"    alpha: %.*g (%.*g)\n",
                      GetMagickPrecision(),scale*
                      channel_distortion[OpacityChannel],GetMagickPrecision(),
                      channel_distortion[OpacityChannel]);
                  break;
                }
              }
              (void) FormatLocaleFile(stderr,"    all: %.*g (%.*g)\n",
                GetMagickPrecision(),scale*
                channel_distortion[CompositeChannels],GetMagickPrecision(),
                channel_distortion[CompositeChannels]);
              break;
            }
            case AbsoluteErrorMetric:
            case NormalizedCrossCorrelationErrorMetric:
            case PeakSignalToNoiseRatioMetric:
            case PerceptualHashErrorMetric:
            {
              switch (image->colorspace)
              {
                case RGBColorspace:
                default:
                {
                  (void) FormatLocaleFile(stderr,"    red: %.*g\n",
                    GetMagickPrecision(),channel_distortion[RedChannel]);
                  (void) FormatLocaleFile(stderr,"    green: %.*g\n",
                    GetMagickPrecision(),channel_distortion[GreenChannel]);
                  (void) FormatLocaleFile(stderr,"    blue: %.*g\n",
                    GetMagickPrecision(),channel_distortion[BlueChannel]);
                  if (image->matte != MagickFalse)
                    (void) FormatLocaleFile(stderr,"    alpha: %.*g\n",
                      GetMagickPrecision(),channel_distortion[OpacityChannel]);
                  break;
                }
                case CMYKColorspace:
                {
                  (void) FormatLocaleFile(stderr,"    cyan: %.*g\n",
                    GetMagickPrecision(),channel_distortion[CyanChannel]);
                  (void) FormatLocaleFile(stderr,"    magenta: %.*g\n",
                    GetMagickPrecision(),channel_distortion[MagentaChannel]);
                  (void) FormatLocaleFile(stderr,"    yellow: %.*g\n",
                    GetMagickPrecision(),channel_distortion[YellowChannel]);
                  (void) FormatLocaleFile(stderr,"    black: %.*g\n",
                    GetMagickPrecision(),channel_distortion[BlackChannel]);
                  if (image->matte != MagickFalse)
                    (void) FormatLocaleFile(stderr,"    alpha: %.*g\n",
                      GetMagickPrecision(),channel_distortion[OpacityChannel]);
                  break;
                }
                case LinearGRAYColorspace:
                case GRAYColorspace:
                {
                  (void) FormatLocaleFile(stderr,"    gray: %.*g\n",
                    GetMagickPrecision(),channel_distortion[GrayChannel]);
                  if (image->matte != MagickFalse)
                    (void) FormatLocaleFile(stderr,"    alpha: %.*g\n",
                      GetMagickPrecision(),channel_distortion[OpacityChannel]);
                  break;
                }
              }
              (void) FormatLocaleFile(stderr,"    all: %.*g\n",
                GetMagickPrecision(),channel_distortion[CompositeChannels]);
              break;
            }
            case MeanErrorPerPixelMetric:
            {
              (void) FormatLocaleFile(stderr,"    %.*g (%.*g, %.*g)\n",
                GetMagickPrecision(),channel_distortion[CompositeChannels],
                GetMagickPrecision(),channel_distortion[CompositeChannels],
                GetMagickPrecision(),image->error.normalized_maximum_error);
              break;
            }
          }
          channel_distortion=(double *) RelinquishMagickMemory(
            channel_distortion);
          if (subimage_search != MagickFalse)
            {
              (void) FormatLocaleFile(stderr,"    Offset: %.20g,%.20g\n",
                (double) difference_image->page.x,(double)
                difference_image->page.y);
              (void) FormatLocaleFile(stderr,"    Similarity metric: %.*g\n",
                GetMagickPrecision(),similarity_metric);
              (void) FormatLocaleFile(stderr,"   Similarity threshold: %*g\n",
                GetMagickPrecision(),similarity_threshold);
              (void) FormatLocaleFile(stderr,
                "   Dissimilarity threshold: %*g\n",GetMagickPrecision(),
                dissimilarity_threshold);
            }
        }
      (void) ResetImagePage(difference_image,"0x0+0+0");
      if (difference_image->next != (Image *) NULL)
        (void) ResetImagePage(difference_image->next,"0x0+0+0");
      status&=WriteImages(image_info,difference_image,argv[argc-1],exception);
      if ((metadata != (char **) NULL) && (format != (char *) NULL))
        {
          char
            *text;

          text=InterpretImageProperties(image_info,difference_image,format);
          InheritException(exception,&image->exception);
          if (text == (char *) NULL)
            ThrowCompareException(ResourceLimitError,"MemoryAllocationFailed",
              GetExceptionMessage(errno));
          (void) ConcatenateString(&(*metadata),text);
          text=DestroyString(text);
        }
      difference_image=DestroyImageList(difference_image);
    }
  DestroyCompare();
  image_info=restore_info;
  if (similar == MagickFalse)
    (void) SetImageOption(image_info,"compare:dissimilar","true");
  return(status != 0 ? MagickTrue : MagickFalse);
}
