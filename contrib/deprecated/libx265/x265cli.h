/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Min Chen <chenm003@163.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#ifndef X265CLI_H
#define X265CLI_H 1

#include "common.h"
#include "param.h"

#include <getopt.h>

#ifdef __cplusplus
namespace X265_NS {
#endif

static const char short_options[] = "o:D:P:p:f:F:r:I:i:b:s:t:q:m:hwV?";
static const struct option long_options[] =
{
    { "help",                 no_argument, NULL, 'h' },
    { "version",              no_argument, NULL, 'V' },
    { "asm",            required_argument, NULL, 0 },
    { "no-asm",               no_argument, NULL, 0 },
    { "pools",          required_argument, NULL, 0 },
    { "numa-pools",     required_argument, NULL, 0 },
    { "preset",         required_argument, NULL, 'p' },
    { "tune",           required_argument, NULL, 't' },
    { "frame-threads",  required_argument, NULL, 'F' },
    { "no-pmode",             no_argument, NULL, 0 },
    { "pmode",                no_argument, NULL, 0 },
    { "no-pme",               no_argument, NULL, 0 },
    { "pme",                  no_argument, NULL, 0 },
    { "log-level",      required_argument, NULL, 0 },
    { "profile",        required_argument, NULL, 'P' },
    { "level-idc",      required_argument, NULL, 0 },
    { "high-tier",            no_argument, NULL, 0 },
    { "uhd-bd",               no_argument, NULL, 0 },
    { "no-high-tier",         no_argument, NULL, 0 },
    { "allow-non-conformance",no_argument, NULL, 0 },
    { "no-allow-non-conformance",no_argument, NULL, 0 },
    { "csv",            required_argument, NULL, 0 },
    { "csv-log-level",  required_argument, NULL, 0 },
    { "no-cu-stats",          no_argument, NULL, 0 },
    { "cu-stats",             no_argument, NULL, 0 },
    { "y4m",                  no_argument, NULL, 0 },
    { "no-progress",          no_argument, NULL, 0 },
    { "output",         required_argument, NULL, 'o' },
    { "output-depth",   required_argument, NULL, 'D' },
    { "input",          required_argument, NULL, 0 },
    { "input-depth",    required_argument, NULL, 0 },
    { "input-res",      required_argument, NULL, 0 },
    { "input-csp",      required_argument, NULL, 0 },
    { "interlace",      required_argument, NULL, 0 },
    { "no-interlace",         no_argument, NULL, 0 },
    { "fps",            required_argument, NULL, 0 },
    { "seek",           required_argument, NULL, 0 },
    { "frame-skip",     required_argument, NULL, 0 },
    { "frames",         required_argument, NULL, 'f' },
    { "recon",          required_argument, NULL, 'r' },
    { "recon-depth",    required_argument, NULL, 0 },
    { "no-wpp",               no_argument, NULL, 0 },
    { "wpp",                  no_argument, NULL, 0 },
    { "ctu",            required_argument, NULL, 's' },
    { "min-cu-size",    required_argument, NULL, 0 },
    { "max-tu-size",    required_argument, NULL, 0 },
    { "tu-intra-depth", required_argument, NULL, 0 },
    { "tu-inter-depth", required_argument, NULL, 0 },
    { "limit-tu",       required_argument, NULL, 0 },
    { "me",             required_argument, NULL, 0 },
    { "subme",          required_argument, NULL, 'm' },
    { "merange",        required_argument, NULL, 0 },
    { "max-merge",      required_argument, NULL, 0 },
    { "no-temporal-mvp",      no_argument, NULL, 0 },
    { "temporal-mvp",         no_argument, NULL, 0 },
    { "rdpenalty",      required_argument, NULL, 0 },
    { "no-rect",              no_argument, NULL, 0 },
    { "rect",                 no_argument, NULL, 0 },
    { "no-amp",               no_argument, NULL, 0 },
    { "amp",                  no_argument, NULL, 0 },
    { "no-early-skip",        no_argument, NULL, 0 },
    { "early-skip",           no_argument, NULL, 0 },
    { "no-rskip",             no_argument, NULL, 0 },
    { "rskip",                no_argument, NULL, 0 },
    { "no-fast-cbf",          no_argument, NULL, 0 },
    { "fast-cbf",             no_argument, NULL, 0 },
    { "no-tskip",             no_argument, NULL, 0 },
    { "tskip",                no_argument, NULL, 0 },
    { "no-tskip-fast",        no_argument, NULL, 0 },
    { "tskip-fast",           no_argument, NULL, 0 },
    { "cu-lossless",          no_argument, NULL, 0 },
    { "no-cu-lossless",       no_argument, NULL, 0 },
    { "no-constrained-intra", no_argument, NULL, 0 },
    { "constrained-intra",    no_argument, NULL, 0 },
    { "cip",                  no_argument, NULL, 0 },
    { "no-cip",               no_argument, NULL, 0 },
    { "fast-intra",           no_argument, NULL, 0 },
    { "no-fast-intra",        no_argument, NULL, 0 },
    { "no-open-gop",          no_argument, NULL, 0 },
    { "open-gop",             no_argument, NULL, 0 },
    { "keyint",         required_argument, NULL, 'I' },
    { "min-keyint",     required_argument, NULL, 'i' },
    { "scenecut",       required_argument, NULL, 0 },
    { "no-scenecut",          no_argument, NULL, 0 },
    { "scenecut-bias",  required_argument, NULL, 0 },
    { "ctu-info",       required_argument, NULL, 0 },
    { "intra-refresh",        no_argument, NULL, 0 },
    { "rc-lookahead",   required_argument, NULL, 0 },
    { "lookahead-slices", required_argument, NULL, 0 },
    { "lookahead-threads", required_argument, NULL, 0 },
    { "bframes",        required_argument, NULL, 'b' },
    { "bframe-bias",    required_argument, NULL, 0 },
    { "b-adapt",        required_argument, NULL, 0 },
    { "no-b-adapt",           no_argument, NULL, 0 },
    { "no-b-pyramid",         no_argument, NULL, 0 },
    { "b-pyramid",            no_argument, NULL, 0 },
    { "ref",            required_argument, NULL, 0 },
    { "limit-refs",     required_argument, NULL, 0 },
    { "no-limit-modes",       no_argument, NULL, 0 },
    { "limit-modes",          no_argument, NULL, 0 },
    { "no-weightp",           no_argument, NULL, 0 },
    { "weightp",              no_argument, NULL, 'w' },
    { "no-weightb",           no_argument, NULL, 0 },
    { "weightb",              no_argument, NULL, 0 },
    { "crf",            required_argument, NULL, 0 },
    { "crf-max",        required_argument, NULL, 0 },
    { "crf-min",        required_argument, NULL, 0 },
    { "vbv-maxrate",    required_argument, NULL, 0 },
    { "vbv-bufsize",    required_argument, NULL, 0 },
    { "vbv-init",       required_argument, NULL, 0 },
    { "vbv-end",        required_argument, NULL, 0 },
    { "vbv-end-fr-adj", required_argument, NULL, 0 },
    { "bitrate",        required_argument, NULL, 0 },
    { "qp",             required_argument, NULL, 'q' },
    { "aq-mode",        required_argument, NULL, 0 },
    { "aq-strength",    required_argument, NULL, 0 },
    { "rc-grain",             no_argument, NULL, 0 },
    { "no-rc-grain",          no_argument, NULL, 0 },
    { "ipratio",        required_argument, NULL, 0 },
    { "pbratio",        required_argument, NULL, 0 },
    { "qcomp",          required_argument, NULL, 0 },
    { "qpstep",         required_argument, NULL, 0 },
    { "qpmin",          required_argument, NULL, 0 },
    { "qpmax",          required_argument, NULL, 0 },
    { "const-vbv",            no_argument, NULL, 0 },
    { "no-const-vbv",         no_argument, NULL, 0 },
    { "ratetol",        required_argument, NULL, 0 },
    { "cplxblur",       required_argument, NULL, 0 },
    { "qblur",          required_argument, NULL, 0 },
    { "cbqpoffs",       required_argument, NULL, 0 },
    { "crqpoffs",       required_argument, NULL, 0 },
    { "rd",             required_argument, NULL, 0 },
    { "rdoq-level",     required_argument, NULL, 0 },
    { "no-rdoq-level",        no_argument, NULL, 0 },
    { "dynamic-rd",     required_argument, NULL, 0 },
    { "psy-rd",         required_argument, NULL, 0 },
    { "psy-rdoq",       required_argument, NULL, 0 },
    { "no-psy-rd",            no_argument, NULL, 0 },
    { "no-psy-rdoq",          no_argument, NULL, 0 },
    { "rd-refine",            no_argument, NULL, 0 },
    { "no-rd-refine",         no_argument, NULL, 0 },
    { "scaling-list",   required_argument, NULL, 0 },
    { "lossless",             no_argument, NULL, 0 },
    { "no-lossless",          no_argument, NULL, 0 },
    { "no-signhide",          no_argument, NULL, 0 },
    { "signhide",             no_argument, NULL, 0 },
    { "no-lft",               no_argument, NULL, 0 }, /* DEPRECATED */
    { "lft",                  no_argument, NULL, 0 }, /* DEPRECATED */
    { "no-deblock",           no_argument, NULL, 0 },
    { "deblock",        required_argument, NULL, 0 },
    { "no-sao",               no_argument, NULL, 0 },
    { "sao",                  no_argument, NULL, 0 },
    { "no-sao-non-deblock",   no_argument, NULL, 0 },
    { "sao-non-deblock",      no_argument, NULL, 0 },
    { "no-ssim",              no_argument, NULL, 0 },
    { "ssim",                 no_argument, NULL, 0 },
    { "no-psnr",              no_argument, NULL, 0 },
    { "psnr",                 no_argument, NULL, 0 },
    { "hash",           required_argument, NULL, 0 },
    { "no-strong-intra-smoothing", no_argument, NULL, 0 },
    { "strong-intra-smoothing",    no_argument, NULL, 0 },
    { "no-cutree",                 no_argument, NULL, 0 },
    { "cutree",                    no_argument, NULL, 0 },
    { "no-hrd",               no_argument, NULL, 0 },
    { "hrd",                  no_argument, NULL, 0 },
    { "sar",            required_argument, NULL, 0 },
    { "overscan",       required_argument, NULL, 0 },
    { "videoformat",    required_argument, NULL, 0 },
    { "range",          required_argument, NULL, 0 },
    { "colorprim",      required_argument, NULL, 0 },
    { "transfer",       required_argument, NULL, 0 },
    { "colormatrix",    required_argument, NULL, 0 },
    { "chromaloc",      required_argument, NULL, 0 },
    { "display-window", required_argument, NULL, 0 },
    { "crop-rect",      required_argument, NULL, 0 }, /* DEPRECATED */
    { "master-display", required_argument, NULL, 0 },
    { "max-cll",        required_argument, NULL, 0 },
    { "min-luma",       required_argument, NULL, 0 },
    { "max-luma",       required_argument, NULL, 0 },
    { "log2-max-poc-lsb", required_argument, NULL, 8 },
    { "vui-timing-info",      no_argument, NULL, 0 },
    { "no-vui-timing-info",   no_argument, NULL, 0 },
    { "vui-hrd-info",         no_argument, NULL, 0 },
    { "no-vui-hrd-info",      no_argument, NULL, 0 },
    { "opt-qp-pps",           no_argument, NULL, 0 },
    { "no-opt-qp-pps",        no_argument, NULL, 0 },
    { "opt-ref-list-length-pps",         no_argument, NULL, 0 },
    { "no-opt-ref-list-length-pps",      no_argument, NULL, 0 },
    { "opt-cu-delta-qp",      no_argument, NULL, 0 },
    { "no-opt-cu-delta-qp",   no_argument, NULL, 0 },
    { "no-dither",            no_argument, NULL, 0 },
    { "dither",               no_argument, NULL, 0 },
    { "no-repeat-headers",    no_argument, NULL, 0 },
    { "repeat-headers",       no_argument, NULL, 0 },
    { "aud",                  no_argument, NULL, 0 },
    { "no-aud",               no_argument, NULL, 0 },
    { "info",                 no_argument, NULL, 0 },
    { "no-info",              no_argument, NULL, 0 },
    { "zones",          required_argument, NULL, 0 },
    { "qpfile",         required_argument, NULL, 0 },
    { "lambda-file",    required_argument, NULL, 0 },
    { "b-intra",              no_argument, NULL, 0 },
    { "no-b-intra",           no_argument, NULL, 0 },
    { "nr-intra",       required_argument, NULL, 0 },
    { "nr-inter",       required_argument, NULL, 0 },
    { "stats",          required_argument, NULL, 0 },
    { "pass",           required_argument, NULL, 0 },
    { "multi-pass-opt-analysis", no_argument, NULL, 0 },
    { "no-multi-pass-opt-analysis",    no_argument, NULL, 0 },
    { "multi-pass-opt-distortion",     no_argument, NULL, 0 },
    { "no-multi-pass-opt-distortion",  no_argument, NULL, 0 },
    { "slow-firstpass",       no_argument, NULL, 0 },
    { "no-slow-firstpass",    no_argument, NULL, 0 },
    { "multi-pass-opt-rps",   no_argument, NULL, 0 },
    { "no-multi-pass-opt-rps", no_argument, NULL, 0 },
    { "analysis-reuse-mode",  required_argument, NULL, 0 },
    { "analysis-reuse-file",  required_argument, NULL, 0 },
    { "analysis-reuse-level", required_argument, NULL, 0 },
    { "scale-factor",   required_argument, NULL, 0 },
    { "refine-intra",   required_argument, NULL, 0 },
    { "refine-inter",   required_argument, NULL, 0 },
    { "strict-cbr",           no_argument, NULL, 0 },
    { "temporal-layers",      no_argument, NULL, 0 },
    { "no-temporal-layers",   no_argument, NULL, 0 },
    { "qg-size",        required_argument, NULL, 0 },
    { "recon-y4m-exec", required_argument, NULL, 0 },
    { "analyze-src-pics", no_argument, NULL, 0 },
    { "no-analyze-src-pics", no_argument, NULL, 0 },
    { "slices",         required_argument, NULL, 0 },
    { "aq-motion",            no_argument, NULL, 0 },
    { "no-aq-motion",         no_argument, NULL, 0 },
    { "ssim-rd",              no_argument, NULL, 0 },
    { "no-ssim-rd",           no_argument, NULL, 0 },
    { "hdr",                  no_argument, NULL, 0 },
    { "no-hdr",               no_argument, NULL, 0 },
    { "hdr-opt",              no_argument, NULL, 0 },
    { "no-hdr-opt",           no_argument, NULL, 0 },
    { "limit-sao",            no_argument, NULL, 0 },
    { "no-limit-sao",         no_argument, NULL, 0 },
    { "dhdr10-info",    required_argument, NULL, 0 },
    { "dhdr10-opt",           no_argument, NULL, 0},
    { "no-dhdr10-opt",        no_argument, NULL, 0},
    { "refine-mv",            no_argument, NULL, 0 },
    { "no-refine-mv",         no_argument, NULL, 0 },
    { "force-flush",    required_argument, NULL, 0 },
    { "splitrd-skip",         no_argument, NULL, 0 },
    { "no-splitrd-skip",      no_argument, NULL, 0 },
    { "lowpass-dct",          no_argument, NULL, 0 },
    { "refine-mv-type", required_argument, NULL, 0 },
    { "copy-pic",             no_argument, NULL, 0 },
    { "no-copy-pic",          no_argument, NULL, 0 },
    { 0, 0, 0, 0 },
    { 0, 0, 0, 0 },
    { 0, 0, 0, 0 },
    { 0, 0, 0, 0 },
    { 0, 0, 0, 0 }
};

static void printVersion(x265_param *param, const x265_api* api)
{
    x265_log(param, X265_LOG_INFO, "HEVC encoder version %s\n", api->version_str);
    x265_log(param, X265_LOG_INFO, "build info %s\n", api->build_info_str);
}

static void showHelp(x265_param *param)
{
    int level = param->logLevel;

#define OPT(value) (value ? "enabled" : "disabled")
#define H0 printf
#define H1 if (level >= X265_LOG_DEBUG) printf

    H0("\nSyntax: x265 [options] infile [-o] outfile\n");
    H0("    infile can be YUV or Y4M\n");
    H0("    outfile is raw HEVC bitstream\n");
    H0("\nExecutable Options:\n");
    H0("-h/--help                        Show this help text and exit\n");
    H0("-V/--version                     Show version info and exit\n");
    H0("\nOutput Options:\n");
    H0("-o/--output <filename>           Bitstream output file name\n");
    H0("-D/--output-depth 8|10|12        Output bit depth (also internal bit depth). Default %d\n", param->internalBitDepth);
    H0("   --log-level <string>          Logging level: none error warning info debug full. Default %s\n", X265_NS::logLevelNames[param->logLevel + 1]);
    H0("   --no-progress                 Disable CLI progress reports\n");
    H0("   --csv <filename>              Comma separated log file, if csv-log-level > 0 frame level statistics, else one line per run\n");
    H0("   --csv-log-level <integer>     Level of csv logging, if csv-log-level > 0 frame level statistics, else one line per run: 0-2\n");
    H0("\nInput Options:\n");
    H0("   --input <filename>            Raw YUV or Y4M input file name. `-` for stdin\n");
    H1("   --y4m                         Force parsing of input stream as YUV4MPEG2 regardless of file extension\n");
    H0("   --fps <float|rational>        Source frame rate (float or num/denom), auto-detected if Y4M\n");
    H0("   --input-res WxH               Source picture size [w x h], auto-detected if Y4M\n");
    H1("   --input-depth <integer>       Bit-depth of input file. Default 8\n");
    H1("   --input-csp <string>          Chroma subsampling, auto-detected if Y4M\n");
    H1("                                 0 - i400 (4:0:0 monochrome)\n");
    H1("                                 1 - i420 (4:2:0 default)\n");
    H1("                                 2 - i422 (4:2:2)\n");
    H1("                                 3 - i444 (4:4:4)\n");
#if ENABLE_HDR10_PLUS
    H0("   --dhdr10-info <filename>      JSON file containing the Creative Intent Metadata to be encoded as Dynamic Tone Mapping\n");
    H0("   --[no-]dhdr10-opt             Insert tone mapping SEI only for IDR frames and when the tone mapping information changes. Default disabled\n");
#endif
    H0("-f/--frames <integer>            Maximum number of frames to encode. Default all\n");
    H0("   --seek <integer>              First frame to encode\n");
    H1("   --[no-]interlace <bff|tff>    Indicate input pictures are interlace fields in temporal order. Default progressive\n");
    H1("   --dither                      Enable dither if downscaling to 8 bit pixels. Default disabled\n");
    H0("   --[no-]copy-pic               Copy buffers of input picture in frame. Default %s\n", OPT(param->bCopyPicToFrame));
    H0("\nQuality reporting metrics:\n");
    H0("   --[no-]ssim                   Enable reporting SSIM metric scores. Default %s\n", OPT(param->bEnableSsim));
    H0("   --[no-]psnr                   Enable reporting PSNR metric scores. Default %s\n", OPT(param->bEnablePsnr));
    H0("\nProfile, Level, Tier:\n");
    H0("-P/--profile <string>            Enforce an encode profile: main, main10, mainstillpicture\n");
    H0("   --level-idc <integer|float>   Force a minimum required decoder level (as '5.0' or '50')\n");
    H0("   --[no-]high-tier              If a decoder level is specified, this modifier selects High tier of that level\n");
    H0("   --uhd-bd                      Enable UHD Bluray compatibility support\n");
    H0("   --[no-]allow-non-conformance  Allow the encoder to generate profile NONE bitstreams. Default %s\n", OPT(param->bAllowNonConformance));
    H0("\nThreading, performance:\n");
    H0("   --pools <integer,...>         Comma separated thread count per thread pool (pool per NUMA node)\n");
    H0("                                 '-' implies no threads on node, '+' implies one thread per core on node\n");
    H0("-F/--frame-threads <integer>     Number of concurrently encoded frames. 0: auto-determined by core count\n");
    H0("   --[no-]wpp                    Enable Wavefront Parallel Processing. Default %s\n", OPT(param->bEnableWavefront));
    H0("   --[no-]slices <integer>       Enable Multiple Slices feature. Default %d\n", param->maxSlices);
    H0("   --[no-]pmode                  Parallel mode analysis. Default %s\n", OPT(param->bDistributeModeAnalysis));
    H0("   --[no-]pme                    Parallel motion estimation. Default %s\n", OPT(param->bDistributeMotionEstimation));
    H0("   --[no-]asm <bool|int|string>  Override CPU detection. Default: auto\n");
    H0("\nPresets:\n");
    H0("-p/--preset <string>             Trade off performance for compression efficiency. Default medium\n");
    H0("                                 ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow, or placebo\n");
    H0("-t/--tune <string>               Tune the settings for a particular type of source or situation:\n");
    H0("                                 psnr, ssim, grain, zerolatency, fastdecode\n");
    H0("\nQuad-Tree size and depth:\n");
    H0("-s/--ctu <64|32|16>              Maximum CU size (WxH). Default %d\n", param->maxCUSize);
    H0("   --min-cu-size <64|32|16|8>    Minimum CU size (WxH). Default %d\n", param->minCUSize);
    H0("   --max-tu-size <32|16|8|4>     Maximum TU size (WxH). Default %d\n", param->maxTUSize);
    H0("   --tu-intra-depth <integer>    Max TU recursive depth for intra CUs. Default %d\n", param->tuQTMaxIntraDepth);
    H0("   --tu-inter-depth <integer>    Max TU recursive depth for inter CUs. Default %d\n", param->tuQTMaxInterDepth);
    H0("   --limit-tu <0..4>             Enable early exit from TU recursion for inter coded blocks. Default %d\n", param->limitTU);
    H0("\nAnalysis:\n");
    H0("   --rd <1..6>                   Level of RDO in mode decision 1:least....6:full RDO. Default %d\n", param->rdLevel);
    H0("   --[no-]psy-rd <0..5.0>        Strength of psycho-visual rate distortion optimization, 0 to disable. Default %.1f\n", param->psyRd);
    H0("   --[no-]rdoq-level <0|1|2>     Level of RDO in quantization 0:none, 1:levels, 2:levels & coding groups. Default %d\n", param->rdoqLevel);
    H0("   --[no-]psy-rdoq <0..50.0>     Strength of psycho-visual optimization in RDO quantization, 0 to disable. Default %.1f\n", param->psyRdoq);
    H0("   --dynamic-rd <0..4.0>         Strength of dynamic RD, 0 to disable. Default %.2f\n", param->dynamicRd);
    H0("   --[no-]ssim-rd                Enable ssim rate distortion optimization, 0 to disable. Default %s\n", OPT(param->bSsimRd));
    H0("   --[no-]rd-refine              Enable QP based RD refinement for rd levels 5 and 6. Default %s\n", OPT(param->bEnableRdRefine));
    H0("   --[no-]early-skip             Enable early SKIP detection. Default %s\n", OPT(param->bEnableEarlySkip));
    H0("   --[no-]rskip                  Enable early exit from recursion. Default %s\n", OPT(param->bEnableRecursionSkip));
    H1("   --[no-]tskip-fast             Enable fast intra transform skipping. Default %s\n", OPT(param->bEnableTSkipFast));
    H1("   --[no-]splitrd-skip           Enable skipping split RD analysis when sum of split CU rdCost larger than none split CU rdCost for Intra CU. Default %s\n", OPT(param->bEnableSplitRdSkip));
    H1("   --nr-intra <integer>          An integer value in range of 0 to 2000, which denotes strength of noise reduction in intra CUs. Default 0\n");
    H1("   --nr-inter <integer>          An integer value in range of 0 to 2000, which denotes strength of noise reduction in inter CUs. Default 0\n");
    H0("   --ctu-info <integer>          Enable receiving ctu information asynchronously and determine reaction to the CTU information (0, 1, 2, 4, 6) Default 0\n"
       "                                    - 1: force the partitions if CTU information is present\n"
       "                                    - 2: functionality of (1) and reduce qp if CTU information has changed\n"
       "                                    - 4: functionality of (1) and force Inter modes when CTU Information has changed, merge/skip otherwise\n"
       "                                    Enable this option only when planning to invoke the API function x265_encoder_ctu_info to copy ctu-info asynchronously\n");
    H0("\nCoding tools:\n");
    H0("-w/--[no-]weightp                Enable weighted prediction in P slices. Default %s\n", OPT(param->bEnableWeightedPred));
    H0("   --[no-]weightb                Enable weighted prediction in B slices. Default %s\n", OPT(param->bEnableWeightedBiPred));
    H0("   --[no-]cu-lossless            Consider lossless mode in CU RDO decisions. Default %s\n", OPT(param->bCULossless));
    H0("   --[no-]signhide               Hide sign bit of one coeff per TU (rdo). Default %s\n", OPT(param->bEnableSignHiding));
    H1("   --[no-]tskip                  Enable intra 4x4 transform skipping. Default %s\n", OPT(param->bEnableTransformSkip));
    H0("\nTemporal / motion search options:\n");
    H0("   --max-merge <1..5>            Maximum number of merge candidates. Default %d\n", param->maxNumMergeCand);
    H0("   --ref <integer>               max number of L0 references to be allowed (1 .. 16) Default %d\n", param->maxNumReferences);
    H0("   --limit-refs <0|1|2|3>        Limit references per depth (1) or CU (2) or both (3). Default %d\n", param->limitReferences);
    H0("   --me <string>                 Motion search method dia hex umh star full. Default %d\n", param->searchMethod);
    H0("-m/--subme <integer>             Amount of subpel refinement to perform (0:least .. 7:most). Default %d \n", param->subpelRefine);
    H0("   --merange <integer>           Motion search range. Default %d\n", param->searchRange);
    H0("   --[no-]rect                   Enable rectangular motion partitions Nx2N and 2NxN. Default %s\n", OPT(param->bEnableRectInter));
    H0("   --[no-]amp                    Enable asymmetric motion partitions, requires --rect. Default %s\n", OPT(param->bEnableAMP));
    H0("   --[no-]limit-modes            Limit rectangular and asymmetric motion predictions. Default %d\n", param->limitModes);
    H1("   --[no-]temporal-mvp           Enable temporal MV predictors. Default %s\n", OPT(param->bEnableTemporalMvp));
    H0("\nSpatial / intra options:\n");
    H0("   --[no-]strong-intra-smoothing Enable strong intra smoothing for 32x32 blocks. Default %s\n", OPT(param->bEnableStrongIntraSmoothing));
    H0("   --[no-]constrained-intra      Constrained intra prediction (use only intra coded reference pixels) Default %s\n", OPT(param->bEnableConstrainedIntra));
    H0("   --[no-]b-intra                Enable intra in B frames in veryslow presets. Default %s\n", OPT(param->bIntraInBFrames));
    H0("   --[no-]fast-intra             Enable faster search method for angular intra predictions. Default %s\n", OPT(param->bEnableFastIntra));
    H0("   --rdpenalty <0..2>            penalty for 32x32 intra TU in non-I slices. 0:disabled 1:RD-penalty 2:maximum. Default %d\n", param->rdPenalty);
    H0("\nSlice decision options:\n");
    H0("   --[no-]open-gop               Enable open-GOP, allows I slices to be non-IDR. Default %s\n", OPT(param->bOpenGOP));
    H0("-I/--keyint <integer>            Max IDR period in frames. -1 for infinite-gop. Default %d\n", param->keyframeMax);
    H0("-i/--min-keyint <integer>        Scenecuts closer together than this are coded as I, not IDR. Default: auto\n");
    H0("   --no-scenecut                 Disable adaptive I-frame decision\n");
    H0("   --scenecut <integer>          How aggressively to insert extra I-frames. Default %d\n", param->scenecutThreshold);
    H1("   --scenecut-bias <0..100.0>    Bias for scenecut detection. Default %.2f\n", param->scenecutBias);
    H0("   --intra-refresh               Use Periodic Intra Refresh instead of IDR frames\n");
    H0("   --rc-lookahead <integer>      Number of frames for frame-type lookahead (determines encoder latency) Default %d\n", param->lookaheadDepth);
    H1("   --lookahead-slices <0..16>    Number of slices to use per lookahead cost estimate. Default %d\n", param->lookaheadSlices);
    H0("   --lookahead-threads <integer> Number of threads to be dedicated to perform lookahead only. Default %d\n", param->lookaheadThreads);
    H0("-b/--bframes <0..16>             Maximum number of consecutive b-frames. Default %d\n", param->bframes);
    H1("   --bframe-bias <integer>       Bias towards B frame decisions. Default %d\n", param->bFrameBias);
    H0("   --b-adapt <0..2>              0 - none, 1 - fast, 2 - full (trellis) adaptive B frame scheduling. Default %d\n", param->bFrameAdaptive);
    H0("   --[no-]b-pyramid              Use B-frames as references. Default %s\n", OPT(param->bBPyramid));
    H1("   --qpfile <string>             Force frametypes and QPs for some or all frames\n");
    H1("                                 Format of each line: framenumber frametype QP\n");
    H1("                                 QP is optional (none lets x265 choose). Frametypes: I,i,K,P,B,b.\n");
    H1("                                 QPs are restricted by qpmin/qpmax.\n");
    H1("   --force-flush <integer>       Force the encoder to flush frames. Default %d\n", param->forceFlush);
    H1("                                 0 - flush the encoder only when all the input pictures are over.\n");
    H1("                                 1 - flush all the frames even when the input is not over. Slicetype decision may change with this option.\n");
    H1("                                 2 - flush the slicetype decided frames only.\n");
    H0("\nRate control, Adaptive Quantization:\n");
    H0("   --bitrate <integer>           Target bitrate (kbps) for ABR (implied). Default %d\n", param->rc.bitrate);
    H1("-q/--qp <integer>                QP for P slices in CQP mode (implied). --ipratio and --pbration determine other slice QPs\n");
    H0("   --crf <float>                 Quality-based VBR (0-51). Default %.1f\n", param->rc.rfConstant);
    H1("   --[no-]lossless               Enable lossless: bypass transform, quant and loop filters globally. Default %s\n", OPT(param->bLossless));
    H1("   --crf-max <float>             With CRF+VBV, limit RF to this value. Default %f\n", param->rc.rfConstantMax);
    H1("                                 May cause VBV underflows!\n");
    H1("   --crf-min <float>             With CRF+VBV, limit RF to this value. Default %f\n", param->rc.rfConstantMin);
    H1("                                 this specifies a minimum rate factor value for encode!\n");
    H0("   --vbv-maxrate <integer>       Max local bitrate (kbit/s). Default %d\n", param->rc.vbvMaxBitrate);
    H0("   --vbv-bufsize <integer>       Set size of the VBV buffer (kbit). Default %d\n", param->rc.vbvBufferSize);
    H0("   --vbv-init <float>            Initial VBV buffer occupancy (fraction of bufsize or in kbits). Default %.2f\n", param->rc.vbvBufferInit);
    H0("   --vbv-end <float>             Final VBV buffer emptiness (fraction of bufsize or in kbits). Default 0 (disabled)\n");
    H0("   --vbv-end-fr-adj <float>      Frame from which qp has to be adjusted to achieve final decode buffer emptiness. Default 0\n");
    H0("   --pass                        Multi pass rate control.\n"
       "                                   - 1 : First pass, creates stats file\n"
       "                                   - 2 : Last pass, does not overwrite stats file\n"
       "                                   - 3 : Nth pass, overwrites stats file\n");
    H0("   --[no-]multi-pass-opt-analysis   Refine analysis in 2 pass based on analysis information from pass 1\n");
    H0("   --[no-]multi-pass-opt-distortion Use distortion of CTU from pass 1 to refine qp in 2 pass\n");
    H0("   --stats                       Filename for stats file in multipass pass rate control. Default x265_2pass.log\n");
    H0("   --[no-]analyze-src-pics       Motion estimation uses source frame planes. Default disable\n");
    H0("   --[no-]slow-firstpass         Enable a slow first pass in a multipass rate control mode. Default %s\n", OPT(param->rc.bEnableSlowFirstPass));
    H0("   --[no-]strict-cbr             Enable stricter conditions and tolerance for bitrate deviations in CBR mode. Default %s\n", OPT(param->rc.bStrictCbr));
    H0("   --analysis-reuse-mode <string|int>  save - Dump analysis info into file, load - Load analysis buffers from the file. Default %d\n", param->analysisReuseMode);
    H0("   --analysis-reuse-file <filename>    Specify file name used for either dumping or reading analysis data. Deault x265_analysis.dat\n");
    H0("   --analysis-reuse-level <1..10>      Level of analysis reuse indicates amount of info stored/reused in save/load mode, 1:least..10:most. Default %d\n", param->analysisReuseLevel);
    H0("   --refine-mv-type <string>     Reuse MV information received through API call. Supported option is avc. Default disabled - %d\n", param->bMVType);
    H0("   --scale-factor <int>          Specify factor by which input video is scaled down for analysis save mode. Default %d\n", param->scaleFactor);
    H0("   --refine-intra <0..3>         Enable intra refinement for encode that uses analysis-reuse-mode=load.\n"
        "                                    - 0 : Forces both mode and depth from the save encode.\n"
        "                                    - 1 : Functionality of (0) + evaluate all intra modes at min-cu-size's depth when current depth is one smaller than min-cu-size's depth.\n"
        "                                    - 2 : Functionality of (1) + irrespective of size evaluate all angular modes when the save encode decides the best mode as angular.\n"
        "                                    - 3 : Functionality of (1) + irrespective of size evaluate all intra modes.\n"
        "                                Default:%d\n", param->intraRefine);
    H0("   --refine-inter <0..3>         Enable inter refinement for encode that uses analysis-reuse-mode=load.\n"
        "                                    - 0 : Forces both mode and depth from the save encode.\n"
        "                                    - 1 : Functionality of (0) + evaluate all inter modes at min-cu-size's depth when current depth is one smaller than\n"
        "                                          min-cu-size's depth. When save encode decides the current block as skip(for all sizes) evaluate skip/merge.\n"
        "                                    - 2 : Functionality of (1) + irrespective of size restrict the modes evaluated when specific modes are decided as the best mode by the save encode.\n"
        "                                    - 3 : Functionality of (1) + irrespective of size evaluate all inter modes.\n"
        "                                Default:%d\n", param->interRefine);
    H0("   --[no-]refine-mv              Enable mv refinement for load mode. Default %s\n", OPT(param->mvRefine));
    H0("   --aq-mode <integer>           Mode for Adaptive Quantization - 0:none 1:uniform AQ 2:auto variance 3:auto variance with bias to dark scenes. Default %d\n", param->rc.aqMode);
    H0("   --aq-strength <float>         Reduces blocking and blurring in flat and textured areas (0 to 3.0). Default %.2f\n", param->rc.aqStrength);
    H0("   --[no-]aq-motion              Adaptive Quantization based on the relative motion of each CU w.r.t., frame. Default %s\n", OPT(param->bOptCUDeltaQP));
    H0("   --qg-size <int>               Specifies the size of the quantization group (64, 32, 16, 8). Default %d\n", param->rc.qgSize);
    H0("   --[no-]cutree                 Enable cutree for Adaptive Quantization. Default %s\n", OPT(param->rc.cuTree));
    H0("   --[no-]rc-grain               Enable ratecontrol mode to handle grains specifically. turned on with tune grain. Default %s\n", OPT(param->rc.bEnableGrain));
    H1("   --ipratio <float>             QP factor between I and P. Default %.2f\n", param->rc.ipFactor);
    H1("   --pbratio <float>             QP factor between P and B. Default %.2f\n", param->rc.pbFactor);
    H1("   --qcomp <float>               Weight given to predicted complexity. Default %.2f\n", param->rc.qCompress);
    H1("   --qpstep <integer>            The maximum single adjustment in QP allowed to rate control. Default %d\n", param->rc.qpStep);
    H1("   --qpmin <integer>             sets a hard lower limit on QP allowed to ratecontrol. Default %d\n", param->rc.qpMin);
    H1("   --qpmax <integer>             sets a hard upper limit on QP allowed to ratecontrol. Default %d\n", param->rc.qpMax);
    H0("   --[no-]const-vbv              Enable consistent vbv. turned on with tune grain. Default %s\n", OPT(param->rc.bEnableConstVbv));
    H1("   --cbqpoffs <integer>          Chroma Cb QP Offset [-12..12]. Default %d\n", param->cbQpOffset);
    H1("   --crqpoffs <integer>          Chroma Cr QP Offset [-12..12]. Default %d\n", param->crQpOffset);
    H1("   --scaling-list <string>       Specify a file containing HM style quant scaling lists or 'default' or 'off'. Default: off\n");
    H1("   --zones <zone0>/<zone1>/...   Tweak the bitrate of regions of the video\n");
    H1("                                 Each zone is of the form\n");
    H1("                                   <start frame>,<end frame>,<option>\n");
    H1("                                   where <option> is either\n");
    H1("                                       q=<integer> (force QP)\n");
    H1("                                   or  b=<float> (bitrate multiplier)\n");
    H1("   --lambda-file <string>        Specify a file containing replacement values for the lambda tables\n");
    H1("                                 MAX_MAX_QP+1 floats for lambda table, then again for lambda2 table\n");
    H1("                                 Blank lines and lines starting with hash(#) are ignored\n");
    H1("                                 Comma is considered to be white-space\n");
    H0("\nLoop filters (deblock and SAO):\n");
    H0("   --[no-]deblock                Enable Deblocking Loop Filter, optionally specify tC:Beta offsets Default %s\n", OPT(param->bEnableLoopFilter));
    H0("   --[no-]sao                    Enable Sample Adaptive Offset. Default %s\n", OPT(param->bEnableSAO));
    H1("   --[no-]sao-non-deblock        Use non-deblocked pixels, else right/bottom boundary areas skipped. Default %s\n", OPT(param->bSaoNonDeblocked));
    H0("   --[no-]limit-sao              Limit Sample Adaptive Offset types. Default %s\n", OPT(param->bLimitSAO));
    H0("\nVUI options:\n");
    H0("   --sar <width:height|int>      Sample Aspect Ratio, the ratio of width to height of an individual pixel.\n");
    H0("                                 Choose from 0=undef, 1=1:1(\"square\"), 2=12:11, 3=10:11, 4=16:11,\n");
    H0("                                 5=40:33, 6=24:11, 7=20:11, 8=32:11, 9=80:33, 10=18:11, 11=15:11,\n");
    H0("                                 12=64:33, 13=160:99, 14=4:3, 15=3:2, 16=2:1 or custom ratio of <int:int>. Default %d\n", param->vui.aspectRatioIdc);
    H1("   --display-window <string>     Describe overscan cropping region as 'left,top,right,bottom' in pixels\n");
    H1("   --overscan <string>           Specify whether it is appropriate for decoder to show cropped region: undef, show or crop. Default undef\n");
    H0("   --videoformat <string>        Specify video format from undef, component, pal, ntsc, secam, mac. Default undef\n");
    H0("   --range <string>              Specify black level and range of luma and chroma signals as full or limited Default limited\n");
    H0("   --colorprim <string>          Specify color primaries from  bt709, unknown, reserved, bt470m, bt470bg, smpte170m,\n");
    H0("                                 smpte240m, film, bt2020, smpte428, smpte431, smpte432. Default undef\n");
    H0("   --transfer <string>           Specify transfer characteristics from bt709, unknown, reserved, bt470m, bt470bg, smpte170m,\n");
    H0("                                 smpte240m, linear, log100, log316, iec61966-2-4, bt1361e, iec61966-2-1,\n");
    H0("                                 bt2020-10, bt2020-12, smpte2084, smpte428, arib-std-b67. Default undef\n");
    H1("   --colormatrix <string>        Specify color matrix setting from undef, bt709, fcc, bt470bg, smpte170m,\n");
    H1("                                 smpte240m, GBR, YCgCo, bt2020nc, bt2020c, smpte2085, chroma-derived-nc, chroma-derived-c, ictcp. Default undef\n");
    H1("   --chromaloc <integer>         Specify chroma sample location (0 to 5). Default of %d\n", param->vui.chromaSampleLocTypeTopField);
    H0("   --master-display <string>     SMPTE ST 2086 master display color volume info SEI (HDR)\n");
    H0("                                    format: G(x,y)B(x,y)R(x,y)WP(x,y)L(max,min)\n");
    H0("   --max-cll <string>            Emit content light level info SEI as \"cll,fall\" (HDR)\n");
    H0("   --[no-]hdr                    Control dumping of HDR SEI packet. If max-cll or master-display has non-zero values, this is enabled. Default %s\n", OPT(param->bEmitHDRSEI));
    H0("   --[no-]hdr-opt                Add luma and chroma offsets for HDR/WCG content. Default %s\n", OPT(param->bHDROpt));
    H0("   --min-luma <integer>          Minimum luma plane value of input source picture\n");
    H0("   --max-luma <integer>          Maximum luma plane value of input source picture\n");
    H0("\nBitstream options:\n");
    H0("   --[no-]repeat-headers         Emit SPS and PPS headers at each keyframe. Default %s\n", OPT(param->bRepeatHeaders));
    H0("   --[no-]info                   Emit SEI identifying encoder and parameters. Default %s\n", OPT(param->bEmitInfoSEI));
    H0("   --[no-]hrd                    Enable HRD parameters signaling. Default %s\n", OPT(param->bEmitHRDSEI));
    H0("   --[no-]temporal-layers        Enable a temporal sublayer for unreferenced B frames. Default %s\n", OPT(param->bEnableTemporalSubLayers));
    H0("   --[no-]aud                    Emit access unit delimiters at the start of each access unit. Default %s\n", OPT(param->bEnableAccessUnitDelimiters));
    H1("   --hash <integer>              Decoded Picture Hash SEI 0: disabled, 1: MD5, 2: CRC, 3: Checksum. Default %d\n", param->decodedPictureHashSEI);
    H0("   --log2-max-poc-lsb <integer>  Maximum of the picture order count\n");
    H0("   --[no-]vui-timing-info        Emit VUI timing information in the bistream. Default %s\n", OPT(param->bEmitVUITimingInfo));
    H0("   --[no-]vui-hrd-info           Emit VUI HRD information in the bistream. Default %s\n", OPT(param->bEmitVUIHRDInfo));
    H0("   --[no-]opt-qp-pps             Dynamically optimize QP in PPS (instead of default 26) based on QPs in previous GOP. Default %s\n", OPT(param->bOptQpPPS));
    H0("   --[no-]opt-ref-list-length-pps  Dynamically set L0 and L1 ref list length in PPS (instead of default 0) based on values in last GOP. Default %s\n", OPT(param->bOptRefListLengthPPS));
    H0("   --[no-]multi-pass-opt-rps     Enable storing commonly used RPS in SPS in multi pass mode. Default %s\n", OPT(param->bMultiPassOptRPS));
    H0("   --[no-]opt-cu-delta-qp        Optimize to signal consistent CU level delta QPs in frame. Default %s\n", OPT(param->bOptCUDeltaQP));
    H1("\nReconstructed video options (debugging):\n");
    H1("-r/--recon <filename>            Reconstructed raw image YUV or Y4M output file name\n");
    H1("   --recon-depth <integer>       Bit-depth of reconstructed raw image file. Defaults to input bit depth, or 8 if Y4M\n");
    H1("   --recon-y4m-exec <string>     pipe reconstructed frames to Y4M viewer, ex:\"ffplay -i pipe:0 -autoexit\"\n");
    H0("   --lowpass-dct                 Use low-pass subband dct approximation. Default %s\n", OPT(param->bLowPassDct));
    H1("\nExecutable return codes:\n");
    H1("    0 - encode successful\n");
    H1("    1 - unable to parse command line\n");
    H1("    2 - unable to open encoder\n");
    H1("    3 - unable to generate stream headers\n");
    H1("    4 - encoder abort\n");
#undef OPT
#undef H0
#undef H1

    if (level < X265_LOG_DEBUG)
        printf("\nUse --log-level full --help for a full listing\n");
    printf("\n\nComplete documentation may be found at http://x265.readthedocs.org/en/default/cli.html\n");
    exit(1);
}

#ifdef __cplusplus
}
#endif

#endif
