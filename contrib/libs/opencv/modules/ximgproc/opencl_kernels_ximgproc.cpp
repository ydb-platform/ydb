// This file is auto-generated. Do not edit!

#include "opencv2/core.hpp"
#include "cvconfig.h"
#include "opencl_kernels_ximgproc.hpp"

#ifdef HAVE_OPENCL

namespace cv
{
namespace ocl
{
namespace ximgproc
{

static const char* const moduleName = "ximgproc";

struct cv::ocl::internal::ProgramEntry anisodiff_oclsrc={moduleName, "anisodiff",
"__kernel void anisodiff(__global const uchar * srcptr, int srcstep, int srcoffset,\n"
"__global uchar * dstptr, int dststep, int dstoffset,\n"
"int rows, int cols, __constant float* exptab, float alpha)\n"
"{\n"
"int x = get_global_id(0);\n"
"int y = get_global_id(1);\n"
"if( x < cols && y < rows )\n"
"{\n"
"int yofs = y*dststep + x*3;\n"
"int xofs = y*srcstep + x*3;\n"
"float4 s = 0.f;\n"
"float4 c = (float4)(srcptr[xofs], srcptr[xofs+1], srcptr[xofs+2], 0.f);\n"
"float4 delta, adelta;\n"
"float w;\n"
"#define UPDATE_SUM(xofs1) \\\n"
"delta = (float4)(srcptr[xofs + xofs1], srcptr[xofs + xofs1 + 1], srcptr[xofs + xofs1 + 2], 0.f) - c; \\\n"
"adelta = fabs(delta); \\\n"
"w = exptab[convert_int(adelta.x + adelta.y + adelta.z)]; \\\n"
"s += delta*w\n"
"UPDATE_SUM(3);\n"
"UPDATE_SUM(-3);\n"
"UPDATE_SUM(-srcstep-3);\n"
"UPDATE_SUM(-srcstep);\n"
"UPDATE_SUM(-srcstep+3);\n"
"UPDATE_SUM(srcstep-3);\n"
"UPDATE_SUM(srcstep);\n"
"UPDATE_SUM(srcstep+3);\n"
"s = s*alpha + c;\n"
"uchar4 d = convert_uchar4_sat(convert_int4_rte(s));\n"
"dstptr[yofs] = d.x;\n"
"dstptr[yofs+1] = d.y;\n"
"dstptr[yofs+2] = d.z;\n"
"}\n"
"}\n"
, "98a8dd2bd56f17e8d574f451af9275e4", NULL};

}}}
#endif
