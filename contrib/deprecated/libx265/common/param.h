/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Deepthi Nandakumar <deepthi@multicorewareinc.com>
 *          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
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

#ifndef X265_PARAM_H
#define X265_PARAM_H

namespace X265_NS {

int   x265_check_params(x265_param *param);
void  x265_print_params(x265_param *param);
void  x265_param_apply_fastfirstpass(x265_param *p);
char* x265_param2string(x265_param *param, int padx, int pady);
int   x265_atoi(const char *str, bool& bError);
double x265_atof(const char *str, bool& bError);
int   parseCpuName(const char *value, bool& bError);
void  setParamAspectRatio(x265_param *p, int width, int height);
void  getParamAspectRatio(x265_param *p, int& width, int& height);
bool  parseLambdaFile(x265_param *param);

/* this table is kept internal to avoid confusion, since log level indices start at -1 */
static const char * const logLevelNames[] = { "none", "error", "warning", "info", "debug", "full", 0 };

#if EXPORT_C_API
#define PARAM_NS
#else
/* declare param functions within private namespace */
void x265_param_free(x265_param *);
x265_param* x265_param_alloc();
void x265_param_default(x265_param *param);
int x265_param_default_preset(x265_param *, const char *preset, const char *tune);
int x265_param_apply_profile(x265_param *, const char *profile);
int x265_param_parse(x265_param *p, const char *name, const char *value);
#define PARAM_NS X265_NS
#endif

#define MAXPARAMSIZE 2000
}

#endif // ifndef X265_PARAM_H
