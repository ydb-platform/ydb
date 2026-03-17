/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Deepthi Nandakumar <deepthi@multicorewareinc.com>
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

#include "common.h"
#include "slice.h"
#include "threading.h"
#include "x265.h"

#if _WIN32
#include <sys/types.h>
#include <sys/timeb.h>
#include <io.h>
#include <fcntl.h>
#else
#include <sys/time.h>
#endif

namespace X265_NS {

#if CHECKED_BUILD || _DEBUG
int g_checkFailures;
#endif

int64_t x265_mdate(void)
{
#if _WIN32
    struct timeb tb;
    ftime(&tb);
    return ((int64_t)tb.time * 1000 + (int64_t)tb.millitm) * 1000;
#else
    struct timeval tv_date;
    gettimeofday(&tv_date, NULL);
    return (int64_t)tv_date.tv_sec * 1000000 + (int64_t)tv_date.tv_usec;
#endif
}

#define X265_ALIGNBYTES 32

#if _WIN32
#if defined(__MINGW32__) && !defined(__MINGW64_VERSION_MAJOR)
#define _aligned_malloc __mingw_aligned_malloc
#define _aligned_free   __mingw_aligned_free
#include "malloc.h"
#endif

void *x265_malloc(size_t size)
{
    return _aligned_malloc(size, X265_ALIGNBYTES);
}

void x265_free(void *ptr)
{
    if (ptr) _aligned_free(ptr);
}

#else // if _WIN32
void *x265_malloc(size_t size)
{
    void *ptr;

    if (posix_memalign((void**)&ptr, X265_ALIGNBYTES, size) == 0)
        return ptr;
    else
        return NULL;
}

void x265_free(void *ptr)
{
    if (ptr) free(ptr);
}

#endif // if _WIN32

/* Not a general-purpose function; multiplies input by -1/6 to convert
 * qp to qscale. */
int x265_exp2fix8(double x)
{
    int i = (int)(x * (-64.f / 6.f) + 512.5f);

    if (i < 0) return 0;
    if (i > 1023) return 0xffff;
    return (x265_exp2_lut[i & 63] + 256) << (i >> 6) >> 8;
}

void general_log(const x265_param* param, const char* caller, int level, const char* fmt, ...)
{
    if (param && level > param->logLevel)
        return;
    const int bufferSize = 4096;
    char buffer[bufferSize];
    int p = 0;
    const char* log_level;
    switch (level)
    {
    case X265_LOG_ERROR:
        log_level = "error";
        break;
    case X265_LOG_WARNING:
        log_level = "warning";
        break;
    case X265_LOG_INFO:
        log_level = "info";
        break;
    case X265_LOG_DEBUG:
        log_level = "debug";
        break;
    case X265_LOG_FULL:
        log_level = "full";
        break;
    default:
        log_level = "unknown";
        break;
    }

    if (caller)
        p += sprintf(buffer, "%-4s [%s]: ", caller, log_level);
    va_list arg;
    va_start(arg, fmt);
    vsnprintf(buffer + p, bufferSize - p, fmt, arg);
    va_end(arg);
    fputs(buffer, stderr);
}

#if _WIN32
/* For Unicode filenames in Windows we convert UTF-8 strings to UTF-16 and we use _w functions.
 * For other OS we do not make any changes. */
void general_log_file(const x265_param* param, const char* caller, int level, const char* fmt, ...)
{
    if (param && level > param->logLevel)
        return;
    const int bufferSize = 4096;
    char buffer[bufferSize];
    int p = 0;
    const char* log_level;
    switch (level)
    {
    case X265_LOG_ERROR:
        log_level = "error";
        break;
    case X265_LOG_WARNING:
        log_level = "warning";
        break;
    case X265_LOG_INFO:
        log_level = "info";
        break;
    case X265_LOG_DEBUG:
        log_level = "debug";
        break;
    case X265_LOG_FULL:
        log_level = "full";
        break;
    default:
        log_level = "unknown";
        break;
    }

    if (caller)
        p += sprintf(buffer, "%-4s [%s]: ", caller, log_level);
    va_list arg;
    va_start(arg, fmt);
    vsnprintf(buffer + p, bufferSize - p, fmt, arg);
    va_end(arg);

    HANDLE console = GetStdHandle(STD_ERROR_HANDLE);
    DWORD mode;
    if (GetConsoleMode(console, &mode))
    {
        wchar_t buf_utf16[bufferSize];
        int length_utf16 = MultiByteToWideChar(CP_UTF8, 0, buffer, -1, buf_utf16, sizeof(buf_utf16)/sizeof(wchar_t)) - 1;
        if (length_utf16 > 0)
            WriteConsoleW(console, buf_utf16, length_utf16, &mode, NULL);
    }
    else
        fputs(buffer, stderr);
}

FILE* x265_fopen(const char* fileName, const char* mode)
{
    wchar_t buf_utf16[MAX_PATH * 2], mode_utf16[16];

    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, fileName, -1, buf_utf16, sizeof(buf_utf16)/sizeof(wchar_t)) &&
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, mode, -1, mode_utf16, sizeof(mode_utf16)/sizeof(wchar_t)))
    {
        return _wfopen(buf_utf16, mode_utf16);
    }
    return NULL;
}

int x265_unlink(const char* fileName)
{
    wchar_t buf_utf16[MAX_PATH * 2];

    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, fileName, -1, buf_utf16, sizeof(buf_utf16)/sizeof(wchar_t)))
        return _wunlink(buf_utf16);

    return -1;
}

int x265_rename(const char* oldName, const char* newName)
{
    wchar_t old_utf16[MAX_PATH * 2], new_utf16[MAX_PATH * 2];

    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, oldName, -1, old_utf16, sizeof(old_utf16)/sizeof(wchar_t)) &&
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, newName, -1, new_utf16, sizeof(new_utf16)/sizeof(wchar_t)))
    {
        return _wrename(old_utf16, new_utf16);
    }
    return -1;
}
#endif

double x265_ssim2dB(double ssim)
{
    double inv_ssim = 1 - ssim;

    if (inv_ssim <= 0.0000000001) /* Max 100dB */
        return 100;

    return -10.0 * log10(inv_ssim);
}

/* The qscale - qp conversion is specified in the standards.
 * Approx qscale increases by 12%  with every qp increment */
double x265_qScale2qp(double qScale)
{
    return 12.0 + 6.0 * (double)X265_LOG2(qScale / 0.85);
}

double x265_qp2qScale(double qp)
{
    return 0.85 * pow(2.0, (qp - 12.0) / 6.0);
}

uint32_t x265_picturePlaneSize(int csp, int width, int height, int plane)
{
    uint32_t size = (uint32_t)(width >> x265_cli_csps[csp].width[plane]) * (height >> x265_cli_csps[csp].height[plane]);

    return size;
}

char* x265_slurp_file(const char *filename)
{
    if (!filename)
        return NULL;

    int bError = 0;
    size_t fSize;
    char *buf = NULL;

    FILE *fh = x265_fopen(filename, "rb");
    if (!fh)
    {
        x265_log_file(NULL, X265_LOG_ERROR, "unable to open file %s\n", filename);
        return NULL;
    }

    bError |= fseek(fh, 0, SEEK_END) < 0;
    bError |= (fSize = ftell(fh)) <= 0;
    bError |= fseek(fh, 0, SEEK_SET) < 0;
    if (bError)
        goto error;

    buf = X265_MALLOC(char, fSize + 2);
    if (!buf)
    {
        x265_log(NULL, X265_LOG_ERROR, "unable to allocate memory\n");
        goto error;
    }

    bError |= fread(buf, 1, fSize, fh) != fSize;
    if (buf[fSize - 1] != '\n')
        buf[fSize++] = '\n';
    buf[fSize] = 0;
    fclose(fh);

    if (bError)
    {
        x265_log(NULL, X265_LOG_ERROR, "unable to read the file\n");
        X265_FREE(buf);
        buf = NULL;
    }
    return buf;

error:
    fclose(fh);
    return NULL;
}

}
