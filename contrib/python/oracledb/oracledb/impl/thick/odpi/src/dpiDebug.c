//-----------------------------------------------------------------------------
// Copyright (c) 2017, 2022, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// dpiDebug.c
//   Methods used for debugging ODPI-C.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

#define DPI_DEBUG_THREAD_FORMAT         "%.5" PRIu64
#define DPI_DEBUG_DATE_FORMAT           "%.4d-%.2d-%.2d"
#define DPI_DEBUG_TIME_FORMAT           "%.2d:%.2d:%.2d.%.3d"

// debug level (populated by environment variable DPI_DEBUG_LEVEL)
unsigned long dpiDebugLevel = 0;

// debug prefix format (populated by environment variable DPI_DEBUG_PREFIX)
static char dpiDebugPrefixFormat[64] = "ODPI [%i] %d %t: ";

// debug file for printing (currently unchangeable)
static FILE *dpiDebugStream = NULL;


//-----------------------------------------------------------------------------
// dpiDebug__getFormatWithPrefix() [INTERNAL]
//   Adjust the provided format to include the prefix requested by the user.
// This method is not permitted to fail, so if there is not enough space, the
// prefix is truncated as needed -- although this is a very unlikely scenario.
//-----------------------------------------------------------------------------
static void dpiDebug__getFormatWithPrefix(const char *format,
        char *formatWithPrefix, size_t maxFormatWithPrefixSize)
{
    char *sourcePtr, *targetPtr;
    int gotTime, tempSize;
    uint64_t threadId;
    size_t size;
#ifdef _WIN32
    SYSTEMTIME time;
#else
    struct timeval timeOfDay;
    struct tm time;
#endif

    gotTime = 0;
    sourcePtr = dpiDebugPrefixFormat;
    targetPtr = formatWithPrefix;
    size = maxFormatWithPrefixSize - strlen(format);
    while (*sourcePtr && size > 20) {

        // all characters except '%' are copied verbatim to the target
        if (*sourcePtr != '%') {
            *targetPtr++ = *sourcePtr++;
            maxFormatWithPrefixSize--;
            continue;
        }

        // handle the different directives
        sourcePtr++;
        switch (*sourcePtr) {
            case 'i':
#ifdef _WIN32
                threadId = (uint64_t) GetCurrentThreadId();
#elif defined __linux
                threadId = (uint64_t) syscall(SYS_gettid);
#elif defined __APPLE__
                pthread_threadid_np(NULL, &threadId);
#else
                threadId = (uint64_t) pthread_self();
#endif
                tempSize = sprintf(targetPtr, DPI_DEBUG_THREAD_FORMAT,
                        threadId);
                size -= tempSize;
                targetPtr += tempSize;
                sourcePtr++;
                break;
            case 'd':
            case 't':
                if (!gotTime) {
                    gotTime = 1;
#ifdef _WIN32
                    GetLocalTime(&time);
#else
                    gettimeofday(&timeOfDay, NULL);
                    localtime_r(&timeOfDay.tv_sec, &time);
#endif
                }
#ifdef _WIN32
                if (*sourcePtr == 'd')
                    tempSize = sprintf(targetPtr, DPI_DEBUG_DATE_FORMAT,
                            time.wYear, time.wMonth, time.wDay);
                else tempSize = sprintf(targetPtr, DPI_DEBUG_TIME_FORMAT,
                        time.wHour, time.wMinute, time.wSecond,
                        time.wMilliseconds);
#else
                if (*sourcePtr == 'd')
                    tempSize = sprintf(targetPtr, DPI_DEBUG_DATE_FORMAT,
                            time.tm_year + 1900, time.tm_mon + 1,
                            time.tm_mday);
                else tempSize = sprintf(targetPtr, DPI_DEBUG_TIME_FORMAT,
                        time.tm_hour, time.tm_min, time.tm_sec,
                        (int) (timeOfDay.tv_usec / 1000));
#endif
                size -= tempSize;
                targetPtr += tempSize;
                sourcePtr++;
                break;
            case '\0':
                break;
            default:
                *targetPtr++ = '%';
                *targetPtr++ = *sourcePtr++;
                break;
        }
    }

    // append original format
    strcpy(targetPtr, format);
}


//-----------------------------------------------------------------------------
// dpiDebug__initialize() [INTERNAL]
//   Initialize debugging infrastructure. This reads the environment variables
// and populates the global variables used for determining which messages to
// print and what prefix should be placed in front of each message.
//-----------------------------------------------------------------------------
void dpiDebug__initialize(void)
{
    char *envValue;

    // determine the value of the environment variable DPI_DEBUG_LEVEL and
    // convert to an integer; if the value in the environment variable is not a
    // valid integer, it is ignored
    envValue = getenv("DPI_DEBUG_LEVEL");
    if (envValue)
        dpiDebugLevel = (unsigned long) strtol(envValue, NULL, 10);

    // determine the value of the environment variable DPI_DEBUG_PREFIX and
    // store it in the static buffer available for it; a static buffer is used
    // since this runs during startup and may not fail; if the value of the
    // environment variable is too large for the buffer, the value is ignored
    // and the default value is used instead
    envValue = getenv("DPI_DEBUG_PREFIX");
    if (envValue && strlen(envValue) < sizeof(dpiDebugPrefixFormat))
        strcpy(dpiDebugPrefixFormat, envValue);

    // messages are written to stderr
    dpiDebugStream = stderr;

    // for any debugging level > 0 print a message indicating that tracing
    // has started
    if (dpiDebugLevel) {
        dpiDebug__print("ODPI-C %s\n", DPI_VERSION_STRING);
        dpiDebug__print("debugging messages initialized at level %lu\n",
                dpiDebugLevel);
    }
}


//-----------------------------------------------------------------------------
// dpiDebug__print() [INTERNAL]
//   Print the specified debugging message with a newly calculated prefix.
//-----------------------------------------------------------------------------
void dpiDebug__print(const char *format, ...)
{
    char formatWithPrefix[512];
    va_list varArgs;

    dpiDebug__getFormatWithPrefix(format, formatWithPrefix,
            sizeof(formatWithPrefix));
    va_start(varArgs, format);
    (void) vfprintf(dpiDebugStream, formatWithPrefix, varArgs);
    va_end(varArgs);
}
