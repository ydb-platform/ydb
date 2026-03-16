/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
%                   Windows NT Utility Methods for MagickCore                 %
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
#if defined(MAGICKCORE_WINDOWS_SUPPORT)
#include "magick/client.h"
#include "magick/exception-private.h"
#include "magick/image-private.h"
#include "magick/locale_.h"
#include "magick/log.h"
#include "magick/magick.h"
#include "magick/memory_.h"
#include "magick/memory-private.h"
#include "magick/nt-base.h"
#include "magick/nt-base-private.h"
#include "magick/resource_.h"
#include "magick/timer.h"
#include "magick/string_.h"
#include "magick/string-private.h"
#include "magick/utility.h"
#include "magick/utility-private.h"
#include "magick/version.h"
#if defined(MAGICKCORE_LTDL_DELEGATE)
#  error #include "ltdl.h"
#endif
#if defined(MAGICKCORE_CIPHER_SUPPORT)
#include <ntsecapi.h>
#include <wincrypt.h>
#endif

/*
  Define declarations.
*/
#if !defined(MAP_FAILED)
#define MAP_FAILED      ((void *)(LONG_PTR) -1)
#endif
#define MaxWideByteExtent  100

/*
  Typdef declarations.
*/

/*
  We need to make sure only one instance is created for each process and that
  is why we wrap the new/delete instance methods.

  From: http://www.ghostscript.com/doc/current/API.htm
    "The Win32 DLL gsdll32.dll can be used by multiple programs simultaneously,
     but only once within each process"
*/
typedef struct _NTGhostInfo
{
  void
    (MagickDLLCall *delete_instance)(gs_main_instance *);

  int
    (MagickDLLCall *new_instance)(gs_main_instance **, void *);

  MagickBooleanType
    has_instance;
} NTGhostInfo;

/*
  Static declarations.
*/
#if !defined(MAGICKCORE_LTDL_DELEGATE)
static char
  *lt_slsearchpath = (char *) NULL;
#endif

static NTGhostInfo
  nt_ghost_info;

static GhostInfo
  ghost_info;

static void
  *ghost_handle = (void *) NULL;

static SemaphoreInfo
  *ghost_semaphore = (SemaphoreInfo *) NULL,
  *winsock_semaphore = (SemaphoreInfo *) NULL;

static WSADATA
  *wsaData = (WSADATA*) NULL;

static size_t
  long_paths_enabled = 2;

struct
{
  const HKEY
    hkey;

  const char
    *name;
}
const registry_roots[2] =
{
  { HKEY_CURRENT_USER,  "HKEY_CURRENT_USER"  },
  { HKEY_LOCAL_MACHINE, "HKEY_LOCAL_MACHINE" }
};

/*
  External declarations.
*/
#if !defined(MAGICKCORE_WINDOWS_SUPPORT)
extern "C" BOOL WINAPI
  DllMain(HINSTANCE handle,DWORD reason,LPVOID lpvReserved);
#endif

static void MagickDLLCall NTGhostscriptDeleteInstance(
  gs_main_instance *instance)
{
  LockSemaphoreInfo(ghost_semaphore);
  nt_ghost_info.delete_instance(instance);
  nt_ghost_info.has_instance=MagickFalse;
  UnlockSemaphoreInfo(ghost_semaphore);
}

static int MagickDLLCall NTGhostscriptNewInstance(gs_main_instance **pinstance,
  void *caller_handle)
{
  int
    status;

  LockSemaphoreInfo(ghost_semaphore);
  status=-1;
  if (nt_ghost_info.has_instance == MagickFalse)
    {
      status=nt_ghost_info.new_instance(pinstance,caller_handle);
      if (status >= 0)
        nt_ghost_info.has_instance=MagickTrue;
    }
  UnlockSemaphoreInfo(ghost_semaphore);
  return(status);
}

static inline char *create_utf8_string(const wchar_t *wideChar)
{
  char
    *utf8;

  int
    count;

  count=WideCharToMultiByte(CP_UTF8,0,wideChar,-1,NULL,0,NULL,NULL);
  if (count < 0)
    return((char *) NULL);
  utf8=(char *) NTAcquireQuantumMemory(count+1,sizeof(*utf8));
  if (utf8 == (char *) NULL)
    return((char *) NULL);
  count=WideCharToMultiByte(CP_UTF8,0,wideChar,-1,utf8,count,NULL,NULL);
  if (count == 0)
    {
      utf8=DestroyString(utf8);
      return((char *) NULL);
    }
  utf8[count]=0;
  return(utf8);
}

static unsigned char *NTGetRegistryValue(HKEY root,const char *key,DWORD flags,
  const char *name)
{
  unsigned char
    *value;

  HKEY
    registry_key;

  DWORD
    size,
    type;

  LSTATUS
    status;

  wchar_t
    wide_name[MaxWideByteExtent];

  value=(unsigned char *) NULL;
  status=RegOpenKeyExA(root,key,0,(KEY_READ | flags),&registry_key);
  if (status != ERROR_SUCCESS)
    return(value);
  if (MultiByteToWideChar(CP_UTF8,0,name,-1,wide_name,MaxWideByteExtent) == 0)
    {
      RegCloseKey(registry_key);
      return(value);
    }
  status=RegQueryValueExW(registry_key,wide_name,0,&type,0,&size);
  if ((status == ERROR_SUCCESS) && (type == REG_SZ))
    {
      LPBYTE
        wide;

      wide=(LPBYTE) NTAcquireQuantumMemory((const size_t) size,sizeof(*wide));
      if (wide != (LPBYTE) NULL)
        {
          status=RegQueryValueExW(registry_key,wide_name,0,&type,wide,&size);
          if ((status == ERROR_SUCCESS) && (type == REG_SZ))
            value=(unsigned char *) create_utf8_string((const wchar_t *) wide);
          wide=(LPBYTE) RelinquishMagickMemory(wide);
        }
    }
  RegCloseKey(registry_key);
  return(value);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   D l l M a i n                                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  DllMain() is an entry point to the DLL which is called when processes and
%  threads are initialized and terminated, or upon calls to the Windows
%  LoadLibrary and FreeLibrary functions.
%
%  The function returns TRUE of it succeeds, or FALSE if initialization fails.
%
%  The format of the DllMain method is:
%
%    BOOL WINAPI DllMain(HINSTANCE handle,DWORD reason,LPVOID lpvReserved)
%
%  A description of each parameter follows:
%
%    o handle: handle to the DLL module
%
%    o reason: reason for calling function:
%
%      DLL_PROCESS_ATTACH - DLL is being loaded into virtual address
%                           space of current process.
%      DLL_THREAD_ATTACH - Indicates that the current process is
%                          creating a new thread.  Called under the
%                          context of the new thread.
%      DLL_THREAD_DETACH - Indicates that the thread is exiting.
%                          Called under the context of the exiting
%                          thread.
%      DLL_PROCESS_DETACH - Indicates that the DLL is being unloaded
%                           from the virtual address space of the
%                           current process.
%
%    o lpvReserved: Used for passing additional info during DLL_PROCESS_ATTACH
%                   and DLL_PROCESS_DETACH.
%
*/
#if defined(_DLL) && defined(ProvideDllMain)
BOOL WINAPI DllMain(HINSTANCE handle,DWORD reason,LPVOID lpvReserved)
{
  magick_unreferenced(lpvReserved);

  switch (reason)
  {
    case DLL_PROCESS_ATTACH:
    {
      char
        *module_path;

      ssize_t
        count;

      wchar_t
        *wide_path;

      MagickCoreGenesis((const char*) NULL,MagickFalse);
      wide_path=(wchar_t *) NTAcquireQuantumMemory(MaxTextExtent,
        sizeof(*wide_path));
      if (wide_path == (wchar_t *) NULL)
        return(FALSE);
      count=(ssize_t) GetModuleFileNameW(handle,wide_path,MaxTextExtent);
      if (count != 0)
        {
          char
            *path;

          module_path=create_utf8_string(wide_path);
          for ( ; count > 0; count--)
            if (module_path[count] == '\\')
              {
                module_path[count+1]='\0';
                break;
              }
          path=(char *) NTAcquireQuantumMemory(MaxTextExtent,16*sizeof(*path));
          if (path == (char *) NULL)
            {
              module_path=DestroyString(module_path);
              wide_path=(wchar_t *) RelinquishMagickMemory(wide_path);
              return(FALSE);
            }
          count=(ssize_t) GetEnvironmentVariable("PATH",path,16*MaxTextExtent);
          if ((count != 0) && (strstr(path,module_path) == (char *) NULL))
            {
              if ((strlen(module_path)+count+1) < (16*MaxTextExtent-1))
                {
                  char
                    *variable;

                  variable=(char *) NTAcquireQuantumMemory(MaxTextExtent,
                    16*sizeof(*variable));
                  if (variable == (char *) NULL)
                    {
                      path=DestroyString(path);
                      module_path=DestroyString(module_path);
                      wide_path=(wchar_t *) RelinquishMagickMemory(wide_path);
                      return(FALSE);
                    }
                  (void) FormatLocaleString(variable,16*MaxTextExtent,
                    "%s;%s",module_path,path);
                  SetEnvironmentVariable("PATH",variable);
                  variable=DestroyString(variable);
                }
            }
          path=DestroyString(path);
          module_path=DestroyString(module_path);
        }
      wide_path=(wchar_t *) RelinquishMagickMemory(wide_path);
      break;
    }
    case DLL_PROCESS_DETACH:
    {
      MagickCoreTerminus();
      break;
    }
    default:
      break;
  }
  return(TRUE);
}
#endif

#if !defined(__MINGW32__)
/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   g e t t i m e o f d a y                                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  The gettimeofday() method get the time of day.
%
%  The format of the gettimeofday method is:
%
%      int gettimeofday(struct timeval *time_value,struct timezone *time_zone)
%
%  A description of each parameter follows:
%
%    o time_value: the time value.
%
%    o time_zone: the time zone.
%
*/
MagickPrivate int gettimeofday (struct timeval *time_value,
  struct timezone *time_zone)
{
#define EpochFiletime  MagickLLConstant(116444736000000000)

  static int
    is_tz_set;

  if (time_value != (struct timeval *) NULL)
    {
      FILETIME
        file_time;

      __int64
        time;

      LARGE_INTEGER
        date_time;

      GetSystemTimeAsFileTime(&file_time);
      date_time.LowPart=file_time.dwLowDateTime;
      date_time.HighPart=file_time.dwHighDateTime;
      time=date_time.QuadPart;
      time-=EpochFiletime;
      time/=10;
      time_value->tv_sec=(ssize_t) (time / 1000000);
      time_value->tv_usec=(ssize_t) (time % 1000000);
    }
  if (time_zone != (struct timezone *) NULL)
    {
      if (is_tz_set == 0)
        {
          _tzset();
          is_tz_set++;
        }
      time_zone->tz_minuteswest=_timezone/60;
      time_zone->tz_dsttime=_daylight;
    }
  return(0);
}
#endif

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T A c c e s s W i d e                                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% NTAccessWide() checks the file accessibility of a wide-character path.
#
# The format of the NTAccessWide method is:
%
%     int NTAccessWide(const char *path, int mode)
%
%  A description of each parameter follows:
%
%    o path: the file path.
%
%    o mode: the accessibility mode.
%
*/
MagickExport int NTAccessWide(const char *path, int mode)
{
  int
    status;

  wchar_t
    *path_wide;

  path_wide=NTCreateWidePath(path);
  if (path_wide == (wchar_t *) NULL)
    return(-1);
  status=_waccess(path_wide,mode);
  path_wide=(wchar_t *) RelinquishMagickMemory(path_wide);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T A r g v T o U T F 8                                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTArgvToUTF8() converts the wide command line arguments to UTF-8 to ensure
%  compatibility with Linux.
%
%  The format of the NTArgvToUTF8 method is:
%
%      char **NTArgvToUTF8(const int argc,wchar_t **argv)
%
%  A description of each parameter follows:
%
%    o argc: the number of command line arguments.
%
%    o argv:  the  wide-character command line arguments.
%
*/
MagickPrivate char **NTArgvToUTF8(const int argc,wchar_t **argv)
{
  char
    **utf8;

  ssize_t
    i;

  utf8=(char **) NTAcquireQuantumMemory(argc,sizeof(*utf8));
  if (utf8 == (char **) NULL)
    ThrowFatalException(ResourceLimitFatalError,"UnableToConvertStringToARGV");
  for (i=0; i < (ssize_t) argc; i++)
  {
    utf8[i]=create_utf8_string(argv[i]);
    if (utf8[i] == (char *) NULL)
      {
        for (i--; i >= 0; i--)
          utf8[i]=DestroyString(utf8[i]);
        ThrowFatalException(ResourceLimitFatalError,
          "UnableToConvertStringToARGV");
      }
  }
  return(utf8);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T C l o s e D i r e c t o r y                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTCloseDirectory() closes the named directory stream and frees the DIR
%  structure.
%
%  The format of the NTCloseDirectory method is:
%
%      int NTCloseDirectory(DIR *entry)
%
%  A description of each parameter follows:
%
%    o entry: Specifies a pointer to a DIR structure.
%
*/
MagickPrivate int NTCloseDirectory(DIR *entry)
{
  assert(entry != (DIR *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"...");
  FindClose(entry->hSearch);
  entry=(DIR *) RelinquishMagickMemory(entry);
  return(0);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T C l o s e L i b r a r y                                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTCloseLibrary() unloads the module associated with the passed handle.
%
%  The format of the NTCloseLibrary method is:
%
%      void NTCloseLibrary(void *handle)
%
%  A description of each parameter follows:
%
%    o handle: Specifies a handle to a previously loaded dynamic module.
%
*/
MagickPrivate int NTCloseLibrary(void *handle)
{
  return(!(FreeLibrary((HINSTANCE) handle)));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T C r e a t e W i d e P a t h                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTCreateWidePath() returns the wide-character version of the specified 
%  UTF-8 path.
%
%  The format of the NTCreateWidePath method is:
%
%      void NTCreateWidePath(void *handle)
%
%  A description of each parameter follows:
%
%    o utf8: Specifies a handle to a previously loaded dynamic module.
%
*/
MagickExport wchar_t* NTCreateWidePath(const char *utf8)
{
  int
    count;

  wchar_t
    *wide;

  count=MultiByteToWideChar(CP_UTF8,0,utf8,-1,NULL,0);
  if ((count > MAX_PATH) && (strncmp(utf8,"\\\\?\\",4) != 0) &&
      (NTLongPathsEnabled() == MagickFalse))
    {
      char
        buffer[MagickPathExtent];

      wchar_t
        shortPath[MAX_PATH],
        *longPath;

      size_t
        length;

      (void) FormatLocaleString(buffer,MagickPathExtent,"\\\\?\\%s",utf8);
      count+=4;
      longPath=(wchar_t *) NTAcquireQuantumMemory((size_t) count,
        sizeof(*longPath));
      if (longPath == (wchar_t *) NULL)
        return((wchar_t *) NULL);
      count=MultiByteToWideChar(CP_UTF8,0,buffer,-1,longPath,count);
      if (count != 0)
        count=(int) GetShortPathNameW(longPath,shortPath,MAX_PATH);
      longPath=(wchar_t *) RelinquishMagickMemory(longPath);
      if ((count < 5) || (count >= MAX_PATH))
        return((wchar_t *) NULL);
      length=(size_t) count-3;
      wide=(wchar_t *) NTAcquireQuantumMemory(length,sizeof(*wide));
      wcscpy_s(wide,length,shortPath+4);
      return(wide);
    }
  wide=(wchar_t *) NTAcquireQuantumMemory((size_t) count,sizeof(*wide));
  if ((wide != (wchar_t *) NULL) &&
      (MultiByteToWideChar(CP_UTF8,0,utf8,-1,wide,count) == 0))
    wide=(wchar_t *) RelinquishMagickMemory(wide);
  return(wide);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T C o n t r o l H a n d l e r                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTControlHandler() registers a control handler that is activated when, for
%  example, a ctrl-c is received.
%
%  The format of the NTControlHandler method is:
%
%      int NTControlHandler(void)
%
*/

static BOOL ControlHandler(DWORD type)
{
  (void) type;
  AsynchronousResourceComponentTerminus();
  return(FALSE);
}

MagickPrivate int NTControlHandler(void)
{
  return(SetConsoleCtrlHandler((PHANDLER_ROUTINE) ControlHandler,TRUE));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T E l a p s e d T i m e                                                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTElapsedTime() returns the elapsed time (in seconds) since the last call to
%  StartTimer().
%
%  The format of the ElapsedTime method is:
%
%      double NTElapsedTime(void)
%
*/
MagickPrivate double NTElapsedTime(void)
{
  union
  {
    FILETIME
      filetime;

    __int64
      filetime64;
  } elapsed_time;

  LARGE_INTEGER
    performance_count;

  static LARGE_INTEGER
    frequency = { 0 };

  SYSTEMTIME
    system_time;

  if (frequency.QuadPart == 0)
    {
      if (QueryPerformanceFrequency(&frequency) == 0)
        frequency.QuadPart=1;
    }
  if (frequency.QuadPart > 1)
    {
      QueryPerformanceCounter(&performance_count);
      return((double) performance_count.QuadPart/frequency.QuadPart);
    }
  GetSystemTime(&system_time);
  SystemTimeToFileTime(&system_time,&elapsed_time.filetime);
  return((double) 1.0e-7*elapsed_time.filetime64);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+   N T E r r o r H a n d l e r                                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTErrorHandler() displays an error reason and then terminates the program.
%
%  The format of the NTErrorHandler method is:
%
%      void NTErrorHandler(const ExceptionType severity,const char *reason,
%        const char *description)
%
%  A description of each parameter follows:
%
%    o severity: Specifies the numeric error category.
%
%    o reason: Specifies the reason to display before terminating the
%      program.
%
%    o description: Specifies any description to the reason.
%
*/
MagickPrivate void NTErrorHandler(const ExceptionType severity,
  const char *reason,const char *description)
{
  char
    buffer[3*MaxTextExtent],
    *message;

  (void) severity;
  if (reason == (char *) NULL)
    {
      MagickCoreTerminus();
      exit(0);
    }
  message=GetExceptionMessage(errno);
  if ((description != (char *) NULL) && errno)
    (void) FormatLocaleString(buffer,MaxTextExtent,"%s: %s (%s) [%s].\n",
      GetClientName(),reason,description,message);
  else
    if (description != (char *) NULL)
      (void) FormatLocaleString(buffer,MaxTextExtent,"%s: %s (%s).\n",
        GetClientName(),reason,description);
    else
      if (errno != 0)
        (void) FormatLocaleString(buffer,MaxTextExtent,"%s: %s [%s].\n",
          GetClientName(),reason,message);
      else
        (void) FormatLocaleString(buffer,MaxTextExtent,"%s: %s.\n",
          GetClientName(),reason);
  message=DestroyString(message);
  (void) MessageBox(NULL,buffer,"ImageMagick Exception",MB_OK | MB_TASKMODAL |
    MB_SETFOREGROUND | MB_ICONEXCLAMATION);
  MagickCoreTerminus();
  exit(0);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T E x i t L i b r a r y                                                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTExitLibrary() exits the dynamic module loading subsystem.
%
%  The format of the NTExitLibrary method is:
%
%      int NTExitLibrary(void)
%
*/
MagickPrivate int NTExitLibrary(void)
{
  return(0);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G a t h e r R a n d o m D a t a                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGatherRandomData() gathers random data and returns it.
%
%  The format of the GatherRandomData method is:
%
%      MagickBooleanType NTGatherRandomData(const size_t length,
%        unsigned char *random)
%
%  A description of each parameter follows:
%
%    length: the length of random data buffer
%
%    random: the random data is returned here.
%
*/
MagickPrivate MagickBooleanType NTGatherRandomData(const size_t length,
  unsigned char *random)
{
#if defined(MAGICKCORE_CIPHER_SUPPORT) && defined(_MSC_VER) && (_MSC_VER > 1200)
  HCRYPTPROV
    handle;

  int
    status;

  handle=(HCRYPTPROV) NULL;
  status=CryptAcquireContext(&handle,NULL,MS_DEF_PROV,PROV_RSA_FULL,
    (CRYPT_VERIFYCONTEXT | CRYPT_MACHINE_KEYSET));
  if (status == 0)
    status=CryptAcquireContext(&handle,NULL,MS_DEF_PROV,PROV_RSA_FULL,
      (CRYPT_VERIFYCONTEXT | CRYPT_MACHINE_KEYSET | CRYPT_NEWKEYSET));
  if (status == 0)
    return(MagickFalse);
  status=CryptGenRandom(handle,(DWORD) length,random);
  if (status == 0)
    {
      status=CryptReleaseContext(handle,0);
      return(MagickFalse);
    }
  status=CryptReleaseContext(handle,0);
  if (status == 0)
    return(MagickFalse);
#else
  (void) random;
  (void) length;
#endif
  return(MagickTrue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G e t E n v i r o n m e n t V a l u e                                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGetEnvironmentValue() returns the environment string that matches the
%  specified name.
%
%  The format of the NTGetEnvironmentValue method is:
%
%      char *GetEnvironmentValue(const char *name)
%
%  A description of each parameter follows:
%
%    o name: the environment name.
%
*/
extern MagickPrivate char *NTGetEnvironmentValue(const char *name)
{
  char
    *environment = (char *) NULL;

  DWORD
    size;

  LPWSTR
    wide;

  wchar_t
    wide_name[MaxWideByteExtent];

  if (MultiByteToWideChar(CP_UTF8,0,name,-1,wide_name,MaxWideByteExtent) == 0)
    return(environment);
  size=GetEnvironmentVariableW(wide_name,(LPWSTR) NULL,0);
  if (size == 0)
    return(environment);
  wide=(LPWSTR) NTAcquireQuantumMemory((const size_t) size,sizeof(*wide));
  if (wide == (LPWSTR) NULL)
    return(environment);
  if (GetEnvironmentVariableW(wide_name,wide,size) != 0)
    environment=create_utf8_string(wide);
  wide=(LPWSTR) RelinquishMagickMemory(wide);
  return(environment);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G e t E x e c u t i o n P a t h                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGetExecutionPath() returns the execution path of a program.
%
%  The format of the GetExecutionPath method is:
%
%      MagickBooleanType NTGetExecutionPath(char *path,const size_t extent)
%
%  A description of each parameter follows:
%
%    o path: the pathname of the executable that started the process.
%
%    o extent: the maximum extent of the path.
%
*/
MagickPrivate MagickBooleanType NTGetExecutionPath(char *path,
  const size_t extent)
{
  wchar_t
    wide_path[MaxTextExtent];

  (void) GetModuleFileNameW((HMODULE) NULL,wide_path,(DWORD) extent);
  (void) WideCharToMultiByte(CP_UTF8,0,wide_path,-1,path,(int) extent,NULL,
    NULL);
  return(MagickTrue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G e t L a s t E r r o r M e s s a g e                                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGetLastErrorMessage() returns the last error that occurred.
%
%  The format of the NTGetLastErrorMessage method is:
%
%      char *NTGetLastErrorMessage(DWORD last_error)
%
%  A description of each parameter follows:
%
%    o last_error: The value of GetLastError.
%
*/
static char *NTGetLastErrorMessage(DWORD last_error)
{
  char
    *reason;

  int
    status;

  LPVOID
    buffer = (LPVOID) NULL;

  status=FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
    FORMAT_MESSAGE_FROM_SYSTEM,NULL,last_error,
    MAKELANGID(LANG_NEUTRAL,SUBLANG_DEFAULT),(LPTSTR) &buffer,0,NULL);
  if (!status)
    reason=AcquireString("An unknown error occurred");
  else
    {
      reason=AcquireString((const char *) buffer);
      LocalFree(buffer);
    }
  return(reason);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G e t L i b r a r y E r r o r                                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  Lt_dlerror() returns a pointer to a string describing the last error
%  associated with a lt_dl method.  Note that this function is not thread
%  safe so it should only be used under the protection of a lock.
%
%  The format of the NTGetLibraryError method is:
%
%      const char *NTGetLibraryError(void)
%
*/
MagickPrivate const char *NTGetLibraryError(void)
{
  static char
    last_error[MaxTextExtent];

  char
    *error;

  *last_error='\0';
  error=NTGetLastErrorMessage(GetLastError());
  if (error)
    (void) CopyMagickString(last_error,error,MaxTextExtent);
  error=DestroyString(error);
  return(last_error);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G e t L i b r a r y S y m b o l                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGetLibrarySymbol() retrieve the procedure address of the method
%  specified by the passed character string.
%
%  The format of the NTGetLibrarySymbol method is:
%
%      void *NTGetLibrarySymbol(void *handle,const char *name)
%
%  A description of each parameter follows:
%
%    o handle: Specifies a handle to the previously loaded dynamic module.
%
%    o name: Specifies the procedure entry point to be returned.
%
*/
void *NTGetLibrarySymbol(void *handle,const char *name)
{
  FARPROC
    proc_address;

  proc_address=GetProcAddress((HMODULE) handle,(LPCSTR) name);
  if (proc_address == (FARPROC) NULL)
    return((void *) NULL);
  return((void *) proc_address);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G e t M o d u l e P a t h                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGetModulePath() returns the path of the specified module.
%
%  The format of the GetModulePath method is:
%
%      MagickBooleanType NTGetModulePath(const char *module,char *path)
%
%  A description of each parameter follows:
%
%    modith: the module name.
%
%    path: the module path is returned here.
%
*/
MagickPrivate MagickBooleanType NTGetModulePath(const char *module,char *path)
{
  char
    module_path[MaxTextExtent];

  HMODULE
    handle;

  ssize_t
    length;

  *path='\0';
  handle=GetModuleHandle(module);
  if (handle == (HMODULE) NULL)
    return(MagickFalse);
  length=GetModuleFileName(handle,module_path,MaxTextExtent);
  if (length != 0)
    GetPathComponent(module_path,HeadPath,path);
  return(MagickTrue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G h o s t s c r i p t D L L V e c t o r s                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGhostscriptDLLVectors() returns a GhostInfo structure that includes
%  function vectors to invoke Ghostscript DLL functions. A null pointer is
%  returned if there is an error when loading the DLL or retrieving the
%  function vectors.
%
%  The format of the NTGhostscriptDLLVectors method is:
%
%      const GhostInfo *NTGhostscriptDLLVectors(void)
%
*/
static int NTLocateGhostscript(DWORD flags,int *root_index,
  const char **product_family,int *major_version,int *minor_version,
  int *patch_version)
{
  int
    i;

  MagickBooleanType
    status;

  static const char
    *products[2] =
    {
      "Artifex Ghostscript",
      "GPL Ghostscript"
    };

  /*
    Find the most recent version of Ghostscript.
  */
  status=MagickFalse;
  *root_index=0;
  *product_family=NULL;
  *major_version=5;
  *minor_version=49; /* min version of Ghostscript is 5.50 */
  for (i=0; i < (ssize_t) (sizeof(products)/sizeof(products[0])); i++)
  {
    char
      key[MagickPathExtent];

    HKEY
      hkey;

    int
      j;

    REGSAM
      mode;

    (void) FormatLocaleString(key,MagickPathExtent,"SOFTWARE\\%s",products[i]);
    for (j=0; j < (ssize_t) (sizeof(registry_roots)/sizeof(registry_roots[0]));
         j++)
    {
      mode=KEY_READ | flags;
      if (RegOpenKeyExA(registry_roots[j].hkey,key,0,mode,&hkey) ==
            ERROR_SUCCESS)
        {
          DWORD
            extent;

          int
            k;

          /*
            Now enumerate the keys.
          */
          extent=sizeof(key)/sizeof(char);
          for (k=0; RegEnumKeyA(hkey,k,key,extent) == ERROR_SUCCESS; k++)
          {
            int
              major,
              minor,
              patch;

            major=0;
            minor=0;
            patch=0;
            if (sscanf(key,"%d.%d.%d",&major,&minor,&patch) != 3)
              if (sscanf(key,"%d.%d",&major,&minor) != 2)
                continue;
            if ((major > *major_version) ||
               ((major == *major_version) && (minor > *minor_version)) ||
               ((minor == *minor_version) && (patch > *patch_version)))
              {
                *root_index=j;
                *product_family=products[i];
                *major_version=major;
                *minor_version=minor;
                *patch_version=patch;
                status=MagickTrue;
              }
         }
         (void) RegCloseKey(hkey);
       }
    }
  }
  if (status == MagickFalse)
    {
      *major_version=0;
      *minor_version=0;
      *patch_version=0;
    }
  (void) LogMagickEvent(ConfigureEvent,GetMagickModule(),"Ghostscript (%s) "
    "version %d.%d.%d",*product_family,*major_version,*minor_version,*patch_version);
  return(status);
}

static MagickBooleanType NTGhostscriptGetString(const char *name,
  BOOL *is_64_bit,char *value,const size_t length)
{
  char
    buffer[MagickPathExtent],
    *directory;

  static const char
    *product_family = (const char *) NULL;

  static BOOL
    is_64_bit_version = FALSE;

  static int
    flags = 0,
    major_version = 0,
    minor_version = 0,
    patch_version = 0,
    root_index = 0;

  unsigned char
    *registry_value;

  /*
    Get a string from the installed Ghostscript.
  */
  *value='\0';
  directory=(char *) NULL;
  if (LocaleCompare(name,"GS_DLL") == 0)
    {
      directory=GetEnvironmentValue("MAGICK_GHOSTSCRIPT_PATH");
      if (directory != (char *) NULL)
        {
          (void) FormatLocaleString(buffer,MagickPathExtent,"%s%sgsdll64.dll",
            directory,DirectorySeparator);
          if (IsPathAccessible(buffer) != MagickFalse)
            {
              directory=DestroyString(directory);
              (void) CopyMagickString(value,buffer,length);
              if (is_64_bit != NULL)
                *is_64_bit=TRUE;
              return(MagickTrue);
            }
          (void) FormatLocaleString(buffer,MagickPathExtent,"%s%sgsdll32.dll",
            directory,DirectorySeparator);
          if (IsPathAccessible(buffer) != MagickFalse)
            {
              directory=DestroyString(directory);
              (void) CopyMagickString(value,buffer,length);
              if (is_64_bit != NULL)
                *is_64_bit=FALSE;
              return(MagickTrue);
            }
          return(MagickFalse);
        }
    }
  if (product_family == (const char *) NULL)
    {
      flags=0;
#if defined(KEY_WOW64_32KEY)
#if defined(_WIN64)
      flags=KEY_WOW64_64KEY;
#else
      flags=KEY_WOW64_32KEY;
#endif
      (void) NTLocateGhostscript(flags,&root_index,&product_family,
        &major_version,&minor_version,&patch_version);
      if (product_family == (const char *) NULL)
#if defined(_WIN64)
        flags=KEY_WOW64_32KEY;
      else
        is_64_bit_version=TRUE;
#else
      flags=KEY_WOW64_64KEY;
#endif
#endif
    }
  if (product_family == (const char *) NULL)
    {
      (void) NTLocateGhostscript(flags,&root_index,&product_family,
        &major_version,&minor_version,&patch_version);
#if !defined(_WIN64)
      is_64_bit_version=TRUE;
#endif
    }
  if (product_family == (const char *) NULL)
    return(MagickFalse);
  if (is_64_bit != NULL)
    *is_64_bit=is_64_bit_version;
  (void) FormatLocaleString(buffer,MagickPathExtent,"SOFTWARE\\%s\\%d.%.2d.%d",
    product_family,major_version,minor_version,patch_version);
  registry_value=NTGetRegistryValue(registry_roots[root_index].hkey,buffer,
    flags,name);
  if (registry_value == (unsigned char *) NULL)
    {
      (void) FormatLocaleString(buffer,MagickPathExtent,"SOFTWARE\\%s\\%d.%02d",
        product_family,major_version,minor_version);
      registry_value=NTGetRegistryValue(registry_roots[root_index].hkey,buffer,
        flags,name);
    }
  if (registry_value == (unsigned char *) NULL)
    return(MagickFalse);
  (void) CopyMagickString(value,(const char *) registry_value,length);
  registry_value=(unsigned char *) RelinquishMagickMemory(registry_value);
  (void) LogMagickEvent(ConfigureEvent,GetMagickModule(),
    "registry: \"%s\\%s\\%s\"=\"%s\"",registry_roots[root_index].name,
    buffer,name,value);
  return(MagickTrue);
}

static MagickBooleanType NTGhostscriptDLL(char *path,int length)
{
  static char
    dll[MagickPathExtent] = { "" };

  static BOOL
    is_64_bit;

  *path='\0';
  if ((*dll == '\0') &&
      (NTGhostscriptGetString("GS_DLL",&is_64_bit,dll,sizeof(dll)) != MagickTrue))
    return(MagickFalse);
#if defined(_WIN64)
  if (!is_64_bit)
    return(MagickFalse);
#else
  if (is_64_bit)
    return(MagickFalse);
#endif
  (void) CopyMagickString(path,dll,length);
  return(MagickTrue);
}

static inline MagickBooleanType NTGhostscriptHasValidHandle()
{
  if ((nt_ghost_info.delete_instance == NULL) || (ghost_info.exit == NULL) ||
      (nt_ghost_info.new_instance == NULL) || (ghost_info.set_stdio == NULL) ||
      (ghost_info.init_with_args == NULL) || (ghost_info.revision == NULL))
    return(MagickFalse);
  return(MagickTrue);
}

MagickPrivate const GhostInfo *NTGhostscriptDLLVectors(void)
{
  char
    path[MaxTextExtent];

  if (ghost_semaphore == (SemaphoreInfo *) NULL)
    ActivateSemaphoreInfo(&ghost_semaphore);
  LockSemaphoreInfo(ghost_semaphore);
  if (ghost_handle != (void *) NULL)
    {
      UnlockSemaphoreInfo(ghost_semaphore);
      if (NTGhostscriptHasValidHandle() == MagickFalse)
        return((GhostInfo *) NULL);
      return(&ghost_info);
    }
  if (NTGhostscriptDLL(path,sizeof(path)) == MagickFalse)
    {
      UnlockSemaphoreInfo(ghost_semaphore);
      return(FALSE);
    }
  ghost_handle=lt_dlopen(path);
  if (ghost_handle == (void *) NULL)
    {
      UnlockSemaphoreInfo(ghost_semaphore);
      return(FALSE);
    }
  (void) memset((void *) &nt_ghost_info,0,sizeof(NTGhostInfo));
  nt_ghost_info.delete_instance=(void (MagickDLLCall *)(gs_main_instance *)) (
    lt_dlsym(ghost_handle,"gsapi_delete_instance"));
  nt_ghost_info.new_instance=(int (MagickDLLCall *)(gs_main_instance **,
    void *)) (lt_dlsym(ghost_handle,"gsapi_new_instance"));
  nt_ghost_info.has_instance=MagickFalse;
  (void) memset((void *) &ghost_info,0,sizeof(GhostInfo));
  ghost_info.delete_instance=NTGhostscriptDeleteInstance;
  ghost_info.exit=(int (MagickDLLCall *)(gs_main_instance*))
    lt_dlsym(ghost_handle,"gsapi_exit");
  ghost_info.init_with_args=(int (MagickDLLCall *)(gs_main_instance *,int,
    char **)) (lt_dlsym(ghost_handle,"gsapi_init_with_args"));
  ghost_info.new_instance=NTGhostscriptNewInstance;
  ghost_info.run_string=(int (MagickDLLCall *)(gs_main_instance *,const char *,
    int,int *)) (lt_dlsym(ghost_handle,"gsapi_run_string"));
  ghost_info.set_stdio=(int (MagickDLLCall *)(gs_main_instance *,int(
    MagickDLLCall *)(void *,char *,int),int(MagickDLLCall *)(void *,
    const char *,int),int(MagickDLLCall *)(void *,const char *,int)))
    (lt_dlsym(ghost_handle,"gsapi_set_stdio"));
  ghost_info.revision=(int (MagickDLLCall *)(gsapi_revision_t *,int)) (
    lt_dlsym(ghost_handle,"gsapi_revision"));
  UnlockSemaphoreInfo(ghost_semaphore);
  if (NTGhostscriptHasValidHandle() == MagickFalse)
    return((GhostInfo *) NULL);
  return(&ghost_info);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G h o s t s c r i p t E X E                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGhostscriptEXE() obtains the path to the latest Ghostscript executable.
%  The method returns FALSE if a full path value is not obtained and returns
%  a default path of gswin32c.exe.
%
%  The format of the NTGhostscriptEXE method is:
%
%      int NTGhostscriptEXE(char *path,int length)
%
%  A description of each parameter follows:
%
%    o path: return the Ghostscript executable path here.
%
%    o length: length of buffer.
%
*/
MagickPrivate int NTGhostscriptEXE(char *path,int length)
{
  char
    *p;

  static char
    program[MaxTextExtent] = { "" };

  static BOOL
    is_64_bit_version = FALSE;

  if (*program == '\0')
    {
      if (ghost_semaphore == (SemaphoreInfo *) NULL)
        ActivateSemaphoreInfo(&ghost_semaphore);
      LockSemaphoreInfo(ghost_semaphore);
      if (*program == '\0')
        {
          if (NTGhostscriptGetString("GS_DLL",&is_64_bit_version,program,
              sizeof(program)) == MagickFalse)
            {
              UnlockSemaphoreInfo(ghost_semaphore);
#if defined(_WIN64)
              (void) CopyMagickString(program,"gswin64c.exe",sizeof(program));
#else
              (void) CopyMagickString(program,"gswin32c.exe",sizeof(program));
#endif
              (void) CopyMagickString(path,program,length);
              return(FALSE);
            }
          p=strrchr(program,'\\');
          if (p != (char *) NULL)
            {
              p++;
              *p='\0';
              (void) ConcatenateMagickString(program,is_64_bit_version ?
                "gswin64c.exe" : "gswin32c.exe",sizeof(program));
            }
        }
      UnlockSemaphoreInfo(ghost_semaphore);
    }
  (void) CopyMagickString(path,program,length);
  return(TRUE);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G h o s t s c r i p t F o n t s                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGhostscriptFonts() obtains the path to the Ghostscript fonts.  The method
%  returns FALSE if it cannot determine the font path.
%
%  The format of the NTGhostscriptFonts method is:
%
%      int NTGhostscriptFonts(char *path,int length)
%
%  A description of each parameter follows:
%
%    o path: return the font path here.
%
%    o length: length of the path buffer.
%
*/
MagickPrivate int NTGhostscriptFonts(char *path,int length)
{
  char
    buffer[MaxTextExtent],
    *directory,
    filename[MaxTextExtent];

  char
    *p,
    *q;

  *path='\0';
  directory=GetEnvironmentValue("MAGICK_GHOSTSCRIPT_FONT_PATH");
  if (directory != (char *) NULL)
    {
      (void) CopyMagickString(buffer,directory,MaxTextExtent);
      directory=DestroyString(directory);
    }
  else
    {
      if (NTGhostscriptGetString("GS_LIB",NULL,buffer,MaxTextExtent) == MagickFalse)
        return(FALSE);
    }
  for (p=buffer-1; p != (char *) NULL; p=strchr(p+1,DirectoryListSeparator))
  {
    (void) CopyMagickString(path,p+1,length+1);
    q=strchr(path,DirectoryListSeparator);
    if (q != (char *) NULL)
      *q='\0';
    (void) FormatLocaleString(filename,MaxTextExtent,"%s%sfonts.dir",path,
      DirectorySeparator);
    if (IsPathAccessible(filename) != MagickFalse)
      return(TRUE);
    (void) FormatLocaleString(filename,MaxTextExtent,"%s%sn019003l.pfb",path,
      DirectorySeparator);
    if (IsPathAccessible(filename) != MagickFalse)
      return(TRUE);
  }
  *path='\0';
  return(FALSE);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T G h o s t s c r i p t U n L o a d D L L                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTGhostscriptUnLoadDLL() unloads the Ghostscript DLL and returns TRUE if
%  it succeeds.
%
%  The format of the NTGhostscriptUnLoadDLL method is:
%
%      int NTGhostscriptUnLoadDLL(void)
%
*/
MagickPrivate int NTGhostscriptUnLoadDLL(void)
{
  int
    status;

  if (ghost_semaphore == (SemaphoreInfo *) NULL)
    ActivateSemaphoreInfo(&ghost_semaphore);
  LockSemaphoreInfo(ghost_semaphore);
  status=FALSE;
  if (ghost_handle != (void *) NULL)
    {
      status=lt_dlclose(ghost_handle);
      ghost_handle=(void *) NULL;
      (void) memset((void *) &ghost_info,0,sizeof(GhostInfo));
    }
  UnlockSemaphoreInfo(ghost_semaphore);
  DestroySemaphoreInfo(&ghost_semaphore);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T I n i t i a l i z e L i b r a r y                                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTInitializeLibrary() initializes the dynamic module loading subsystem.
%
%  The format of the NTInitializeLibrary method is:
%
%      int NTInitializeLibrary(void)
%
*/
MagickPrivate int NTInitializeLibrary(void)
{
  return(0);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T I n i t i a l i z e W i n s o c k                                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTInitializeWinsock() initializes Winsock.
%
%  The format of the NTInitializeWinsock method is:
%
%      void NTInitializeWinsock(void)
%
*/
MagickPrivate void NTInitializeWinsock(MagickBooleanType use_lock)
{
  if (use_lock)
    {
      if (winsock_semaphore == (SemaphoreInfo *) NULL)
        ActivateSemaphoreInfo(&winsock_semaphore);
      LockSemaphoreInfo(winsock_semaphore);
    }
  if (wsaData == (WSADATA *) NULL)
    {
      wsaData=(WSADATA *) AcquireMagickMemory(sizeof(WSADATA));
      if (WSAStartup(MAKEWORD(2,2),wsaData) != 0)
        ThrowFatalException(CacheFatalError,"WSAStartup failed");
    }
  if (use_lock)
    UnlockSemaphoreInfo(winsock_semaphore);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T L o n g P a t h s E n a b l e d                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTLongPathsEnabled() returns a boolean indicating whether long paths are
$  enabled.
%
%  The format of the NTLongPathsEnabled method is:
%
%      MagickBooleanType NTLongPathsEnabled()
%
*/
MagickExport MagickBooleanType NTLongPathsEnabled()
{
  if (long_paths_enabled == 2)
    {
      DWORD
        size,
        type,
        value;

      HKEY
        registry_key;

      LONG
        status;

      registry_key=(HKEY) INVALID_HANDLE_VALUE;
      status=RegOpenKeyExA(HKEY_LOCAL_MACHINE,
        "SYSTEM\\CurrentControlSet\\Control\\FileSystem",0,KEY_READ,
        &registry_key);
      if (status != ERROR_SUCCESS)
        {
          long_paths_enabled=0;
          RegCloseKey(registry_key);
          return(MagickFalse);
        }
      value=0;
      status=RegQueryValueExA(registry_key,"LongPathsEnabled",0,&type,NULL,
        NULL);
      if ((status != ERROR_SUCCESS) || (type != REG_DWORD))
        {
          long_paths_enabled=0;
          RegCloseKey(registry_key);
          return(MagickFalse);
        }
      status=RegQueryValueExA(registry_key,"LongPathsEnabled",0,&type,
        (LPBYTE) &value,&size);
      RegCloseKey(registry_key);
      if (status != ERROR_SUCCESS)
        {
          long_paths_enabled=0;
          return(MagickFalse);
        }
      long_paths_enabled=(size_t) value;
    }
  return(long_paths_enabled == 1 ? MagickTrue : MagickFalse);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+  N T M a p M e m o r y                                                      %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTMapMemory() emulates the Unix method of the same name.
%
%  The format of the NTMapMemory method is:
%
%    void *NTMapMemory(char *address,size_t length,int protection,int access,
%      int file,MagickOffsetType offset)
%
*/
MagickPrivate void *NTMapMemory(char *address,size_t length,int protection,
  int flags,int file,MagickOffsetType offset)
{
  DWORD
    access_mode,
    high_length,
    high_offset,
    low_length,
    low_offset,
    protection_mode;

  HANDLE
    file_handle,
    map_handle;

  void
    *map;

  (void) address;
  access_mode=0;
  file_handle=INVALID_HANDLE_VALUE;
  low_length=(DWORD) (length & 0xFFFFFFFFUL);
  high_length=(DWORD) ((((MagickOffsetType) length) >> 32) & 0xFFFFFFFFUL);
  map_handle=INVALID_HANDLE_VALUE;
  map=(void *) NULL;
  low_offset=(DWORD) (offset & 0xFFFFFFFFUL);
  high_offset=(DWORD) ((offset >> 32) & 0xFFFFFFFFUL);
  protection_mode=0;
  if (protection & PROT_WRITE)
    {
      access_mode=FILE_MAP_WRITE;
      if (!(flags & MAP_PRIVATE))
        protection_mode=PAGE_READWRITE;
      else
        {
          access_mode=FILE_MAP_COPY;
          protection_mode=PAGE_WRITECOPY;
        }
    }
  else
    if (protection & PROT_READ)
      {
        access_mode=FILE_MAP_READ;
        protection_mode=PAGE_READONLY;
      }
  if ((file == -1) && (flags & MAP_ANONYMOUS))
    file_handle=INVALID_HANDLE_VALUE;
  else
    file_handle=(HANDLE) _get_osfhandle(file);
  map_handle=CreateFileMapping(file_handle,0,protection_mode,high_length,
    low_length,0);
  if (map_handle)
    {
      map=(void *) MapViewOfFile(map_handle,access_mode,high_offset,low_offset,
        length);
      CloseHandle(map_handle);
    }
  if (map == (void *) NULL)
    return((void *) ((char *) MAP_FAILED));
  return((void *) ((char *) map));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T O p e n D i r e c t o r y                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTOpenDirectory() opens the directory named by filename and associates a
%  directory stream with it.
%
%  The format of the NTOpenDirectory method is:
%
%      DIR *NTOpenDirectory(const char *path)
%
%  A description of each parameter follows:
%
%    o entry: Specifies a pointer to a DIR structure.
%
*/
MagickPrivate DIR *NTOpenDirectory(const char *path)
{
  DIR
    *entry;

  size_t
    length;

  wchar_t
    file_specification[MaxTextExtent];

  assert(path != (const char *) NULL);
  length=MultiByteToWideChar(CP_UTF8,0,path,-1,file_specification,
    MaxTextExtent);
  if (length == 0)
    return((DIR *) NULL);
  if(wcsncat(file_specification,L"\\*.*",MaxTextExtent-wcslen(
      file_specification)-1) == (wchar_t*) NULL)
      return((DIR *) NULL);
  entry=(DIR *) AcquireCriticalMemory(sizeof(DIR));
  entry->firsttime=TRUE;
  entry->hSearch=FindFirstFileW(file_specification,&entry->Win32FindData);
  if (entry->hSearch == INVALID_HANDLE_VALUE)
    {
      entry=(DIR *) RelinquishMagickMemory(entry);
      return((DIR *) NULL);
    }
  return(entry);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T O p e n L i b r a r y                                                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTOpenLibrary() loads a dynamic module into memory and returns a handle that
%  can be used to access the various procedures in the module.
%
%  The format of the NTOpenLibrary method is:
%
%      void *NTOpenLibrary(const char *filename)
%
%  A description of each parameter follows:
%
%    o path: Specifies a pointer to string representing dynamic module that
%      is to be loaded.
%
*/
static inline const char *GetSearchPath(void)
{
#if defined(MAGICKCORE_LTDL_DELEGATE)
  return(lt_dlgetsearchpath());
#else
  return(lt_slsearchpath);
#endif
}

static UINT ChangeErrorMode(void)
{
  typedef UINT
    (CALLBACK *GETERRORMODE)(void);

  GETERRORMODE
    getErrorMode;

  HMODULE
    handle;

  UINT
    mode;

  mode=SEM_FAILCRITICALERRORS | SEM_NOOPENFILEERRORBOX;

  handle=GetModuleHandle("kernel32.dll");
  if (handle == (HMODULE) NULL)
    return SetErrorMode(mode);

  getErrorMode=(GETERRORMODE) NTGetLibrarySymbol(handle,"GetErrorMode");
  if (getErrorMode != (GETERRORMODE) NULL)
    mode=getErrorMode() | SEM_FAILCRITICALERRORS | SEM_NOOPENFILEERRORBOX;

  return SetErrorMode(mode);
}

static inline void *NTLoadLibrary(const char *filename)
{
  int
    length;

  wchar_t
    path[MaxTextExtent];

  length=MultiByteToWideChar(CP_UTF8,0,filename,-1,path,MaxTextExtent);
  if (length == 0)
    return((void *) NULL);
  return (void *) LoadLibraryExW(path,NULL,LOAD_WITH_ALTERED_SEARCH_PATH);
}

MagickPrivate void *NTOpenLibrary(const char *filename)
{
  char
    path[MaxTextExtent];

  const char
    *p,
    *q;

  UINT
    mode;

  void
    *handle;

  mode=ChangeErrorMode();
  handle=NTLoadLibrary(filename);
  if (handle == (void *) NULL)
    {
      p=GetSearchPath();
      while (p != (const char*) NULL)
      {
        q=strchr(p,DirectoryListSeparator);
        if (q != (const char*) NULL)
          (void) CopyMagickString(path,p,q-p+1);
        else
          (void) CopyMagickString(path,p,MaxTextExtent);
        (void) ConcatenateMagickString(path,DirectorySeparator,MaxTextExtent);
        (void) ConcatenateMagickString(path,filename,MaxTextExtent);
        handle=NTLoadLibrary(path);
        if (handle != (void *) NULL || q == (const char*) NULL)
          break;
        p=q+1;
      }
    }
  SetErrorMode(mode);
  return(handle);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%  N T O p e n F i l e W i d e                                                %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTOpenFileWide() opens a file and returns a file pointer.
%
%  The format of the NTOpenFileWide method is:
%
%      FILE *NTOpenFileWide(const char* path, const char* mode)
%
%  A description of each parameter follows:
%
%    o path: the file path.
%
%    o mode: the file open mode.
%
*/
static inline wchar_t *create_wchar_mode(const char *mode)
{
  int
    count;

  wchar_t
    *wide;

  count=MultiByteToWideChar(CP_UTF8,0,mode,-1,NULL,0);
  wide=(wchar_t *) AcquireQuantumMemory((size_t) count+1,
    sizeof(*wide));
  if (wide == (wchar_t *) NULL)
    return((wchar_t *) NULL);
  if (MultiByteToWideChar(CP_UTF8,0,mode,-1,wide,count) == 0)
    {
      wide=(wchar_t *) RelinquishMagickMemory(wide);
      return((wchar_t *) NULL);
    }
  /* Specifies that the file is not inherited by child processes */
  wide[count] = L'\0';
  wide[count-1] = L'N';
  return(wide);
}

MagickExport FILE *NTOpenFileWide(const char* path, const char* mode)
{
  FILE
    *file;

  wchar_t
    *mode_wide,
    *path_wide;

  path_wide=NTCreateWidePath(path);
  if (path_wide == (wchar_t *) NULL)
    return((FILE *) NULL);
  mode_wide=create_wchar_mode(mode);
  if (mode_wide == (wchar_t *) NULL)
    {
      path_wide=(wchar_t *) RelinquishMagickMemory(path_wide);
      return((FILE *) NULL);
    }
  if (_wfopen_s(&file,path_wide,mode_wide) != 0)
    file=(FILE *) NULL;
  mode_wide=(wchar_t *) RelinquishMagickMemory(mode_wide);
  path_wide=(wchar_t *) RelinquishMagickMemory(path_wide);
  return(file);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%  N T O p e n P i p e W i d e                                                %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTOpenPipeWide() opens a pipe and returns a file pointer.
%
%  The format of the NTOpenPipeWide method is:
%
%      FILE *NTOpenPipeWide(const char* command, const char* type)
%
%  A description of each parameter follows:
%
%    o command: the command to execute.
%
%    o type: the file open mode.
%
*/
MagickExport FILE *NTOpenPipeWide(const char *command,const char *type)
{
  FILE
    *file;

  int
    length;

  wchar_t
    *command_wide,
    type_wide[5];

  file=(FILE *) NULL;
  length=MultiByteToWideChar(CP_UTF8,0,type,-1,type_wide,5);
  if (length == 0)
    return(file);
  length=MultiByteToWideChar(CP_UTF8,0,command,-1,NULL,0);
  if (length == 0)
    return(file);
  command_wide=(wchar_t *) AcquireQuantumMemory((size_t) length,
    sizeof(*command_wide));
  if (command_wide == (wchar_t *) NULL)
    return(file);
  length=MultiByteToWideChar(CP_UTF8,0,command,-1,command_wide,length);
  if (length != 0)
    file=_wpopen(command_wide,type_wide);
  command_wide=(wchar_t *) RelinquishMagickMemory(command_wide);
  return(file);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%  N T O p e n W i d e                                                        %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTOpenWide() opens the file specified by path and mode.
%
%  The format of the NTOpenWide method is:
%
%      FILE *NTOpenWide(const char* path, const char* mode)
%
%  A description of each parameter follows:
%
%    o path: the file path.
%
%    o flags: the file open flags.
%
%    o mode: the file open mode.
%
*/
MagickExport int NTOpenWide(const char* path,int flags,mode_t mode)
{
  int
    file_handle,
    status;

  wchar_t
    *path_wide;

  path_wide=NTCreateWidePath(path);
  if (path_wide == (wchar_t *) NULL)
    return(-1);
  /* O_NOINHERIT specifies that the file is not inherited by child processes */
  status=_wsopen_s(&file_handle,path_wide,flags | O_NOINHERIT,_SH_DENYNO,mode);
  path_wide=(wchar_t *) RelinquishMagickMemory(path_wide);
  return(status == 0 ? file_handle : -1);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%    N T R e a d D i r e c t o r y                                            %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTReadDirectory() returns a pointer to a structure representing the
%  directory entry at the current position in the directory stream to which
%  entry refers.
%
%  The format of the NTReadDirectory
%
%      NTReadDirectory(entry)
%
%  A description of each parameter follows:
%
%    o entry: Specifies a pointer to a DIR structure.
%
*/
MagickPrivate struct dirent *NTReadDirectory(DIR *entry)
{
  int
    status;

  size_t
    length;

  if (entry == (DIR *) NULL)
    return((struct dirent *) NULL);
  if (!entry->firsttime)
    {
      status=FindNextFileW(entry->hSearch,&entry->Win32FindData);
      if (status == 0)
        return((struct dirent *) NULL);
    }
  length=WideCharToMultiByte(CP_UTF8,0,entry->Win32FindData.cFileName,-1,
    entry->file_info.d_name,sizeof(entry->file_info.d_name),NULL,NULL);
  if (length == 0)
    return((struct dirent *) NULL);
  entry->firsttime=FALSE;
  entry->file_info.d_namlen=(int) strlen(entry->file_info.d_name);
  return(&entry->file_info);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%    N T R e a l P a t h W i d e                                              %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTRealPathWide returns the absolute path of the specified path.
%
%  The format of the NTRealPathWide method is:
%
%      char *NTRealPathWide(const char *path)
%
%  A description of each parameter follows:
%
%    o path: the file path.
%
*/
static inline wchar_t* resolve_symlink(const wchar_t* path)
{
  DWORD
    link_length;

  HANDLE
    file_handle;

  wchar_t
    *link;

  file_handle=CreateFileW(path,GENERIC_READ,FILE_SHARE_READ |FILE_SHARE_WRITE |
    FILE_SHARE_DELETE,NULL,OPEN_EXISTING,FILE_FLAG_BACKUP_SEMANTICS,NULL);
  if (file_handle == INVALID_HANDLE_VALUE)
    return((wchar_t *) NULL);
  link_length=GetFinalPathNameByHandleW(file_handle,NULL,0,
    FILE_NAME_NORMALIZED);
  link=(wchar_t *) AcquireQuantumMemory(link_length,sizeof(wchar_t));
  if (link == (wchar_t *) NULL)
    {
      CloseHandle(file_handle);
      return((wchar_t *) NULL);
    }
  GetFinalPathNameByHandleW(file_handle,link,link_length,FILE_NAME_NORMALIZED);
  CloseHandle(file_handle);
  return(link);
}

MagickExport char *NTRealPathWide(const char *path)
{
  char
    *real_path;

  wchar_t
    *wide_real_path,
    *wide_path;

  wide_path=NTCreateWidePath(path);
  wide_real_path=resolve_symlink(wide_path);
  if (wide_real_path == (wchar_t*) NULL)
    {
      DWORD
        full_path_length;

      full_path_length=GetFullPathNameW(wide_path,0,NULL,NULL);
      wide_real_path=(wchar_t *) AcquireQuantumMemory(full_path_length,
        sizeof(wchar_t));
      if (wide_real_path == (wchar_t*) NULL)
        {
          wide_path=(wchar_t *) RelinquishMagickMemory(wide_path);
          return((char*) NULL);
        }
      GetFullPathNameW(wide_path,full_path_length,wide_real_path,NULL);
    }
  wide_path=(wchar_t *) RelinquishMagickMemory(wide_path);
  /*
    Remove \\?\ prefix for POSIX-like behavior.
  */
  if (wcsncmp(wide_real_path,L"\\\\?\\",4) == 0)
    real_path=create_utf8_string(wide_real_path+4);
  else
    real_path=create_utf8_string(wide_real_path);
  wide_real_path=(wchar_t *) RelinquishMagickMemory(wide_real_path);
  return(real_path);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T R e g i s t r y K e y L o o k u p                                     %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTRegistryKeyLookup() returns ImageMagick installation path settings
%  stored in the Windows Registry.  Path settings are specific to the
%  installed ImageMagick version so that multiple Image Magick installations
%  may coexist.
%
%  Values are stored in the registry under a base path similar to
%  "HKEY_LOCAL_MACHINE/SOFTWARE\ImageMagick\6.7.4\Q:16" or
%  "HKEY_CURRENT_USER/SOFTWARE\ImageMagick\6.7.4\Q:16". The provided subkey
%  is appended to this base path to form the full key.
%
%  The format of the NTRegistryKeyLookup method is:
%
%      unsigned char *NTRegistryKeyLookup(const char *subkey)
%
%  A description of each parameter follows:
%
%    o subkey: Specifies a string that identifies the registry object.
%      Currently supported sub-keys include: "BinPath", "ConfigurePath",
%      "LibPath", "CoderModulesPath", "FilterModulesPath", "SharePath".
%
*/
MagickPrivate unsigned char *NTRegistryKeyLookup(const char *subkey)
{
  char
    package_key[MaxTextExtent] = "";

  unsigned char
    *value;

  (void) FormatLocaleString(package_key,MagickPathExtent,
    "SOFTWARE\\%s\\%s\\Q:%d",MagickPackageName,MagickLibVersionText,
    MAGICKCORE_QUANTUM_DEPTH);
  value=NTGetRegistryValue(HKEY_LOCAL_MACHINE,package_key,0,subkey);
  if (value == (unsigned char *) NULL)
    value=NTGetRegistryValue(HKEY_CURRENT_USER,package_key,0,subkey);
  return(value);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T R e m o v e W i d e                                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTRemoveWide() removes the specified file.
%
%  The format of the NTRemoveWide method is:
%
%      int NTRemoveWide(const char *path)
%
%  A description of each parameter follows:
%
%    o path: the file path.
%
*/
MagickExport int NTRemoveWide(const char *path)
{
  int
    status;

  wchar_t
    *path_wide;

  path_wide=NTCreateWidePath(path);
  if (path_wide == (wchar_t *) NULL)
    return(-1);
  status=_wremove(path_wide);
  path_wide=(wchar_t *) RelinquishMagickMemory(path_wide);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T R e n a m e W i d e                                                   %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTRenameWide() renames a file.
%
%  The format of the NTRenameWide method is:
%
%      int NTRenameWide(const char *source, const char *destination)
%
%  A description of each parameter follows:
%
%    o source: the source file path.
%
%    o destination: the destination file path.
%
*/
MagickExport int NTRenameWide(const char* source, const char* destination)
{
 int
   status;

  wchar_t
    *destination_wide,
    *source_wide;

  source_wide=NTCreateWidePath(source);
  if (source_wide == (wchar_t *) NULL)
    return(-1);
  destination_wide=NTCreateWidePath(destination);
  if (destination_wide == (wchar_t *) NULL)
    {
      source_wide=(wchar_t *) RelinquishMagickMemory(source_wide);
      return(-1);
    }
  status=_wrename(source_wide,destination_wide);
  destination_wide=(wchar_t *) RelinquishMagickMemory(destination_wide);
  source_wide=(wchar_t *) RelinquishMagickMemory(source_wide);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T R e p o r t E v e n t                                                 %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTReportEvent() reports an event.
%
%  The format of the NTReportEvent method is:
%
%      MagickBooleanType NTReportEvent(const char *event,
%        const MagickBooleanType error)
%
%  A description of each parameter follows:
%
%    o event: the event.
%
%    o error: MagickTrue the event is an error.
%
*/
MagickPrivate MagickBooleanType NTReportEvent(const char *event,
  const MagickBooleanType error)
{
  const char
    *events[1];

  HANDLE
    handle;

  WORD
    type;

  handle=RegisterEventSource(NULL,MAGICKCORE_PACKAGE_NAME);
  if (handle == NULL)
    return(MagickFalse);
  events[0]=event;
  type=error ? EVENTLOG_ERROR_TYPE : EVENTLOG_WARNING_TYPE;
  ReportEvent(handle,type,0,0,NULL,1,0,events,NULL);
  DeregisterEventSource(handle);
  return(MagickTrue);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T R e s o u r c e T o B l o b                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTResourceToBlob() returns a blob containing the contents of the resource
%  in the current executable specified by the id parameter. This currently
%  used to retrieve MGK files tha have been embedded into the various command
%  line utilities.
%
%  The format of the NTResourceToBlob method is:
%
%      unsigned char *NTResourceToBlob(const char *id)
%
%  A description of each parameter follows:
%
%    o id: Specifies a string that identifies the resource.
%
*/
MagickPrivate unsigned char *NTResourceToBlob(const char *id)
{

#ifndef MAGICKCORE_LIBRARY_NAME
  char
    path[MaxTextExtent];
#endif

  DWORD
    length;

  HGLOBAL
    global;

  HMODULE
    handle;

  HRSRC
    resource;

  unsigned char
    *blob,
    *value;

  assert(id != (const char *) NULL);
  if (IsEventLogging() != MagickFalse)
    (void) LogMagickEvent(TraceEvent,GetMagickModule(),"%s",id);
#ifdef MAGICKCORE_LIBRARY_NAME
  handle=GetModuleHandle(MAGICKCORE_LIBRARY_NAME);
#else
  (void) FormatLocaleString(path,MaxTextExtent,"%s%s%s",GetClientPath(),
    DirectorySeparator,GetClientName());
  if (IsPathAccessible(path) != MagickFalse)
    handle=GetModuleHandle(path);
  else
    handle=GetModuleHandle(0);
#endif
  if (!handle)
    return((unsigned char *) NULL);
  resource=FindResource(handle,id,"IMAGEMAGICK");
  if (!resource)
    return((unsigned char *) NULL);
  global=LoadResource(handle,resource);
  if (!global)
    return((unsigned char *) NULL);
  length=SizeofResource(handle,resource);
  value=(unsigned char *) LockResource(global);
  if (!value)
    {
      FreeResource(global);
      return((unsigned char *) NULL);
    }
  blob=(unsigned char *) AcquireQuantumMemory(length+MaxTextExtent,
    sizeof(*blob));
  if (blob != (unsigned char *) NULL)
    {
      (void) memcpy(blob,value,length);
      blob[length]='\0';
    }
  UnlockResource(global);
  FreeResource(global);
  return(blob);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   NT S e t F i l e T i m e s t a m p                                        %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTSetFileTimestamp() sets the file timestamps for a specified file.
%
%  The format of the NTSetFileTimestamp method is:
%
%      int NTSetFileTimestamp(const char *path, struct stat *attributes)
%
%  A description of each parameter follows:
%
%    o path: the file path.
%
%    o attributes: the file attributes.
%
*/
MagickExport int NTSetFileTimestamp(const char *path, struct stat *attributes)
{
  HANDLE
    handle;

  int
    status;

  wchar_t
    *path_wide;

  status=(-1);
  path_wide=NTCreateWidePath(path);
  if (path_wide == (WCHAR *) NULL)
    return(status);
  handle=CreateFileW(path_wide,FILE_WRITE_ATTRIBUTES,FILE_SHARE_WRITE |
    FILE_SHARE_READ,NULL,OPEN_EXISTING,0,NULL);
  if (handle != (HANDLE) NULL)
    {
      FILETIME
        creation_time,
        last_access_time,
        last_write_time;

      ULARGE_INTEGER
        date_time;

      date_time.QuadPart=(ULONGLONG) (attributes->st_ctime*10000000LL)+
        116444736000000000LL;
      creation_time.dwLowDateTime=date_time.LowPart;
      creation_time.dwHighDateTime=date_time.HighPart;
      date_time.QuadPart=(ULONGLONG) (attributes->st_atime*10000000LL)+
        116444736000000000LL;
      last_access_time.dwLowDateTime=date_time.LowPart;
      last_access_time.dwHighDateTime=date_time.HighPart;
      date_time.QuadPart=(ULONGLONG) (attributes->st_mtime*10000000LL)+
        116444736000000000LL;
      last_write_time.dwLowDateTime=date_time.LowPart;
      last_write_time.dwHighDateTime=date_time.HighPart;
      status=SetFileTime(handle,&creation_time,&last_access_time,&last_write_time);
      CloseHandle(handle);
      status=0;
    }
  path_wide=(WCHAR *) RelinquishMagickMemory(path_wide);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T S e t S e a r c h P a t h                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTSetSearchPath() sets the current locations that the subsystem should
%  look at to find dynamically loadable modules.
%
%  The format of the NTSetSearchPath method is:
%
%      int NTSetSearchPath(const char *path)
%
%  A description of each parameter follows:
%
%    o path: Specifies a pointer to string representing the search path
%      for DLL's that can be dynamically loaded.
%
*/
MagickPrivate int NTSetSearchPath(const char *path)
{
#if defined(MAGICKCORE_LTDL_DELEGATE)
  lt_dlsetsearchpath(path);
#else
  if (lt_slsearchpath != (char *) NULL)
    lt_slsearchpath=DestroyString(lt_slsearchpath);
  if (path != (char *) NULL)
    lt_slsearchpath=AcquireString(path);
#endif
  return(0);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T S t a t W i d e                                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTStatWide() gets the file attributes for a specified file.
%
%  The format of the NTStatWide method is:
%
%      int NTStatWide(const char *path,struct stat *attributes)
%
%  A description of each parameter follows:
%
%    o path: the file path.
%
%    o attributes: the file attributes.
%
*/
MagickExport int NTStatWide(const char *path,struct stat *attributes)
{
  int
    status;

  wchar_t
    *path_wide;

  path_wide=NTCreateWidePath(path);
  if (path_wide == (WCHAR *) NULL)
    return(-1);
  status=wstat(path_wide,attributes);
  path_wide=(WCHAR *) RelinquishMagickMemory(path_wide);
  return(status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T S y s t e m C o m m a n d                                             %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTSystemCommand() executes the specified command and waits until it
%  terminates.  The returned value is the exit status of the command.
%
%  The format of the NTSystemCommand method is:
%
%      int NTSystemCommand(MagickFalse,const char *command)
%
%  A description of each parameter follows:
%
%    o command: This string is the command to execute.
%
%    o output: an optional buffer to store the output from stderr/stdout.
%
*/
MagickPrivate int NTSystemCommand(const char *command,char *output)
{
#define CleanupOutputHandles \
  if (read_output != (HANDLE) NULL) \
    { \
       CloseHandle(read_output); \
       read_output=(HANDLE) NULL; \
       CloseHandle(write_output); \
       write_output=(HANDLE) NULL; \
    }

#define CopyLastError \
  last_error=GetLastError(); \
  if (output != (char *) NULL) \
    { \
      error=NTGetLastErrorMessage(last_error); \
      if (error != (char *) NULL) \
        { \
          CopyMagickString(output,error,MaxTextExtent); \
          error=DestroyString(error); \
        } \
    }

  char
    *error,
    local_command[MaxTextExtent];

  DWORD
    child_status,
    last_error;

  int
    status;

  MagickBooleanType
    asynchronous;

  HANDLE
    read_output,
    write_output;

  PROCESS_INFORMATION
    process_info;

  size_t
    output_offset;

  STARTUPINFO
    startup_info;

  if (command == (char *) NULL)
    return(-1);
  read_output=(HANDLE) NULL;
  write_output=(HANDLE) NULL;
  GetStartupInfo(&startup_info);
  startup_info.dwFlags=STARTF_USESHOWWINDOW;
  startup_info.wShowWindow=SW_SHOWMINNOACTIVE;
  (void) CopyMagickString(local_command,command,MaxTextExtent);
  asynchronous=command[strlen(command)-1] == '&' ? MagickTrue : MagickFalse;
  if (asynchronous != MagickFalse)
    {
      local_command[strlen(command)-1]='\0';
      startup_info.wShowWindow=SW_SHOWDEFAULT;
    }
  else
    {
      if (command[strlen(command)-1] == '|')
        local_command[strlen(command)-1]='\0';
      else
        startup_info.wShowWindow=SW_HIDE;
      read_output=(HANDLE) NULL;
      if (output != (char *) NULL)
        {
          if (CreatePipe(&read_output,&write_output,NULL,0))
            {
              if (SetHandleInformation(write_output,HANDLE_FLAG_INHERIT,
                  HANDLE_FLAG_INHERIT))
                {
                  startup_info.dwFlags|=STARTF_USESTDHANDLES;
                  startup_info.hStdOutput=write_output;
                  startup_info.hStdError=write_output;
                }
              else
                CleanupOutputHandles;
            }
          else
            read_output=(HANDLE) NULL;
        }
    }
  status=CreateProcess((LPCTSTR) NULL,local_command,(LPSECURITY_ATTRIBUTES)
    NULL,(LPSECURITY_ATTRIBUTES) NULL,(BOOL) TRUE,(DWORD)
    NORMAL_PRIORITY_CLASS,(LPVOID) NULL,(LPCSTR) NULL,&startup_info,
    &process_info);
  if (status == 0)
    {
      CopyLastError;
      CleanupOutputHandles;
      return(last_error == ERROR_FILE_NOT_FOUND ? 127 : -1);
    }
  if (output != (char *) NULL)
    *output='\0';
  if (asynchronous != MagickFalse)
    return(status == 0);
  output_offset=0;
  status=STATUS_TIMEOUT;
  while (status == STATUS_TIMEOUT)
  {
    DWORD
      size;

    status=WaitForSingleObject(process_info.hProcess,1000);
    size=0;
    if (read_output != (HANDLE) NULL)
      if (!PeekNamedPipe(read_output,NULL,0,NULL,&size,NULL))
        break;
    while (size > 0)
    {
      char
        buffer[MagickPathExtent];

      DWORD
        bytes_read;

      if (ReadFile(read_output,buffer,MagickPathExtent-1,&bytes_read,NULL))
        {
          size_t
            count;

          count=MagickMin(MagickPathExtent-output_offset,
            (size_t) bytes_read+1);
          if (count > 0)
            {
              CopyMagickString(output+output_offset,buffer,count);
              output_offset+=count-1;
            }
        }
      if (!PeekNamedPipe(read_output,NULL,0,NULL,&size,NULL))
        break;
    }
  }
  if (status != WAIT_OBJECT_0)
    {
      CopyLastError;
      CleanupOutputHandles;
      return(status);
    }
  status=GetExitCodeProcess(process_info.hProcess,&child_status);
  if (status == 0)
    {
      CopyLastError;
      CleanupOutputHandles;
      return(-1);
    }
  CloseHandle(process_info.hProcess);
  CloseHandle(process_info.hThread);
  CleanupOutputHandles;
  return((int) child_status);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T S y s t e m C o n i f i g u r a t i o n                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTSystemConfiguration() provides a way for the application to determine
%  values for system limits or options at runtime.
%
%  The format of the exit method is:
%
%      ssize_t NTSystemConfiguration(int name)
%
%  A description of each parameter follows:
%
%    o name: _SC_PAGE_SIZE or _SC_PHYS_PAGES.
%
*/
MagickPrivate ssize_t NTSystemConfiguration(int name)
{
  switch (name)
  {
    case _SC_PAGE_SIZE:
    {
      SYSTEM_INFO
        system_info;

      GetSystemInfo(&system_info);
      return(system_info.dwPageSize);
    }
    case _SC_PHYS_PAGES:
    {
      MEMORYSTATUSEX
        status;

      SYSTEM_INFO
        system_info;

      status.dwLength=sizeof(status);
      if (GlobalMemoryStatusEx(&status) == 0)
        return(0L);
      GetSystemInfo(&system_info);
      return((ssize_t) status.ullTotalPhys/system_info.dwPageSize);
    }
    case _SC_OPEN_MAX:
      return(2048);
    default:
      break;
  }
  return(-1);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T T r u n c a t e F i l e                                               %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTTruncateFile() truncates a file to a specified length.
%
%  The format of the NTTruncateFile method is:
%
%      int NTTruncateFile(int file,off_t length)
%
%  A description of each parameter follows:
%
%    o file: the file.
%
%    o length: the file length.
%
*/
MagickPrivate int NTTruncateFile(int file,off_t length)
{
  DWORD
    file_pointer;

  HANDLE
    file_handle;

  long
    high,
    low;

  file_handle=(HANDLE) _get_osfhandle(file);
  if (file_handle == INVALID_HANDLE_VALUE)
    return(-1);
  low=(long) (length & 0xffffffffUL);
  high=(long) ((((MagickOffsetType) length) >> 32) & 0xffffffffUL);
  file_pointer=SetFilePointer(file_handle,low,&high,FILE_BEGIN);
  if ((file_pointer == 0xFFFFFFFF) && (GetLastError() != NO_ERROR))
    return(-1);
  if (SetEndOfFile(file_handle) == 0)
    return(-1);
  return(0);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
+  N T U n m a p M e m o r y                                                  %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTUnmapMemory() emulates the Unix munmap method.
%
%  The format of the NTUnmapMemory method is:
%
%      int NTUnmapMemory(void *map,size_t length)
%
%  A description of each parameter follows:
%
%    o map: the address of the binary large object.
%
%    o length: the length of the binary large object.
%
*/
MagickPrivate int NTUnmapMemory(void *map,size_t length)
{
  (void) length;
  if (UnmapViewOfFile(map) == 0)
    return(-1);
  return(0);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T U s e r T i m e                                                       %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTUserTime() returns the total time the process has been scheduled (e.g.
%  seconds) since the last call to StartTimer().
%
%  The format of the UserTime method is:
%
%      double NTUserTime(void)
%
*/
MagickPrivate double NTUserTime(void)
{
  DWORD
    status;

  FILETIME
    create_time,
    exit_time;

  OSVERSIONINFO
    OsVersionInfo;

  union
  {
    FILETIME
      filetime;

    __int64
      filetime64;
  } kernel_time;

  union
  {
    FILETIME
      filetime;

    __int64
      filetime64;
  } user_time;

  OsVersionInfo.dwOSVersionInfoSize=sizeof(OSVERSIONINFO);
  GetVersionEx(&OsVersionInfo);
  if (OsVersionInfo.dwPlatformId != VER_PLATFORM_WIN32_NT)
    return(NTElapsedTime());
  status=GetProcessTimes(GetCurrentProcess(),&create_time,&exit_time,
    &kernel_time.filetime,&user_time.filetime);
  if (status != TRUE)
    return(0.0);
  return((double) 1.0e-7*(kernel_time.filetime64+user_time.filetime64));
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T W a r n i n g H a n d l e r                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTWarningHandler() displays a warning reason.
%
%  The format of the NTWarningHandler method is:
%
%      void NTWarningHandler(const ExceptionType severity,const char *reason,
%        const char *description)
%
%  A description of each parameter follows:
%
%    o severity: Specifies the numeric warning category.
%
%    o reason: Specifies the reason to display before terminating the
%      program.
%
%    o description: Specifies any description to the reason.
%
*/
MagickPrivate void NTWarningHandler(const ExceptionType severity,
  const char *reason,const char *description)
{
  char
    buffer[2*MaxTextExtent];

  (void) severity;
  if (reason == (char *) NULL)
    return;
  if (description == (char *) NULL)
    (void) FormatLocaleString(buffer,MaxTextExtent,"%s: %s.\n",GetClientName(),
      reason);
  else
    (void) FormatLocaleString(buffer,MaxTextExtent,"%s: %s (%s).\n",
      GetClientName(),reason,description);
  (void) MessageBox(NULL,buffer,"ImageMagick Warning",MB_OK | MB_TASKMODAL |
    MB_SETFOREGROUND | MB_ICONINFORMATION);
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T W i n d o w s G e n e s i s                                           %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTWindowsGenesis() initializes the MagickCore Windows environment.
%
%  The format of the NTWindowsGenesis method is:
%
%      void NTWindowsGenesis(void)
%
*/
MagickPrivate void NTWindowsGenesis(void)
{
  char
    *mode;

  mode=GetEnvironmentValue("MAGICK_ERRORMODE");
  if (mode != (char *) NULL)
    {
      (void) SetErrorMode(StringToInteger(mode));
      mode=DestroyString(mode);
    }
#if defined(_DEBUG) && !defined(__MINGW32__)
  if (IsEventLogging() != MagickFalse)
    {
      int
        debug;

      debug=_CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
      //debug |= _CRTDBG_CHECK_ALWAYS_DF;
      debug |= _CRTDBG_DELAY_FREE_MEM_DF;
      debug |= _CRTDBG_LEAK_CHECK_DF;
      (void) _CrtSetDbgFlag(debug);

      //_ASSERTE(_CrtCheckMemory());

      //_CrtSetBreakAlloc(42);
    }
#endif
#if defined(MAGICKCORE_INSTALLED_SUPPORT)
  {
    unsigned char
      *path;

    path=NTRegistryKeyLookup("LibPath");
    if (path != (unsigned char *) NULL)
      {
        wchar_t
          *lib_path;

        lib_path=NTCreateWidePath((const char *) path);
        if (lib_path != (wchar_t *) NULL)
          {
            SetDllDirectoryW(lib_path);
            lib_path=(wchar_t *) RelinquishMagickMemory(lib_path);
          }
        path=(unsigned char *) RelinquishMagickMemory(path);
      }
  }
#endif
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%                                                                             %
%                                                                             %
%                                                                             %
%   N T W i n d o w s T e r m i n u s                                         %
%                                                                             %
%                                                                             %
%                                                                             %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%  NTWindowsTerminus() terminates the MagickCore Windows environment.
%
%  The format of the NTWindowsTerminus method is:
%
%      void NTWindowsTerminus(void)
%
*/
MagickPrivate void NTWindowsTerminus(void)
{
  NTGhostscriptUnLoadDLL();
  if (winsock_semaphore == (SemaphoreInfo *) NULL)
    ActivateSemaphoreInfo(&winsock_semaphore);
  LockSemaphoreInfo(winsock_semaphore);
  if (wsaData != (WSADATA *) NULL)
    {
      WSACleanup();
      wsaData=(WSADATA *) RelinquishMagickMemory((void *) wsaData);
    }
  UnlockSemaphoreInfo(winsock_semaphore);
  DestroySemaphoreInfo(&winsock_semaphore);
}
#endif
