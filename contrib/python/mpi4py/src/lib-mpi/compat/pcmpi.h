#ifndef PyMPI_COMPAT_PCMPI_H
#define PyMPI_COMPAT_PCMPI_H

/* ---------------------------------------------------------------- */

static int PyMPI_PCMPI_MPI_Request_get_status(MPI_Request request,
                                              int *flag,
                                              MPI_Status *status)
{
  MPI_Status sts;
  if (!status ||
      status == MPI_STATUS_IGNORE ||
      status == MPI_STATUSES_IGNORE) status = &sts;
  return MPI_Request_get_status(request, flag, status);
}
#undef  MPI_Request_get_status
#define MPI_Request_get_status PyMPI_PCMPI_MPI_Request_get_status

/* ---------------------------------------------------------------- */

static int PyMPI_PCMPI_MPI_Win_get_attr(MPI_Win win,
                                        int keyval,
                                        void *attrval,
                                        int *flag)
{
  int ierr;
  ierr = MPI_Win_get_attr(win, keyval, attrval, flag);
  if (ierr == MPI_SUCCESS && keyval == MPI_WIN_BASE && *flag)
    *((void **)attrval) = **((void ***)attrval);
  return ierr;
}
#undef  MPI_Win_get_attr
#define MPI_Win_get_attr PyMPI_PCMPI_MPI_Win_get_attr

/* ---------------------------------------------------------------- */

#ifndef PCMPI_DLOPEN_LIBMPI
#define PCMPI_DLOPEN_LIBMPI 1
#endif

#if PCMPI_DLOPEN_LIBMPI
#if HAVE_DLOPEN

#include "../../dynload.h"

static void PyMPI_PCMPI_dlopen_libmpi(void)
{
  void *handle1 = (void *)0;
  void *handle2 = (void *)0;
  int mode = RTLD_NOW | RTLD_GLOBAL;
  #ifdef RTLD_NOLOAD
  mode |= RTLD_NOLOAD;
  #endif
#if defined(__APPLE__)
  if (!handle1) handle1 = dlopen("libmpi.2.dylib", mode);
  if (!handle1) handle1 = dlopen("libmpi.1.dylib", mode);
  if (!handle1) handle1 = dlopen("libmpi.dylib", mode);
  if (!handle2) handle2 = dlopen("libmpio.2.dylib", mode);
  if (!handle2) handle2 = dlopen("libmpio.1.dylib", mode);
  if (!handle2) handle2 = dlopen("libmpio.dylib", mode);
#else
  if (!handle1) handle1 = dlopen("libmpi.so.2", mode);
  if (!handle1) handle1 = dlopen("libmpi.so.1", mode);
  if (!handle1) handle1 = dlopen("libmpi.so", mode);
  if (!handle2) handle2 = dlopen("libmpio.so.2", mode);
  if (!handle2) handle2 = dlopen("libmpio.so.1", mode);
  if (!handle2) handle2 = dlopen("libmpio.so", mode);
#endif
}

static int PyMPI_PCMPI_MPI_Init(int *argc, char ***argv)
{
  PyMPI_PCMPI_dlopen_libmpi();
  return MPI_Init(argc, argv);
}
#undef  MPI_Init
#define MPI_Init PyMPI_PCMPI_MPI_Init

static int PyMPI_PCMPI_MPI_Init_thread(int *argc, char ***argv,
                                       int required, int *provided)
{
  PyMPI_PCMPI_dlopen_libmpi();
  return MPI_Init_thread(argc, argv, required, provided);
}
#undef  MPI_Init_thread
#define MPI_Init_thread PyMPI_PCMPI_MPI_Init_thread

#endif /* !HAVE_DLOPEN */
#endif /* !PCMPI_DLOPEN_LIBMPI */

/* ---------------------------------------------------------------- */

#endif /* !PyMPI_COMPAT_PCMPI_H */
