#ifndef PyMPI_COMPAT_MPICH1_H
#define PyMPI_COMPAT_MPICH1_H

/* ---------------------------------------------------------------- */

static int    PyMPI_MPICH1_argc    = 0;
static char  *PyMPI_MPICH1_argv[1] = {(char*)0};

static void PyMPI_MPICH1_FixArgs(int **argc, char ****argv)
{
  if (argc[0] && argv[0]) return;
  argc[0] = (int *)    &PyMPI_MPICH1_argc;
  argv[0] = (char ***) &PyMPI_MPICH1_argv;
}

static int PyMPI_MPICH1_MPI_Init(int *argc, char ***argv)
{
  PyMPI_MPICH1_FixArgs(&argc, &argv);
  return MPI_Init(argc, argv);
}
#undef  MPI_Init
#define MPI_Init PyMPI_MPICH1_MPI_Init

static int PyMPI_MPICH1_MPI_Init_thread(int *argc, char ***argv,
                                        int required, int *provided)
{
  PyMPI_MPICH1_FixArgs(&argc, &argv);
  return MPI_Init_thread(argc, argv, required, provided);
}
#undef  MPI_Init_thread
#define MPI_Init_thread PyMPI_MPICH1_MPI_Init_thread

/* ---------------------------------------------------------------- */

#undef  MPI_SIGNED_CHAR
#define MPI_SIGNED_CHAR MPI_CHAR

/* ---------------------------------------------------------------- */

static int PyMPI_MPICH1_MPI_Status_set_elements(MPI_Status *status,
                                                MPI_Datatype datatype,
                                                int count)
{
  if (datatype == MPI_DATATYPE_NULL) return MPI_ERR_TYPE;
  return MPI_Status_set_elements(status, datatype, count);
}
#undef  MPI_Status_set_elements
#define MPI_Status_set_elements PyMPI_MPICH1_MPI_Status_set_elements

/* ---------------------------------------------------------------- */

static int PyMPI_MPICH1_MPI_Sendrecv(void *sendbuf,
                                     int sendcount,
                                     MPI_Datatype sendtype,
                                     int dest,
                                     int sendtag,
                                     void *recvbuf,
                                     int recvcount,
                                     MPI_Datatype recvtype,
                                     int source,
                                     int recvtag,
                                     MPI_Comm comm,
                                     MPI_Status *status)
{
  MPI_Status dummy;
  if (status == MPI_STATUS_IGNORE) status = &dummy;
  return MPI_Sendrecv(sendbuf, sendcount, sendtype, dest,   sendtag,
                      recvbuf, recvcount, recvtype, source, recvtag,
                      comm, status);
}
#undef  MPI_Sendrecv
#define MPI_Sendrecv PyMPI_MPICH1_MPI_Sendrecv

static int PyMPI_MPICH1_MPI_Sendrecv_replace(void *buf,
                                             int count,
                                             MPI_Datatype datatype,
                                             int dest,
                                             int sendtag,
                                             int source,
                                             int recvtag,
                                             MPI_Comm comm,
                                             MPI_Status *status)
{
  MPI_Status dummy;
  if (status == MPI_STATUS_IGNORE) status = &dummy;
  return MPI_Sendrecv_replace(buf, count, datatype,
                              dest, sendtag, source, recvtag,
                              comm, status);
}
#undef  MPI_Sendrecv_replace
#define MPI_Sendrecv_replace PyMPI_MPICH1_MPI_Sendrecv_replace

/* ---------------------------------------------------------------- */

#ifndef PyMPI_HAVE_MPI_Win
#undef  MPI_Win_c2f
#define MPI_Win_c2f(win) ((MPI_Fint)0)
#undef  MPI_Win_f2c
#define MPI_Win_f2c(win) MPI_WIN_NULL
#endif

/* ---------------------------------------------------------------- */

#if defined(__cplusplus)
extern "C" {
#endif
extern void *MPIR_ToPointer(int);
#if defined(__cplusplus)
}
#endif

#if defined(ROMIO_VERSION)

#if defined(__cplusplus)
extern "C" {
#endif

struct MPIR_Errhandler {
  unsigned long        cookie;
  MPI_Handler_function *routine;
  int                  ref_count;
};

#if defined(__cplusplus)
}
#endif

static int PyMPI_MPICH1_MPI_File_get_errhandler(MPI_File file,
                                                MPI_Errhandler *errhandler)
{
  int ierr = MPI_SUCCESS;
  ierr = MPI_File_get_errhandler(file, errhandler);
  if (ierr != MPI_SUCCESS) return ierr;
  if (errhandler == 0) return ierr; /* just in case  */
  /* manage reference counting */
  if (*errhandler != MPI_ERRHANDLER_NULL) {
    struct MPIR_Errhandler *eh =
      (struct MPIR_Errhandler *) MPIR_ToPointer(*errhandler);
    if (eh) eh->ref_count++;
  }
  return MPI_SUCCESS;
}

static int PyMPI_MPICH1_MPI_File_set_errhandler(MPI_File file,
                                                MPI_Errhandler errhandler)
{
  int ierr = MPI_SUCCESS;
  MPI_Errhandler previous = MPI_ERRHANDLER_NULL;
  ierr = MPI_File_get_errhandler(file, &previous);
  if (ierr != MPI_SUCCESS) return ierr;
  ierr = MPI_File_set_errhandler(file, errhandler);
  if (ierr != MPI_SUCCESS) return ierr;
  /* manage reference counting */
  if (previous != MPI_ERRHANDLER_NULL) {
    struct MPIR_Errhandler *eh =
      (struct MPIR_Errhandler *) MPIR_ToPointer(previous);
    if (eh) eh->ref_count--;
  }
  if (errhandler != MPI_ERRHANDLER_NULL) {
    struct MPIR_Errhandler *eh =
      (struct MPIR_Errhandler *) MPIR_ToPointer(errhandler);
    if (eh) eh->ref_count++;
  }
  return MPI_SUCCESS;
}

#undef  MPI_File_get_errhandler
#define MPI_File_get_errhandler PyMPI_MPICH1_MPI_File_get_errhandler

#undef  MPI_File_set_errhandler
#define MPI_File_set_errhandler PyMPI_MPICH1_MPI_File_set_errhandler

#endif /* !ROMIO_VERSION */

/* ---------------------------------------------------------------- */

#undef  MPI_ERR_KEYVAL
#define MPI_ERR_KEYVAL MPI_ERR_OTHER

#undef  MPI_MAX_OBJECT_NAME
#define MPI_MAX_OBJECT_NAME MPI_MAX_NAME_STRING

/* ---------------------------------------------------------------- */

#endif /* !PyMPI_COMPAT_MPICH1_H */

/*
  Local variables:
  c-basic-offset: 2
  indent-tabs-mode: nil
  End:
*/
