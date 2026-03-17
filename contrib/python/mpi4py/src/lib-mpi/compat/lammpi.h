#ifndef PyMPI_COMPAT_LAMMPI_H
#define PyMPI_COMPAT_LAMMPI_H

/* ---------------------------------------------------------------- */

static int PyMPI_LAMMPI_MPI_Info_free(MPI_Info *info)
{
  if (info == 0)              return MPI_ERR_ARG;
  if (*info == MPI_INFO_NULL) return MPI_ERR_ARG;
  return MPI_Info_free(info);
}
#undef  MPI_Info_free
#define MPI_Info_free PyMPI_LAMMPI_MPI_Info_free

/* ---------------------------------------------------------------- */

static int PyMPI_LAMMPI_MPI_Cancel(MPI_Request *request)
{
  int ierr = MPI_SUCCESS;
  ierr = MPI_Cancel(request);
  if (ierr == MPI_ERR_ARG) {
    if (request != 0 &&  *request == MPI_REQUEST_NULL)
      ierr = MPI_ERR_REQUEST;
  }
  return ierr;
}
#undef  MPI_Cancel
#define MPI_Cancel PyMPI_LAMMPI_MPI_Cancel

static int PyMPI_LAMMPI_MPI_Comm_disconnect(MPI_Comm *comm)
{
  if (comm == 0)               return MPI_ERR_ARG;
  if (*comm == MPI_COMM_NULL)  return MPI_ERR_COMM;
  if (*comm == MPI_COMM_SELF)  return MPI_ERR_COMM;
  if (*comm == MPI_COMM_WORLD) return MPI_ERR_COMM;
  return MPI_Comm_disconnect(comm);
}
#undef  MPI_Comm_disconnect
#define MPI_Comm_disconnect PyMPI_LAMMPI_MPI_Comm_disconnect

/* ---------------------------------------------------------------- */

#if defined(__cplusplus)
extern "C" {
#endif

struct _errhdl {
  void  (*eh_func)(void);
  int   eh_refcount;
  int   eh_f77handle;
  int   eh_flags;
};

#if defined(__cplusplus)
}
#endif

static int PyMPI_LAMMPI_Errhandler_free(MPI_Errhandler *errhandler)
{
  if (errhandler == 0) return MPI_ERR_ARG;
  if (*errhandler == MPI_ERRORS_RETURN ||
      *errhandler == MPI_ERRORS_ARE_FATAL) {
    struct _errhdl *eh = (struct _errhdl *) (*errhandler);
    eh->eh_refcount--;
    *errhandler = MPI_ERRHANDLER_NULL;
    return MPI_SUCCESS;
  } else {
    return MPI_Errhandler_free(errhandler);
  }
}
#undef  MPI_Errhandler_free
#define MPI_Errhandler_free PyMPI_LAMMPI_Errhandler_free

/* -- */

static int PyMPI_LAMMPI_MPI_Comm_get_errhandler(MPI_Comm comm,
                                                MPI_Errhandler *errhandler)
{
  int ierr = MPI_SUCCESS;
  if (comm == MPI_COMM_NULL) return MPI_ERR_COMM;
  if (errhandler == 0) return MPI_ERR_ARG;
  /* get error handler */
  ierr = MPI_Errhandler_get(comm, errhandler);
  if (ierr != MPI_SUCCESS) return ierr;
  return MPI_SUCCESS;
}
#undef  MPI_Errhandler_get
#define MPI_Errhandler_get PyMPI_LAMMPI_MPI_Comm_get_errhandler
#undef MPI_Comm_get_errhandler
#define MPI_Comm_get_errhandler PyMPI_LAMMPI_MPI_Comm_get_errhandler

static int PyMPI_LAMMPI_MPI_Comm_set_errhandler(MPI_Comm comm,
                                                MPI_Errhandler errhandler)
{
  int ierr = MPI_SUCCESS, ierr2 = MPI_SUCCESS;
  MPI_Errhandler previous = MPI_ERRHANDLER_NULL;
  if (comm       == MPI_COMM_NULL)       return MPI_ERR_COMM;
  if (errhandler == MPI_ERRHANDLER_NULL) return MPI_ERR_ARG;
  /* get previous error handler*/
  ierr2 = MPI_Errhandler_get(comm, &previous);
  if (ierr2 != MPI_SUCCESS) return ierr2;
  /* increment reference counter */
  if (errhandler != MPI_ERRHANDLER_NULL) {
    struct _errhdl *eh = (struct _errhdl *) (errhandler);
    eh->eh_refcount++;
  }
  /* set error handler */
  ierr = MPI_Errhandler_set(comm, errhandler);
  /* decrement reference counter */
  if (errhandler != MPI_ERRHANDLER_NULL) {
    struct _errhdl *eh = (struct _errhdl *) (errhandler);
    eh->eh_refcount--;
  }
  /* free previous error handler*/
  if (previous != MPI_ERRHANDLER_NULL) {
    ierr2 = MPI_Errhandler_free(&previous);
  }
  if (ierr  != MPI_SUCCESS) return ierr;
  if (ierr2 != MPI_SUCCESS) return ierr2;
  return MPI_SUCCESS;
}
#undef  MPI_Errhandler_set
#define MPI_Errhandler_set PyMPI_LAMMPI_MPI_Comm_set_errhandler
#undef  MPI_Comm_set_errhandler
#define MPI_Comm_set_errhandler PyMPI_LAMMPI_MPI_Comm_set_errhandler

/* -- */

static int PyMPI_LAMMPI_MPI_Win_get_errhandler(MPI_Win win,
                                               MPI_Errhandler *errhandler)
{
  int ierr = MPI_SUCCESS;
  if (win == MPI_WIN_NULL) return MPI_ERR_WIN;
  if (errhandler == 0) return MPI_ERR_ARG;
  /* get error handler */
  ierr = MPI_Win_get_errhandler(win, errhandler);
  if (ierr != MPI_SUCCESS) return ierr;
  /* increment reference counter */
  if (*errhandler != MPI_ERRHANDLER_NULL) {
    struct _errhdl *eh = (struct _errhdl *) (*errhandler);
    eh->eh_refcount++;
  }
  return MPI_SUCCESS;
}
#undef  MPI_Win_get_errhandler
#define MPI_Win_get_errhandler PyMPI_LAMMPI_MPI_Win_get_errhandler

static int PyMPI_LAMMPI_MPI_Win_set_errhandler(MPI_Win win,
                                               MPI_Errhandler errhandler)
{
  int ierr = MPI_SUCCESS, ierr2 = MPI_SUCCESS;
  MPI_Errhandler previous = MPI_ERRHANDLER_NULL;
  if (win        == MPI_WIN_NULL)         return MPI_ERR_WIN;
  if (errhandler == MPI_ERRHANDLER_NULL)  return MPI_ERR_ARG;
  /* get previous error handler*/
  ierr2 = MPI_Win_get_errhandler(win, &previous);
  if (ierr2 != MPI_SUCCESS) return ierr2;
  /* increment reference counter */
  if (errhandler != MPI_ERRHANDLER_NULL) {
    struct _errhdl *eh = (struct _errhdl *) (errhandler);
    eh->eh_refcount++;
  }
  /* set error handler */
  ierr  = MPI_Win_set_errhandler(win, errhandler);
  /* decrement reference counter */
  if (errhandler != MPI_ERRHANDLER_NULL) {
    struct _errhdl *eh = (struct _errhdl *) (errhandler);
    eh->eh_refcount--;
  }
  /* free previous error handler*/
  if (previous != MPI_ERRHANDLER_NULL) {
    ierr2 = MPI_Errhandler_free(&previous);
  }
  if (ierr  != MPI_SUCCESS) return ierr;
  if (ierr2 != MPI_SUCCESS) return ierr2;
  return MPI_SUCCESS;
}
#undef  MPI_Win_set_errhandler
#define MPI_Win_set_errhandler PyMPI_LAMMPI_MPI_Win_set_errhandler

static int PyMPI_LAMMPI_MPI_Win_create(void *base,
                                       MPI_Aint size,
                                       int disp_unit,
                                       MPI_Info info,
                                       MPI_Comm comm,
                                       MPI_Win *win)
{
  int ierr = MPI_SUCCESS;
  MPI_Errhandler errhandler = MPI_ERRHANDLER_NULL;
  ierr = MPI_Win_create(base, size, disp_unit, info, comm, win);
  if (ierr != MPI_SUCCESS) return ierr;
  ierr = MPI_Win_get_errhandler(*win, &errhandler);
  if (ierr != MPI_SUCCESS) return ierr;
  return MPI_SUCCESS;
}
#undef  MPI_Win_create
#define MPI_Win_create PyMPI_LAMMPI_MPI_Win_create

static int PyMPI_LAMMPI_MPI_Win_free(MPI_Win *win)
{
  int ierr = MPI_SUCCESS, ierr2 = MPI_SUCCESS;
  MPI_Errhandler errhandler = MPI_ERRHANDLER_NULL;
  if (win != 0 && *win != MPI_WIN_NULL ) {
    MPI_Errhandler previous;
    ierr2 = MPI_Win_get_errhandler(*win, &previous);
    if (ierr2 != MPI_SUCCESS) return ierr2;
    errhandler = previous;
    if (previous != MPI_ERRHANDLER_NULL) {
      ierr2 = MPI_Errhandler_free(&previous);
      if (ierr2 != MPI_SUCCESS) return ierr2;
    }
  }
  ierr = MPI_Win_free(win);
  if (errhandler != MPI_ERRHANDLER_NULL) {
    ierr2 = MPI_Errhandler_free(&errhandler);
    if (ierr2 != MPI_SUCCESS) return ierr2;
  }
  if (ierr != MPI_SUCCESS) return ierr;
  return MPI_SUCCESS;
}
#undef  MPI_Win_free
#define MPI_Win_free PyMPI_LAMMPI_MPI_Win_free


/* -- */

#if defined(ROMIO_VERSION)

#if defined(__cplusplus)
extern "C" {
#endif

#define ADIOI_FILE_COOKIE 2487376
#define FDTYPE      int
#define ADIO_Offset MPI_Offset
#define ADIOI_Fns   struct ADIOI_Fns_struct
#define ADIOI_Hints struct ADIOI_Hints_struct
extern MPI_Errhandler ADIOI_DFLT_ERR_HANDLER;

struct ADIOI_FileD {
    int cookie;              /* for error checking */
    FDTYPE fd_sys;              /* system file descriptor */
#ifdef XFS
    int fd_direct;           /* On XFS, this is used for direct I/O;
                                fd_sys is used for buffered I/O */
    int direct_read;         /* flag; 1 means use direct read */
    int direct_write;        /* flag; 1 means use direct write  */
    /* direct I/O attributes */
    unsigned d_mem;          /* data buffer memory alignment */
    unsigned d_miniosz;      /* min xfer size, xfer size multiple,
                                and file seek offset alignment */
    unsigned d_maxiosz;      /* max xfer size */
#endif
    ADIO_Offset fp_ind;      /* individual file pointer in MPI-IO (in bytes)*/
    ADIO_Offset fp_sys_posn; /* current location of the system file-pointer
                                in bytes */
    ADIOI_Fns *fns;          /* struct of I/O functions to use */
    MPI_Comm comm;           /* communicator indicating who called open */
    char *filename;
    int file_system;         /* type of file system */
    int access_mode;
    ADIO_Offset disp;        /* reqd. for MPI-IO */
    MPI_Datatype etype;      /* reqd. for MPI-IO */
    MPI_Datatype filetype;   /* reqd. for MPI-IO */
    int etype_size;          /* in bytes */
    ADIOI_Hints *hints;      /* structure containing fs-indep. info values */
    MPI_Info info;
    int split_coll_count;    /* count of outstanding split coll. ops. */
    char *shared_fp_fname;   /* name of file containing shared file pointer */
    struct ADIOI_FileD *shared_fp_fd;  /* file handle of file
                                         containing shared fp */
    int async_count;         /* count of outstanding nonblocking operations */
    int perm;
    int atomicity;          /* true=atomic, false=nonatomic */
    int iomode;             /* reqd. to implement Intel PFS modes */
    MPI_Errhandler err_handler;
};

#if defined(__cplusplus)
}
#endif

static int PyMPI_LAMMPI_MPI_File_get_errhandler(MPI_File file,
                                                MPI_Errhandler *errhandler)
{
  /* check arguments */
  if (file != MPI_FILE_NULL) {
    struct ADIOI_FileD * fh = (struct ADIOI_FileD *) file;
    if (fh->cookie != ADIOI_FILE_COOKIE)  return MPI_ERR_ARG;
  }
  if (errhandler == 0)  return MPI_ERR_ARG;
  /* get error handler */
  if (file == MPI_FILE_NULL) {
    *errhandler = ADIOI_DFLT_ERR_HANDLER;
  } else {
    struct ADIOI_FileD * fh = (struct ADIOI_FileD *) file;
    *errhandler = fh->err_handler;
  }
  /* increment reference counter */
  if (*errhandler != MPI_ERRHANDLER_NULL) {
    struct _errhdl *eh = (struct _errhdl *) (*errhandler);
    eh->eh_refcount++;
  }
  return MPI_SUCCESS;
}
#undef  MPI_File_get_errhandler
#define MPI_File_get_errhandler PyMPI_LAMMPI_MPI_File_get_errhandler

static int PyMPI_LAMMPI_MPI_File_set_errhandler(MPI_File file,
                                                MPI_Errhandler errhandler)
{
  /* check arguments */
  if (file != MPI_FILE_NULL) {
    struct ADIOI_FileD * fh = (struct ADIOI_FileD *) file;
    if (fh->cookie != ADIOI_FILE_COOKIE)  return MPI_ERR_ARG;
  }
  if (errhandler == MPI_ERRHANDLER_NULL)  return MPI_ERR_ARG;
  if (errhandler != MPI_ERRORS_RETURN &&
      errhandler != MPI_ERRORS_ARE_FATAL) return MPI_ERR_ARG;
  /* increment reference counter */
  if (errhandler != MPI_ERRHANDLER_NULL ) {
    struct _errhdl *eh = (struct _errhdl *) errhandler;
    eh->eh_refcount++;
  }
  /* set error handler */
  if (file == MPI_FILE_NULL) {
    MPI_Errhandler tmp = ADIOI_DFLT_ERR_HANDLER;
    ADIOI_DFLT_ERR_HANDLER = errhandler;
    errhandler = tmp;
  } else {
    struct ADIOI_FileD *fh = (struct ADIOI_FileD *) file;
    MPI_Errhandler tmp = fh->err_handler;
    fh->err_handler = errhandler;
    errhandler = tmp;
  }
  /* decrement reference counter */
  if (errhandler != MPI_ERRHANDLER_NULL ) {
    struct _errhdl *eh = (struct _errhdl *) errhandler;
    eh->eh_refcount--;
  }
  return MPI_SUCCESS;
}
#undef  MPI_File_set_errhandler
#define MPI_File_set_errhandler PyMPI_LAMMPI_MPI_File_set_errhandler

#endif

/* ---------------------------------------------------------------- */

#endif /* !PyMPI_COMPAT_LAMMPI_H */

/*
  Local variables:
  c-basic-offset: 2
  indent-tabs-mode: nil
  End:
*/
