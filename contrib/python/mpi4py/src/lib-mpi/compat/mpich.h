#ifndef PyMPI_COMPAT_MPICH_H
#define PyMPI_COMPAT_MPICH_H
#if defined(MPICH_NUMVERSION)

#if (MPICH_NUMVERSION >= 30400000 && MPICH_NUMVERSION < 40000000)

static int PyMPI_MPICH_MPI_Win_get_attr(MPI_Win win,
                                        int keyval,
                                        void *attrval,
                                        int *flag)
{
  int ierr; static MPI_Aint zero[1] = {0}; zero[0] = 0;
  ierr = MPI_Win_get_attr(win, keyval, attrval, flag); if (ierr) return ierr;
  if (keyval == MPI_WIN_SIZE && flag && *flag && attrval)
    if (**((MPI_Aint**)attrval) == -1) *((void**)attrval) = zero;
  return ierr;
}
#define MPI_Win_get_attr PyMPI_MPICH_MPI_Win_get_attr

#endif

#if (MPICH_NUMVERSION == 30101300)

static int PyMPI_MPICH_MPI_Status_c2f(const MPI_Status *c_status,
                                      MPI_Fint *f_status)
{
  if (c_status == MPI_STATUS_IGNORE ||
      c_status == MPI_STATUSES_IGNORE) return MPI_ERR_OTHER;
  *(MPI_Status *)f_status = *c_status;
  return MPI_SUCCESS;
}
#define MPI_Status_c2f PyMPI_MPICH_MPI_Status_c2f

#endif

#if (MPICH_NUMVERSION < 30100301)

static int PyMPI_MPICH_MPI_Add_error_class(int *errorclass)
{
  int ierr; char errstr[1] = {0};
  ierr = MPI_Add_error_class(errorclass); if (ierr) return ierr;
  return MPI_Add_error_string(*errorclass,errstr);
}
#undef  MPI_Add_error_class
#define MPI_Add_error_class PyMPI_MPICH_MPI_Add_error_class

static int PyMPI_MPICH_MPI_Add_error_code(int errorclass,
                                          int *errorcode)
{
  int ierr; char errstr[1] = {0};
  ierr = MPI_Add_error_code(errorclass,errorcode); if (ierr) return ierr;
  return MPI_Add_error_string(*errorcode,errstr);
}
#undef  MPI_Add_error_code
#define MPI_Add_error_code PyMPI_MPICH_MPI_Add_error_code

#endif

#if (MPICH_NUMVERSION < 30100000)

static int PyMPI_MPICH_MPI_Type_size_x(MPI_Datatype datatype,
                                       MPI_Count *size)
{
  int ierr = MPI_Type_commit(&datatype); if (ierr) return ierr;
  return MPI_Type_size_x(datatype,size);
}
#undef  MPI_Type_size_x
#define MPI_Type_size_x PyMPI_MPICH_MPI_Type_size_x

static int PyMPI_MPICH_MPI_Type_get_extent_x(MPI_Datatype datatype,
                                             MPI_Count *lb,
                                             MPI_Count *extent)
{
  int ierr = MPI_Type_commit(&datatype); if (ierr) return ierr;
  return MPI_Type_get_extent_x(datatype,lb,extent);
}
#undef  MPI_Type_get_extent_x
#define MPI_Type_get_extent_x PyMPI_MPICH_MPI_Type_get_extent_x

static int PyMPI_MPICH_MPI_Type_get_true_extent_x(MPI_Datatype datatype,
                                                  MPI_Count *lb,
                                                  MPI_Count *extent)
{
  int ierr = MPI_Type_commit(&datatype); if (ierr) return ierr;
  return MPI_Type_get_true_extent_x(datatype,lb,extent);
}
#undef  MPI_Type_get_true_extent_x
#define MPI_Type_get_true_extent_x PyMPI_MPICH_MPI_Type_get_true_extent_x

static int PyMPI_MPICH_MPI_Get_accumulate(const void *origin_addr,
                                          int origin_count,
                                          MPI_Datatype origin_datatype,
                                          void *result_addr,
                                          int result_count,
                                          MPI_Datatype result_datatype,
                                          int target_rank,
                                          MPI_Aint target_disp,
                                          int target_count,
                                          MPI_Datatype target_datatype,
                                          MPI_Op op, MPI_Win win)
{
  double origin_buf, result_buf;
  if (!origin_addr && !origin_count) origin_addr = (const void *)&origin_buf;
  if (!result_addr && !result_count) result_addr = (void *)&result_buf;
  return MPI_Get_accumulate(origin_addr, origin_count, origin_datatype,
                            result_addr, result_count, result_datatype,
                            target_rank,
                            target_disp, target_count, target_datatype,
                            op, win);
}
#undef  MPI_Get_accumulate
#define MPI_Get_accumulate PyMPI_MPICH_MPI_Get_accumulate

static int PyMPI_MPICH_MPI_Rget_accumulate(const void *origin_addr,
                                           int origin_count,
                                           MPI_Datatype origin_datatype,
                                           void *result_addr,
                                           int result_count,
                                           MPI_Datatype result_datatype,
                                           int target_rank,
                                           MPI_Aint target_disp,
                                           int target_count,
                                           MPI_Datatype target_datatype,
                                           MPI_Op op, MPI_Win win,
                                           MPI_Request *request)
{
  double origin_buf, result_buf;
  if (!origin_addr && !origin_count) origin_addr = (const void *)&origin_buf;
  if (!result_addr && !result_count) result_addr = (void *)&result_buf;
  return MPI_Rget_accumulate(origin_addr, origin_count, origin_datatype,
                             result_addr, result_count, result_datatype,
                             target_rank,
                             target_disp, target_count, target_datatype,
                             op, win, request);
}
#undef  MPI_Rget_accumulate
#define MPI_Rget_accumulate PyMPI_MPICH_MPI_Rget_accumulate

#endif

#endif /* !MPICH_NUMVERSION      */
#endif /* !PyMPI_COMPAT_MPICH_H */
