/* Author:  Lisandro Dalcin   */
/* Contact: dalcinl@gmail.com */

#if defined(MPIX_HAVE_MPI_STATUS_GETSET)

#define PyMPI_Status_get_source MPIX_Status_get_source
#define PyMPI_Status_set_source MPIX_Status_set_source
#define PyMPI_Status_get_tag    MPIX_Status_get_tag
#define PyMPI_Status_set_tag    MPIX_Status_set_tag
#define PyMPI_Status_get_error  MPIX_Status_get_error
#define PyMPI_Status_set_error  MPIX_Status_set_error

#else

#define PyMPI_Status_GETSET(name,NAME)                         \
  static int PyMPI_Status_get_##name(MPI_Status *s, int *i)    \
  { if (s && i) { *i = s->MPI_##NAME; } return MPI_SUCCESS; }  \
  static int PyMPI_Status_set_##name(MPI_Status *s, int i)     \
  { if (s) { s->MPI_##NAME = i; } return MPI_SUCCESS; }        \

PyMPI_Status_GETSET( source, SOURCE )
PyMPI_Status_GETSET( tag,    TAG    )
PyMPI_Status_GETSET( error,  ERROR  )

#undef PyMPI_Status_GETSET

#endif
