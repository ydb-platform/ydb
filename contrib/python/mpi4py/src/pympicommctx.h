/* Author:  Lisandro Dalcin   */
/* Contact: dalcinl@gmail.com */

#include <stdlib.h>
#ifndef PyMPI_MALLOC
#define PyMPI_MALLOC malloc
#endif
#ifndef PyMPI_FREE
#define PyMPI_FREE free
#endif

#ifndef MPIAPI
#define MPIAPI
#endif

#undef  CHKERR
#define CHKERR(ierr) do { if (ierr != MPI_SUCCESS) return ierr; } while(0)

typedef struct {
  MPI_Comm dupcomm;
  MPI_Comm localcomm;
  int      tag;
  int      low_group;
} PyMPI_Commctx;

static int PyMPI_Commctx_KEYVAL = MPI_KEYVAL_INVALID;
static int PyMPI_Commctx_TAG_UB = -1;

static int PyMPI_Commctx_new(PyMPI_Commctx **_commctx)
{
  PyMPI_Commctx *commctx;
  if (PyMPI_Commctx_TAG_UB < 0) {
    int ierr, *attrval = NULL, flag = 0;
    ierr = MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_TAG_UB, &attrval, &flag); CHKERR(ierr);
    PyMPI_Commctx_TAG_UB = (flag && attrval) ? *attrval : 32767;
  }
  commctx = (PyMPI_Commctx *)PyMPI_MALLOC(sizeof(PyMPI_Commctx));
  if (commctx) {
    commctx->dupcomm = MPI_COMM_NULL;
    commctx->localcomm = MPI_COMM_NULL;
    commctx->tag = 0;
    commctx->low_group = -1;
  }
  *_commctx = commctx;
  return MPI_SUCCESS;
}

static int MPIAPI PyMPI_Commctx_free_fn(MPI_Comm comm, int k, void *v, void *xs)
{
  int ierr, finalized = 1;
  PyMPI_Commctx *commctx = (PyMPI_Commctx *)v;
  (void)comm; (void)k; (void)xs; /* unused */
  if (!commctx) return MPI_SUCCESS;
  ierr = MPI_Finalized(&finalized); CHKERR(ierr);
  if (finalized) goto fn_exit;
  if (commctx->localcomm != MPI_COMM_NULL)
    {ierr = MPI_Comm_free(&commctx->localcomm); CHKERR(ierr);}
  if (commctx->dupcomm != MPI_COMM_NULL)
    {ierr = MPI_Comm_free(&commctx->dupcomm); CHKERR(ierr);}
 fn_exit:
  PyMPI_FREE(commctx);
  return MPI_SUCCESS;
}

static int PyMPI_Commctx_keyval(int *keyval)
{
  int ierr;
  if (PyMPI_Commctx_KEYVAL != MPI_KEYVAL_INVALID) goto fn_exit;
  ierr = MPI_Comm_create_keyval(MPI_COMM_NULL_COPY_FN,
                                PyMPI_Commctx_free_fn,
                                &PyMPI_Commctx_KEYVAL, NULL); CHKERR(ierr);
 fn_exit:
  if (keyval) *keyval = PyMPI_Commctx_KEYVAL;
  return MPI_SUCCESS;
}

static int PyMPI_Commctx_lookup(MPI_Comm comm, PyMPI_Commctx **_commctx)
{
  int ierr, found = 0, keyval = MPI_KEYVAL_INVALID;
  PyMPI_Commctx *commctx = NULL;

  ierr = PyMPI_Commctx_keyval(&keyval); CHKERR(ierr);
  ierr = MPI_Comm_get_attr(comm, keyval, &commctx, &found); CHKERR(ierr);
  if (found && commctx) goto fn_exit;

  ierr = PyMPI_Commctx_new(&commctx); CHKERR(ierr);
  if (!commctx) {(void)MPI_Comm_call_errhandler(comm, MPI_ERR_INTERN); return MPI_ERR_INTERN;}
  ierr = MPI_Comm_set_attr(comm, keyval, commctx); CHKERR(ierr);
  ierr = MPI_Comm_dup(comm, &commctx->dupcomm); CHKERR(ierr);

 fn_exit:
  if (commctx->tag >= PyMPI_Commctx_TAG_UB) commctx->tag = 0;
  if (_commctx) *_commctx = commctx;
  return MPI_SUCCESS;
}

static int PyMPI_Commctx_clear(MPI_Comm comm)
{
  int ierr, found = 0, keyval = PyMPI_Commctx_KEYVAL;
  PyMPI_Commctx *commctx = NULL;

  if (keyval == MPI_KEYVAL_INVALID) return MPI_SUCCESS;
  ierr = MPI_Comm_get_attr(comm, keyval, &commctx, &found); CHKERR(ierr);
  if (found) {ierr = MPI_Comm_delete_attr(comm, keyval); CHKERR(ierr);}
  return MPI_SUCCESS;
}

static int PyMPI_Commctx_intra(MPI_Comm comm, MPI_Comm *dupcomm, int *tag)
{
  int ierr;
  PyMPI_Commctx *commctx = NULL;
  ierr = PyMPI_Commctx_lookup(comm, &commctx);CHKERR(ierr);
  if (dupcomm)
    *dupcomm = commctx->dupcomm;
  if (tag)
    *tag = commctx->tag++;
  return MPI_SUCCESS;
}

static int PyMPI_Commctx_inter(MPI_Comm comm, MPI_Comm *dupcomm, int *tag,
                               MPI_Comm *localcomm, int *low_group)
{
  int ierr;
  PyMPI_Commctx *commctx = NULL;
  ierr = PyMPI_Commctx_lookup(comm, &commctx);CHKERR(ierr);
  if (commctx->localcomm == MPI_COMM_NULL) {
    int localsize, remotesize, mergerank;
    MPI_Comm mergecomm = MPI_COMM_NULL;
    ierr = MPI_Comm_size(comm, &localsize); CHKERR(ierr);
    ierr = MPI_Comm_remote_size(comm, &remotesize); CHKERR(ierr);
    ierr = MPI_Intercomm_merge(comm, localsize>remotesize, &mergecomm); CHKERR(ierr);
    ierr = MPI_Comm_rank(mergecomm, &mergerank); CHKERR(ierr);
    commctx->low_group = ((localsize>remotesize) ? 0 :
                          (localsize<remotesize) ? 1 :
                          (mergerank<localsize));
#if (MPI_VERSION > 2) || (MPI_VERSION == 2 && MPI_SUBVERSION >= 2)
  {
    MPI_Group localgroup = MPI_GROUP_NULL;
    ierr = MPI_Comm_group(comm, &localgroup); CHKERR(ierr);
    ierr = MPI_Comm_create(mergecomm, localgroup, &commctx->localcomm); CHKERR(ierr);
    ierr = MPI_Group_free(&localgroup); CHKERR(ierr);
  }
#else
    ierr = MPI_Comm_split(mergecomm, commctx->low_group, 0, &commctx->localcomm); CHKERR(ierr);
#endif
    ierr = MPI_Comm_free(&mergecomm); CHKERR(ierr);
  }
  if (dupcomm)
    *dupcomm = commctx->dupcomm;
  if (tag)
    *tag = commctx->tag++;
  if (localcomm)
    *localcomm = commctx->localcomm;
  if (low_group)
    *low_group  = commctx->low_group;
  return MPI_SUCCESS;
}

static int PyMPI_Commctx_finalize(void)
{
  int ierr;
  if (PyMPI_Commctx_KEYVAL == MPI_KEYVAL_INVALID) return MPI_SUCCESS;
  ierr = PyMPI_Commctx_clear(MPI_COMM_SELF); CHKERR(ierr);
  ierr = PyMPI_Commctx_clear(MPI_COMM_WORLD); CHKERR(ierr);
  ierr = MPI_Comm_free_keyval(&PyMPI_Commctx_KEYVAL); CHKERR(ierr);
  PyMPI_Commctx_TAG_UB = -1;
  return MPI_SUCCESS;
}

#undef CHKERR

/*
   Local variables:
   c-basic-offset: 2
   indent-tabs-mode: nil
   End:
*/
