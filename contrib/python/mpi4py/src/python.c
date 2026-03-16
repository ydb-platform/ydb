/* Author:  Lisandro Dalcin   */
/* Contact: dalcinl@gmail.com */

/* -------------------------------------------------------------------------- */

#include <Python.h>

#define MPICH_IGNORE_CXX_SEEK 1
#define OMPI_IGNORE_CXX_SEEK 1
#include <mpi.h>

#if PY_MAJOR_VERSION <= 2
#define Py_BytesMain Py_Main
#elif PY_VERSION_HEX < 0x03070000
static int Py_BytesMain(int, char **);
#elif PY_VERSION_HEX < 0x03080000
PyAPI_FUNC(int) _Py_UnixMain(int, char **);
#define Py_BytesMain _Py_UnixMain
#endif

/* -------------------------------------------------------------------------- */

int
main(int argc, char **argv)
{
  int status = 0, flag = 1, finalize = 0;

  /* MPI initalization */
  (void)MPI_Initialized(&flag);
  if (!flag) {
#if defined(MPI_VERSION) && (MPI_VERSION > 1)
    int required = MPI_THREAD_MULTIPLE;
    int provided = MPI_THREAD_SINGLE;
    (void)MPI_Init_thread(&argc, &argv, required, &provided);
#else
    (void)MPI_Init(&argc, &argv);
#endif
    finalize = 1;
  }

  /* Python main */
  status = Py_BytesMain(argc, argv);

  /* MPI finalization */
  (void)MPI_Finalized(&flag);
  if (!flag) {
    if (status)
      (void)MPI_Abort(MPI_COMM_WORLD, status);
    if (finalize)
      (void)MPI_Finalize();
  }

  return status;
}

/* -------------------------------------------------------------------------- */

#if PY_MAJOR_VERSION >= 3 && PY_VERSION_HEX < 0x03070000

#include <locale.h>

static wchar_t **mk_wargs(int, char **);
static wchar_t **cp_wargs(int, wchar_t **);
static void rm_wargs(wchar_t **, int);

static int
Py_BytesMain(int argc, char **argv)
{
  int sts = 0;
  wchar_t **wargv  = mk_wargs(argc, argv);
  wchar_t **wargv2 = cp_wargs(argc, wargv);
  if (wargv && wargv2)
    sts = Py_Main(argc, wargv);
  else
    sts = 1;
  rm_wargs(wargv2, 1);
  rm_wargs(wargv,  0);
  return sts;
}

#if PY_VERSION_HEX < 0x03050000
#define Py_DecodeLocale _Py_char2wchar
#endif

static wchar_t **
mk_wargs(int argc, char **argv)
{
  int i; char *saved_locale = NULL;
  wchar_t **args = NULL;

  args = (wchar_t **)malloc((size_t)(argc+1)*sizeof(wchar_t *));
  if (!args) goto oom;

  saved_locale = strdup(setlocale(LC_ALL, NULL));
  if (!saved_locale) goto oom;
  setlocale(LC_ALL, "");

  for (i=0; i<argc; i++) {
    args[i] = Py_DecodeLocale(argv[i], NULL);
    if (!args[i]) goto oom;
  }
  args[argc] = NULL;

  setlocale(LC_ALL, saved_locale);
  free(saved_locale);

  return args;

 oom:
  fprintf(stderr, "out of memory\n");
  if (saved_locale) {
    setlocale(LC_ALL, saved_locale);
    free(saved_locale);
  }
  if (args)
    rm_wargs(args, 1);
  return NULL;
}

static wchar_t **
cp_wargs(int argc, wchar_t **args)
{
  int i; wchar_t **args_copy = NULL;
  if (!args) return NULL;
  args_copy = (wchar_t **)malloc((size_t)(argc+1)*sizeof(wchar_t *));
  if (!args_copy) goto oom;
  for (i=0; i<(argc+1); i++) { args_copy[i] = args[i]; }
  return args_copy;
 oom:
  fprintf(stderr, "out of memory\n");
  return NULL;
}

static void
rm_wargs(wchar_t **args, int deep)
{
  int i = 0;
  if (args && deep)
    while (args[i])
      free(args[i++]);
  if (args)
    free(args);
}

#endif /* !(PY_MAJOR_VERSION >= 3) */

/* -------------------------------------------------------------------------- */

/*
   Local variables:
   c-basic-offset: 2
   indent-tabs-mode: nil
   End:
*/
