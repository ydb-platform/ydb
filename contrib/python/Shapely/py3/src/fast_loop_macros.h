/**
 * Copied from numpy/src/comp/umath/fast_loop_macros.h
 *
 * Macros to help build fast ufunc inner loops.
 *
 * These expect to have access to the arguments of a typical ufunc loop,
 *
 *     char **args
 *     npy_intp *dimensions
 *     npy_intp *steps
 */


/** (ip1) -> () */
#define NO_OUTPUT_LOOP\
    char *ip1 = args[0];\
    npy_intp is1 = steps[0];\
    npy_intp n = dimensions[0];\
    npy_intp i;\
    for(i = 0; i < n; i++, ip1 += is1)


/** (ip1) -> (op1) */
#define UNARY_LOOP                         \
  char *ip1 = args[0], *op1 = args[1];     \
  npy_intp is1 = steps[0], os1 = steps[1]; \
  npy_intp n = dimensions[0];              \
  npy_intp i;                              \
  for (i = 0; i < n; i++, ip1 += is1, op1 += os1)

/** (ip1, ip2) -> (op1) */
#define BINARY_LOOP                                        \
  char *ip1 = args[0], *ip2 = args[1], *op1 = args[2];     \
  npy_intp is1 = steps[0], is2 = steps[1], os1 = steps[2]; \
  npy_intp n = dimensions[0];                              \
  npy_intp i;                                              \
  for (i = 0; i < n; i++, ip1 += is1, ip2 += is2, op1 += os1)

/** (ip1, ip2, ip3) -> (op1) */
#define TERNARY_LOOP                                                       \
  char *ip1 = args[0], *ip2 = args[1], *ip3 = args[2], *op1 = args[3];     \
  npy_intp is1 = steps[0], is2 = steps[1], is3 = steps[2], os1 = steps[3]; \
  npy_intp n = dimensions[0];                                              \
  npy_intp i;                                                              \
  for (i = 0; i < n; i++, ip1 += is1, ip2 += is2, ip3 += is3, op1 += os1)

/** (ip1, ip2, ip3, ip4) -> (op1) */
#define QUATERNARY_LOOP                                                                \
  char *ip1 = args[0], *ip2 = args[1], *ip3 = args[2], *ip4 = args[3], *op1 = args[4]; \
  npy_intp is1 = steps[0], is2 = steps[1], is3 = steps[2], is4 = steps[3],             \
           os1 = steps[4];                                                             \
  npy_intp n = dimensions[0];                                                          \
  npy_intp i;                                                                          \
  for (i = 0; i < n; i++, ip1 += is1, ip2 += is2, ip3 += is3, ip4 += is4, op1 += os1)

/** (ip1, ip2, ip3, ip4, ip5) -> (op1) */
#define QUINARY_LOOP                                                                 \
  char *ip1 = args[0], *ip2 = args[1], *ip3 = args[2], *ip4 = args[3], *ip5 = args[4], \
       *op1 = args[5];                                                                 \
  npy_intp is1 = steps[0], is2 = steps[1], is3 = steps[2], is4 = steps[3],            \
           is5 = steps[4], os1 = steps[5];                                            \
  npy_intp n = dimensions[0];                                                         \
  npy_intp i;                                                                         \
  for (i = 0; i < n; i++, ip1 += is1, ip2 += is2, ip3 += is3, ip4 += is4, ip5 += is5, \
           op1 += os1)

/** (ip1, cp1) -> (op1) */
#define SINGLE_COREDIM_LOOP_OUTER                          \
  char *ip1 = args[0], *op1 = args[1], *cp1;               \
  npy_intp is1 = steps[0], os1 = steps[1], cs1 = steps[2]; \
  npy_intp n = dimensions[0], n_c1 = dimensions[1];        \
  npy_intp i, i_c1;                                        \
  for (i = 0; i < n; i++, ip1 += is1, op1 += os1)

#define SINGLE_COREDIM_LOOP_INNER \
  cp1 = ip1;                      \
  for (i_c1 = 0; i_c1 < n_c1; i_c1++, cp1 += cs1)

/** (ip1, cp1) -> (op1, op2, op3, op4) */
#define SINGLE_COREDIM_LOOP_OUTER_NOUT4                                                \
  char *ip1 = args[0], *op1 = args[1], *op2 = args[2], *op3 = args[3], *op4 = args[4], \
       *cp1;                                                                           \
  npy_intp is1 = steps[0], os1 = steps[1], os2 = steps[2], os3 = steps[3],             \
           os4 = steps[4], cs1 = steps[5];                                             \
  npy_intp n = dimensions[0], n_c1 = dimensions[1];                                    \
  npy_intp i, i_c1;                                                                    \
  for (i = 0; i < n; i++, ip1 += is1, op1 += os1, op2 += os2, op3 += os3, op4 += os4)

/** (ip1, cp1, cp2) -> (op1) */
#define DOUBLE_COREDIM_LOOP_OUTER                                          \
  char *ip1 = args[0], *op1 = args[1];                         \
  npy_intp is1 = steps[0], os1 = steps[1], cs1 = steps[3], cs2 = steps[4]; \
  npy_intp n = dimensions[0], n_c1 = dimensions[1], n_c2 = dimensions[2];  \
  npy_intp i;                                                  \
  for (i = 0; i < n; i++, ip1 += is1, op1 += os1)

#define DOUBLE_COREDIM_LOOP_INNER_1 \
  cp1 = ip1;                        \
  for (i_c1 = 0; i_c1 < n_c1; i_c1++, cp1 += cs1)

#define DOUBLE_COREDIM_LOOP_INNER_2 \
  cp2 = cp1;                        \
  for (i_c2 = 0; i_c2 < n_c2; i_c2++, cp2 += cs2)

/** (ip1, ip2, cp1) -> (op1) */
#define BINARY_SINGLE_COREDIM_LOOP_OUTER                                   \
  char *ip1 = args[0], *ip2 = args[1], *op1 = args[2], *cp1;               \
  npy_intp is1 = steps[0], is2 = steps[1], os1 = steps[2], cs1 = steps[3]; \
  npy_intp n = dimensions[0], n_c1 = dimensions[1];                        \
  npy_intp i, i_c1;                                                        \
  for (i = 0; i < n; i++, ip1 += is1, ip2 += is2, op1 += os1)

#define BINARY_SINGLE_COREDIM_LOOP_INNER for (i_c1 = 0; i_c1 < n_c1; i_c1++, cp1 += cs1)
