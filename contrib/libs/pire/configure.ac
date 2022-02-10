AC_PREREQ([2.63]) 
AC_INIT([pire], [0.0.2], [dprokoptsev@yandex-team.ru]) 
AM_INIT_AUTOMAKE([foreign -Wall]) 
AC_CONFIG_SRCDIR([pire/classes.cpp]) 
AC_CONFIG_HEADERS([config.h]) 
AC_CONFIG_MACRO_DIR([m4]) 
 
AC_LANG_CPLUSPLUS 
 
# Require neccessary binaries to build ourselves 
AC_PROG_CXX 
AC_PROG_CC 
AC_PROG_LEX 
AC_PROG_YACC 
AC_PROG_LIBTOOL 
 
# Check for cppunit 
AM_PATH_CPPUNIT([0.0.0],[with_unittests=yes],[ 
	AC_WARN([cppunit not found. Unit tests will not compile and run.]) 
	with_unittests=no 
]) 
AM_CONDITIONAL([WITH_UNITTESTS], [test x"$with_unittests" = xyes]) 
 
# Just for conscience' sake 
AC_CHECK_HEADERS([stdlib.h string.h sys/time.h]) 
AC_HEADER_STDBOOL 
AC_C_INLINE 
AC_TYPE_SIZE_T 
AC_CHECK_TYPES([ptrdiff_t]) 
AC_FUNC_ERROR_AT_LINE 
AC_FUNC_MALLOC 
AC_CHECK_FUNCS([memset strchr]) 
 
# Require little-endian platform 
AC_C_BIGENDIAN 
if test x"$ac_cv_c_bigendian" = xyes; then 
	AC_ERROR([pire has not been ported to big-endian platforms yet.]) 
fi 
 
# Optional features 
AC_ARG_ENABLE([extra], AS_HELP_STRING([--enable-extra], [Add extra functionality (capturing scanner, etc...)])) 
AC_ARG_ENABLE([debug], AS_HELP_STRING([--enable-debug], [Make Pire dump all constructed FSMs to std::clog (useless unless debugging Pire)])) 
AM_CONDITIONAL([ENABLE_EXTRA], [test x"$enable_extra" = xyes]) 
AM_CONDITIONAL([ENABLE_DEBUG], [test x"$enable_debug" = xyes]) 
 
AC_CONFIG_FILES([Makefile pire/Makefile tests/Makefile pkg/Makefile samples/Makefile samples/bench/Makefile]) 
AC_OUTPUT 
