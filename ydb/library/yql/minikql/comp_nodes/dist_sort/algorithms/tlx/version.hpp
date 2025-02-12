/*******************************************************************************
 * tlx/version.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_VERSION_HEADER
#define TLX_VERSION_HEADER

namespace tlx {

// versions: synchronize with CMakeLists.txt

//! TLX_MAJOR_VERSION is the library interface major version number: currently
//! zero.
#define TLX_MAJOR_VERSION     0

//! TLX_MINOR_VERSION is the minor version number.
#define TLX_MINOR_VERSION     6

//! TLX_PATCH_VERSION is the patch version number.
#define TLX_PATCH_VERSION     1

/*[[[perl
  return "keep" if $ENV{USER} ne "tb";
  use POSIX qw(strftime);
  my $date = strftime("%Y%m%d", localtime);
  print "//! TLX_DATE_VERSION is the date of the last commit.\n";
  print "#define TLX_DATE_VERSION     $date\n";
]]]*/
//! TLX_DATE_VERSION is the date of the last commit.
#define TLX_DATE_VERSION     20230523
// [[[end]]]

//! TLX_VERSION is a combination of TLX_MAJOR_VERSION, TLX_MINOR_VERSION, and
//! TLX_PATCH_VERSION
#define TLX_VERSION                                                \
    ((TLX_MAJOR_VERSION * 100lu + TLX_MINOR_VERSION) * 100000000lu \
     + TLX_PATCH_VERSION)

} // namespace tlx

#endif // !TLX_VERSION_HEADER

/******************************************************************************/
