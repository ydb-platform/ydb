pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.3.14";

  src = fetchurl {
    url = "http://www.unixodbc.org/${pname}-${version}.tar.gz";
    hash = "sha256-TigU3j4B/DCwufdeg7taupGrA4TulRKGUEu3AgVSR3E=";
  };

  # Do not let it use glibc-only argz.h.
  configureFlags = attrs.configureFlags ++ [
    "ac_cv_header_argz_h=no"
    "ac_cv_func_argz_add=no"
    "ac_cv_func_argz_append=no"
    "ac_cv_func_argz_count=no"
    "ac_cv_func_argz_create_sep=no"
    "ac_cv_func_argz_insert=no"
    "ac_cv_func_argz_next=no"
    "ac_cv_func_argz_stringify=no"
  ];

  # Do not let libltdl/libtool delete libltdl/.libs/libltdlcS.c
  postConfigure = ''
    substituteInPlace libltdl/libtool --replace 'RM="rm -f"' 'RM="echo"'
  '';
}
