self: super: with self; {
  boost_any = stdenv.mkDerivation rec {
    pname = "boost_any";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "any";
      rev = "boost-${version}";
      hash = "sha256-BSDvDYphP60T8s3IdtOiA3iyQSz+W+E0QlsZReHtoG8=";
    };
  };
}
