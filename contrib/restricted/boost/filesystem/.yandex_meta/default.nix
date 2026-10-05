self: super: with self; {
  boost_filesystem = stdenv.mkDerivation rec {
    pname = "boost_filesystem";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "filesystem";
      rev = "boost-${version}";
      hash = "sha256-s3THtuVPvDeKvABsaBlhFrEMshiIGAZcV4Qa4KjuCx4=";
    };
  };
}
