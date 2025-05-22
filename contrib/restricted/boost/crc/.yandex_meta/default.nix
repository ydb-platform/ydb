self: super: with self; {
  boost_crc = stdenv.mkDerivation rec {
    pname = "boost_crc";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "crc";
      rev = "boost-${version}";
      hash = "sha256-lmVm65cHe43RoidOJJ2g13I3SJAf4Lmg2skcRq41zYE=";
    };
  };
}
