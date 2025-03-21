self: super: with self; {
  boost_bimap = stdenv.mkDerivation rec {
    pname = "boost_bimap";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "bimap";
      rev = "boost-${version}";
      hash = "sha256-gyWR4Ix7omtWG+DW2LICGplPvqTvwPAsI7toflUWlSA=";
    };
  };
}
