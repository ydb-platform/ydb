self: super: with self; {
  boost_align = stdenv.mkDerivation rec {
    pname = "boost_align";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "align";
      rev = "boost-${version}";
      hash = "sha256-Afomw3RjQK6NGMu9lU9z3y41EwyqPj3Ghd0ELFSmYTA=";
    };
  };
}
