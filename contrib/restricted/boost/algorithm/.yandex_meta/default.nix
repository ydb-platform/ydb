self: super: with self; {
  boost_algorithm = stdenv.mkDerivation rec {
    pname = "boost_algorithm";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "algorithm";
      rev = "boost-${version}";
      hash = "sha256-6KAJuBeVpqHYSPC6pV3BYJhF/iIqmB990TTI3fg7SJA=";
    };
  };
}
