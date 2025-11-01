self: super: with self; {
  boost_endian = stdenv.mkDerivation rec {
    pname = "boost_endian";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "endian";
      rev = "boost-${version}";
      hash = "sha256-fpG5BghLPZAKJuDqFHHaYjWrQ5bXveQMMQv/oFlhjbA=";
    };
  };
}
