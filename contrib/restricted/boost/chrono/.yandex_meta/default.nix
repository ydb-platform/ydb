self: super: with self; {
  boost_chrono = stdenv.mkDerivation rec {
    pname = "boost_chrono";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "chrono";
      rev = "boost-${version}";
      hash = "sha256-njCpMM79EwHTW6v0i5O10RhD0v0PIKX8BD7HdRufqbU=";
    };
  };
}
