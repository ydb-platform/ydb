self: super: with self; {
  boost_bimap = stdenv.mkDerivation rec {
    pname = "boost_bimap";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "bimap";
      rev = "boost-${version}";
      hash = "sha256-ZZn2oSdmSy8iygrZq3+RMOkSY0zVaODcgZbAN/w80Cc=";
    };
  };
}
