self: super: with self; {
  boost_function_types = stdenv.mkDerivation rec {
    pname = "boost_function_types";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "function_types";
      rev = "boost-${version}";
      hash = "sha256-o7L2gCEs8/Xcc88Tp3rhxI1boUf32vQEcAa2hbIqVDY=";
    };
  };
}
