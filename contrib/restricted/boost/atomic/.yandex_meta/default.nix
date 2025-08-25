self: super: with self; {
  boost_atomic = stdenv.mkDerivation rec {
    pname = "boost_atomic";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "atomic";
      rev = "boost-${version}";
      hash = "sha256-oSRJDM5tJdJWsF1uHNdGjO4Wd6hxgb91lrlx0g6BW/A=";
    };
  };
}
