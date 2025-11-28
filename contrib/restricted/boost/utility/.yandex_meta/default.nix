self: super: with self; {
  boost_utility = stdenv.mkDerivation rec {
    pname = "boost_utility";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "utility";
      rev = "boost-${version}";
      hash = "sha256-jCO+VDInIIV0gPePl6zambgmjflD/uuJvdetqSVGTE4=";
    };
  };
}
