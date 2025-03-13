self: super: with self; {
  boost_optional = stdenv.mkDerivation rec {
    pname = "boost_optional";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "optional";
      rev = "boost-${version}";
      hash = "sha256-RRHnzA5K/8NTC/gghxX0bWE2+MyZfAWWFOaZDyfcAns=";
    };
  };
}
