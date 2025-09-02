self: super: with self; {
  boost_container = stdenv.mkDerivation rec {
    pname = "boost_container";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container";
      rev = "boost-${version}";
      hash = "sha256-i2eaT1vx2xgmqXw2sEXJH9Mmo+QLy8NNTHUsAfKKyYc=";
    };
  };
}
