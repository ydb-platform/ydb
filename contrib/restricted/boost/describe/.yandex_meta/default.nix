self: super: with self; {
  boost_describe = stdenv.mkDerivation rec {
    pname = "boost_describe";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "describe";
      rev = "boost-${version}";
      hash = "sha256-nZohSKFU6tQfBQa8GTd0jLe1qgqFoJlMUaISrmPP4Ng=";
    };
  };
}
