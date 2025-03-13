self: super: with self; {
  boost_lambda = stdenv.mkDerivation rec {
    pname = "boost_lambda";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "lambda";
      rev = "boost-${version}";
      hash = "sha256-IQkSGsQuyCPGSye6QdRms8LXN3H5whkxjVVjCyQUx3Y=";
    };
  };
}
