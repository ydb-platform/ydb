self: super: with self; {
  boost_lambda = stdenv.mkDerivation rec {
    pname = "boost_lambda";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "lambda";
      rev = "boost-${version}";
      hash = "sha256-IbsR0TS+6FOsG6EBIsd4qTA25voKFCAZQK7qYVvgJeQ=";
    };
  };
}
