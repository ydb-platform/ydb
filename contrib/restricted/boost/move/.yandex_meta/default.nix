self: super: with self; {
  boost_move = stdenv.mkDerivation rec {
    pname = "boost_move";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "move";
      rev = "boost-${version}";
      hash = "sha256-zcmNj0gjO2N7HD1moQ3hmoDcSv/Vy123+RdIR4nOf2M=";
    };
  };
}
