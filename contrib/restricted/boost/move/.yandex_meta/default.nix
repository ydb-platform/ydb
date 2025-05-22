self: super: with self; {
  boost_move = stdenv.mkDerivation rec {
    pname = "boost_move";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "move";
      rev = "boost-${version}";
      hash = "sha256-9DntfxeB2cQnOSc8d+OtGncurSpTx1qwmKmj0iN5Aas=";
    };
  };
}
