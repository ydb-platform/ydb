self: super: with self; {
  boost_config = stdenv.mkDerivation rec {
    pname = "boost_config";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "config";
      rev = "boost-${version}";
      hash = "sha256-KjFkaaE7dLtCLFQciqS36r213uRcJy1CFD27tacWa4E=";
    };
  };
}
