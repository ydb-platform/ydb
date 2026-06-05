self: super: with self; {
  boost_config = stdenv.mkDerivation rec {
    pname = "boost_config";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "config";
      rev = "boost-${version}";
      hash = "sha256-40a2Ho63M5yWGQ6FDFeZxs9yJy1A8Ec6KcW4FvT2TaQ=";
    };
  };
}
