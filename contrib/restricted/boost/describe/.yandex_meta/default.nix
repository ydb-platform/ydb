self: super: with self; {
  boost_describe = stdenv.mkDerivation rec {
    pname = "boost_describe";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "describe";
      rev = "boost-${version}";
      hash = "sha256-6DQOpYH2FvA2ui7wIFIm4DUPL0fikIQc0c9b+26efzI=";
    };
  };
}
