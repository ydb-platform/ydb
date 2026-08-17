self: super: with self; {
  boost_any = stdenv.mkDerivation rec {
    pname = "boost_any";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "any";
      rev = "boost-${version}";
      hash = "sha256-Xx/0+j/tOFJkkjihZqM8nSnyy9V+B62CMVJ15EiEvcA=";
    };
  };
}
