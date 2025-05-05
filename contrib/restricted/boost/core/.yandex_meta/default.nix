self: super: with self; {
  boost_core = stdenv.mkDerivation rec {
    pname = "boost_core";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "core";
      rev = "boost-${version}";
      hash = "sha256-TwmbaVG3kXsV4c6crM5bTlmmw5S+8ZUbVqvlSCwrW7U=";
    };
  };
}
