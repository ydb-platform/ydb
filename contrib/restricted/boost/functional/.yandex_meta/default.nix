self: super: with self; {
  boost_functional = stdenv.mkDerivation rec {
    pname = "boost_functional";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "functional";
      rev = "boost-${version}";
      hash = "sha256-U2tk/oxz+TTO/8Cz0ruSnG9QqjXBXAXLaXiedIuzQLI=";
    };
  };
}
