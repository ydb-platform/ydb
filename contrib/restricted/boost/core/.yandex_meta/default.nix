self: super: with self; {
  boost_core = stdenv.mkDerivation rec {
    pname = "boost_core";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "core";
      rev = "boost-${version}";
      hash = "sha256-R38lFVHSoNUYyR3e37PwK/weMI+mbtMmmpX9iPMczs8=";
    };
  };
}
