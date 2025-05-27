self: super: with self; {
  boost_multi_array = stdenv.mkDerivation rec {
    pname = "boost_multi_array";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "multi_array";
      rev = "boost-${version}";
      hash = "sha256-oG70CtBohtcka45hXehFHlpoThZiYlbKJtDtkB1p0O4=";
    };
  };
}
