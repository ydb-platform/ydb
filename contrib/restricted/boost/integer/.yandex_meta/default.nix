self: super: with self; {
  boost_integer = stdenv.mkDerivation rec {
    pname = "boost_integer";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "integer";
      rev = "boost-${version}";
      hash = "sha256-BCN0cvxwChy2sEuZT1o2YEo996bPgperYiY+r92qE/E=";
    };
  };
}
