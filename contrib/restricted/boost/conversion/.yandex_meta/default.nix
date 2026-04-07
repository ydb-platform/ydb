self: super: with self; {
  boost_conversion = stdenv.mkDerivation rec {
    pname = "boost_conversion";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "conversion";
      rev = "boost-${version}";
      hash = "sha256-7pCi7ZVzH0cTOpaKYiJWJtOvIur77t7s8+Vi9EzLEog=";
    };
  };
}
