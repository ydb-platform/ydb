self: super: with self; {
  boost_preprocessor = stdenv.mkDerivation rec {
    pname = "boost_preprocessor";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "preprocessor";
      rev = "boost-${version}";
      hash = "sha256-4GTIVg58rx21Sqpj5fQH8VXVdHCGI9reauZ64ZlDhfA=";
    };
  };
}
