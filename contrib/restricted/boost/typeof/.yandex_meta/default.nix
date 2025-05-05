self: super: with self; {
  boost_typeof = stdenv.mkDerivation rec {
    pname = "boost_typeof";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "typeof";
      rev = "boost-${version}";
      hash = "sha256-rht//Y2U+MkCOfm84gZBpbxE8pTL+1URQk6DkFnLIiM=";
    };
  };
}
