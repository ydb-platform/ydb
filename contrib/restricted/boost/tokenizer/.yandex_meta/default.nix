self: super: with self; {
  boost_tokenizer = stdenv.mkDerivation rec {
    pname = "boost_tokenizer";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tokenizer";
      rev = "boost-${version}";
      hash = "sha256-rDxOh/aLjA9PvjMoO2Eae8hNEmfP43DS3/KouMeuABQ=";
    };
  };
}
