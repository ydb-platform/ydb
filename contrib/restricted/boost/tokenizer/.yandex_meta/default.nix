self: super: with self; {
  boost_tokenizer = stdenv.mkDerivation rec {
    pname = "boost_tokenizer";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tokenizer";
      rev = "boost-${version}";
      hash = "sha256-lSJfD4+xHUWOOwXaKktm04BWWE9sBad3K2TB6coE71I=";
    };
  };
}
