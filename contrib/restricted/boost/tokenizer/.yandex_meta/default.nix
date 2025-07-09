self: super: with self; {
  boost_tokenizer = stdenv.mkDerivation rec {
    pname = "boost_tokenizer";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tokenizer";
      rev = "boost-${version}";
      hash = "sha256-xjnAXQ+c7G6CMB1RggAERkCdh0+2rKMAIv28dDScEfk=";
    };
  };
}
