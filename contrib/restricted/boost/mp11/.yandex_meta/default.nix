self: super: with self; {
  boost_mp11 = stdenv.mkDerivation rec {
    pname = "boost_mp11";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mp11";
      rev = "boost-${version}";
      hash = "sha256-zlNarxhVbvFKfqfIFpNIO2v+xicsfVFZNDbxo5OaNoE=";
    };
  };
}
