self: super: with self; {
  boost_predef = stdenv.mkDerivation rec {
    pname = "boost_predef";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "predef";
      rev = "boost-${version}";
      hash = "sha256-C1aMk1RkLMqSBIv8oTlfkZhlp1kzWqXoAn3Cyev9Jtc=";
    };
  };
}
