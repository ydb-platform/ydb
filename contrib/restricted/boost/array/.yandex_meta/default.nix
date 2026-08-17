self: super: with self; {
  boost_array = stdenv.mkDerivation rec {
    pname = "boost_array";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "array";
      rev = "boost-${version}";
      hash = "sha256-kkUQv22+Oby5OpBaOuisQR94TNx60fEkNXMIEpDf7oY=";
    };
  };
}
