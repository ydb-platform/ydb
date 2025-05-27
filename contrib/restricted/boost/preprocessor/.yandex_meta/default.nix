self: super: with self; {
  boost_preprocessor = stdenv.mkDerivation rec {
    pname = "boost_preprocessor";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "preprocessor";
      rev = "boost-${version}";
      hash = "sha256-AqEYAPuU4GyEAb8i0fVoGUp0XQC+o29ljvoxSglfkNo=";
    };
  };
}
