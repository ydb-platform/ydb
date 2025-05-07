self: super: with self; {
  boost_array = stdenv.mkDerivation rec {
    pname = "boost_array";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "array";
      rev = "boost-${version}";
      hash = "sha256-P7pFHRtIkmDxWv3Oq5xul5l2eh2zX6rAr6/24pk/daY=";
    };
  };
}
