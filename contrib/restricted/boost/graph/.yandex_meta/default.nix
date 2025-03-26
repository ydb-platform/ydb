self: super: with self; {
  boost_graph = stdenv.mkDerivation rec {
    pname = "boost_graph";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "graph";
      rev = "boost-${version}";
      hash = "sha256-b3WNm/FhKETRFHaGFYK4AT6mPuNfULQDM2lg0Ai9I2k=";
    };
  };
}
