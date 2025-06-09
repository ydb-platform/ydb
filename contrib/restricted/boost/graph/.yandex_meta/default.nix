self: super: with self; {
  boost_graph = stdenv.mkDerivation rec {
    pname = "boost_graph";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "graph";
      rev = "boost-${version}";
      hash = "sha256-djo7DIA49MXStNzp9fJPyBj+L1nuTVu6Z5Sf1Cf2ZtY=";
    };
  };
}
