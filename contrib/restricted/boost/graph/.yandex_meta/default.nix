self: super: with self; {
  boost_graph = stdenv.mkDerivation rec {
    pname = "boost_graph";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "graph";
      rev = "boost-${version}";
      hash = "sha256-FbEIgC/aZw6Jy+3fht/lsZ8AW9ByxKBpv13P7PO35jM=";
    };
  };
}
