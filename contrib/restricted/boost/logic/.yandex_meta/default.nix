self: super: with self; {
  boost_logic = stdenv.mkDerivation rec {
    pname = "boost_logic";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "logic";
      rev = "boost-${version}";
      hash = "sha256-GUoHIzTeYn0eL4gN9qjFSJu9nnl8dLT2PGvc7w1egg8=";
    };
  };
}
