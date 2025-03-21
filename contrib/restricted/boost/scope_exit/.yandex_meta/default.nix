self: super: with self; {
  boost_scope_exit = stdenv.mkDerivation rec {
    pname = "boost_scope_exit";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "scope_exit";
      rev = "boost-${version}";
      hash = "sha256-Ik90kLERwb/PrCiFwpRxiQGa1dbEpwlAYRHkA5rD8LM=";
    };
  };
}
