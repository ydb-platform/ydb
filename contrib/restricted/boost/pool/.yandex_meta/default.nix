self: super: with self; {
  boost_pool = stdenv.mkDerivation rec {
    pname = "boost_pool";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "pool";
      rev = "boost-${version}";
      hash = "sha256-JurFimHIzn1P4VHnyiHdCusHKuBINyn019bLXpBbN/M=";
    };
  };
}
