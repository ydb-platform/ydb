self: super: with self; {
  boost_assert = stdenv.mkDerivation rec {
    pname = "boost_assert";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "assert";
      rev = "boost-${version}";
      hash = "sha256-2PuFFOmCPQPQQAtZ08gjiEPwTNnzf2UaI8pDsN1gfs4=";
    };
  };
}
