self: super: with self; {
  boost_dynamic_bitset = stdenv.mkDerivation rec {
    pname = "boost_dynamic_bitset";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "dynamic_bitset";
      rev = "boost-${version}";
      hash = "sha256-ahwd85x2p9K06EbrXX5kpZEMGjcH9a97sZ9mWvv3CJA=";
    };
  };
}
