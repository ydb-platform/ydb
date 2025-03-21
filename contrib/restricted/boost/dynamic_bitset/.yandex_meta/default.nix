self: super: with self; {
  boost_dynamic_bitset = stdenv.mkDerivation rec {
    pname = "boost_dynamic_bitset";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "dynamic_bitset";
      rev = "boost-${version}";
      hash = "sha256-zTjTwvRgETHTST9vVqpy5yIcZ+QT+eyuPY4AmNprnpM=";
    };
  };
}
