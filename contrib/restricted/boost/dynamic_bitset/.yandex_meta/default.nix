self: super: with self; {
  boost_dynamic_bitset = stdenv.mkDerivation rec {
    pname = "boost_dynamic_bitset";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "dynamic_bitset";
      rev = "boost-${version}";
      hash = "sha256-lN9Ve/YFZ+jGAJImwlejwVO2JElfeRmWvV7+85BIQ4U=";
    };
  };
}
