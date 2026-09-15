self: super: with self; {
  boost_dynamic_bitset = stdenv.mkDerivation rec {
    pname = "boost_dynamic_bitset";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "dynamic_bitset";
      rev = "boost-${version}";
      hash = "sha256-VZeHGgFBAD2lA22MV34ka5PyZGH/8IS9DhxAUSXt0/Y=";
    };
  };
}
