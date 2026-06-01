self: super: with self; {
  boost_multi_array = stdenv.mkDerivation rec {
    pname = "boost_multi_array";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "multi_array";
      rev = "boost-${version}";
      hash = "sha256-MJg/G1AKboZjkmiMS3+kByKLX77QAa+SirLaiomMJ+I=";
    };
  };
}
