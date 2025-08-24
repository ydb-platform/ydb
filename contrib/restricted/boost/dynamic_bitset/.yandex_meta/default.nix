self: super: with self; {
  boost_dynamic_bitset = stdenv.mkDerivation rec {
    pname = "boost_dynamic_bitset";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "dynamic_bitset";
      rev = "boost-${version}";
      hash = "sha256-EYyMQEQZvs2vp25O1xZTBmnGR819xHVQsZ/MVx1CWv8=";
    };
  };
}
