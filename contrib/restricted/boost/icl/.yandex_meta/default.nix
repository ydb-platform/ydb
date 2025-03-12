self: super: with self; {
  boost_icl = stdenv.mkDerivation rec {
    pname = "boost_icl";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "icl";
      rev = "boost-${version}";
      hash = "sha256-KNgAvevg1P0+qaE2eUtvfV/YCwEcXfUoZDTT6O5P0jU=";
    };
  };
}
