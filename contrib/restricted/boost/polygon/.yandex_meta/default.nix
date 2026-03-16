self: super: with self; {
  boost_polygon = stdenv.mkDerivation rec {
    pname = "boost_polygon";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "polygon";
      rev = "boost-${version}";
      hash = "sha256-m7unjr8cz+TQrziFiDCd4NdznZnGkXYvMKPBhTTXVwo=";
    };
  };
}
