self: super: with self; {
  boost_crc = stdenv.mkDerivation rec {
    pname = "boost_crc";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "crc";
      rev = "boost-${version}";
      hash = "sha256-zPCiNBmDIcn2Aa0sYmUwXg9AB3kajuZLZuUVrolIxT4=";
    };
  };
}
