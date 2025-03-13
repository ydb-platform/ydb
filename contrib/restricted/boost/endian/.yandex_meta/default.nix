self: super: with self; {
  boost_endian = stdenv.mkDerivation rec {
    pname = "boost_endian";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "endian";
      rev = "boost-${version}";
      hash = "sha256-VvRTa92Ca9wdx0cArbKr2xlnbi2UQLUSRuiwVLA5kyA=";
    };
  };
}
