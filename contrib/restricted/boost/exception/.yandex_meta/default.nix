self: super: with self; {
  boost_exception = stdenv.mkDerivation rec {
    pname = "boost_exception";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "exception";
      rev = "boost-${version}";
      hash = "sha256-dht0v69IBywurMux8ghe+8L2268pQUoBBW3Mixo+LeA=";
    };
  };
}
