self: super: with self; {
  boost_system = stdenv.mkDerivation rec {
    pname = "boost_system";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "system";
      rev = "boost-${version}";
      hash = "sha256-FP7oVJA3FSk67dRKpNFxWmGznPGKuni1Zp0k1h1+1bU=";
    };
  };
}
