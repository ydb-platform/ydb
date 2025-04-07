self: super: with self; {
  boost_proto = stdenv.mkDerivation rec {
    pname = "boost_proto";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "proto";
      rev = "boost-${version}";
      hash = "sha256-RZ3BYAtDIrOpag96S1HeTDfOxAMkba8/KuxswX/mZpI=";
    };
  };
}
