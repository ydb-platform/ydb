self: super: with self; {
  boost_range = stdenv.mkDerivation rec {
    pname = "boost_range";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "range";
      rev = "boost-${version}";
      hash = "sha256-JWIDKme6si91ZRAPLRSczE1zoSjL0KeqV0DsGKUvfCU=";
    };
  };
}
