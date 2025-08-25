self: super: with self; {
  boost_system = stdenv.mkDerivation rec {
    pname = "boost_system";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "system";
      rev = "boost-${version}";
      hash = "sha256-XRXaDEbnDSgbaTzlHb4Rf42mU37ODAuBKnpjiwlpP7E=";
    };
  };
}
