self: super: with self; {
  boost_config = stdenv.mkDerivation rec {
    pname = "boost_config";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "config";
      rev = "boost-${version}";
      hash = "sha256-t1JEqiyD0xtB+3st9plPAVZvCtsQCoFCAFh9nEHUK9g=";
    };
  };
}
