self: super: with self; {
  boost_system = stdenv.mkDerivation rec {
    pname = "boost_system";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "system";
      rev = "boost-${version}";
      hash = "sha256-wI/Hs+kS/jPg90iE5E6ZFhQ41O34JCh0N17iRjteYyk=";
    };
  };
}
