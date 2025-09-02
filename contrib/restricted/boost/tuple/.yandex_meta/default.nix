self: super: with self; {
  boost_tuple = stdenv.mkDerivation rec {
    pname = "boost_tuple";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tuple";
      rev = "boost-${version}";
      hash = "sha256-2PTGblaitDEhy/XvThETYyeAz2A2lDFvWfQ/atT8mn0=";
    };
  };
}
