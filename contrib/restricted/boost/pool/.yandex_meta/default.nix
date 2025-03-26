self: super: with self; {
  boost_pool = stdenv.mkDerivation rec {
    pname = "boost_pool";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "pool";
      rev = "boost-${version}";
      hash = "sha256-epQslfXvCvtVTStR2VBVIKa6I1fkT8AAs3+5bDaBaHw=";
    };
  };
}
