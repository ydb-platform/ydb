self: super: with self; {
  boost_rational = stdenv.mkDerivation rec {
    pname = "boost_rational";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "rational";
      rev = "boost-${version}";
      hash = "sha256-Ps443rMBsVBfh6DS4olwuR/fAmlgKVMjNO9H+fWO0Mw=";
    };
  };
}
