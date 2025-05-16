self: super: with self; {
  boost_multi_index = stdenv.mkDerivation rec {
    pname = "boost_multi_index";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "multi_index";
      rev = "boost-${version}";
      hash = "sha256-VC2QL0YBsaYfS9DH5lMG8f9Ui236bsv1Dz+ScyqKFBU=";
    };
  };
}
