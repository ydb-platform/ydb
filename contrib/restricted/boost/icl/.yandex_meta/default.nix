self: super: with self; {
  boost_icl = stdenv.mkDerivation rec {
    pname = "boost_icl";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "icl";
      rev = "boost-${version}";
      hash = "sha256-o2odrL0lRD4HvTL5JSCe1H5Gc4wwq08N5uPvoD6eQi0=";
    };
  };
}
