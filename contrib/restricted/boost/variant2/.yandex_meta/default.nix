self: super: with self; {
  boost_variant2 = stdenv.mkDerivation rec {
    pname = "boost_variant2";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant2";
      rev = "boost-${version}";
      hash = "sha256-3CoHanukdvX66QvsxSeqFL5CERf6BBLIpDjz3e+O8TA=";
    };
  };
}
