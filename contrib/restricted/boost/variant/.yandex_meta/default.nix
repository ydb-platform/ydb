self: super: with self; {
  boost_variant = stdenv.mkDerivation rec {
    pname = "boost_variant";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant";
      rev = "boost-${version}";
      hash = "sha256-jSs1iTUthmPSc4valr55BAJY+8VhDJYSo6ggCtgBLbA=";
    };
  };
}
