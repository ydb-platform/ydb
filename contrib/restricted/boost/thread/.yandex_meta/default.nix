self: super: with self; {
  boost_thread = stdenv.mkDerivation rec {
    pname = "boost_thread";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "thread";
      rev = "boost-${version}";
      hash = "sha256-mB7ws4tk+r1A9gVJFfIxRCN74HagCd08hvxvnNzaBPo=";
    };
  };
}
