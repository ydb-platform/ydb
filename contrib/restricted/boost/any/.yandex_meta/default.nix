self: super: with self; {
  boost_any = stdenv.mkDerivation rec {
    pname = "boost_any";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "any";
      rev = "boost-${version}";
      hash = "sha256-ysJiQM+TB/VNdSmkwPXz/xLMG5uGIXNo9chxBSumlb4=";
    };
  };
}
