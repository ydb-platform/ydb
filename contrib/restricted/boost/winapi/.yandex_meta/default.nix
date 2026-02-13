self: super: with self; {
  boost_winapi = stdenv.mkDerivation rec {
    pname = "boost_winapi";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "winapi";
      rev = "boost-${version}";
      hash = "sha256-p1s5xMfZ075K1KJreHky6zrpZ+PbcP/1mZrHLvPmMrI=";
    };
  };
}
