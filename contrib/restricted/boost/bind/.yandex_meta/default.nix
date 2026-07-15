self: super: with self; {
  boost_bind = stdenv.mkDerivation rec {
    pname = "boost_bind";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "bind";
      rev = "boost-${version}";
      hash = "sha256-tAJNR99ZRouSwZZSupwvi/iDAv4Ax/h8p7trLtriIHU=";
    };
  };
}
