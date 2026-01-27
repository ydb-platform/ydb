self: super: with self; {
  boost_parameter = stdenv.mkDerivation rec {
    pname = "boost_parameter";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "parameter";
      rev = "boost-${version}";
      hash = "sha256-QQ6E1HoXYBWZyAcXx6u7/XmWWd0DTws4oCudsBTjILo=";
    };
  };
}
