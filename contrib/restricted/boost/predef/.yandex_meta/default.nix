self: super: with self; {
  boost_predef = stdenv.mkDerivation rec {
    pname = "boost_predef";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "predef";
      rev = "boost-${version}";
      hash = "sha256-CxSlsedkHXHs1LTG9aWPjS0jh2QeIXGalZDmGCt7Jf4=";
    };
  };
}
