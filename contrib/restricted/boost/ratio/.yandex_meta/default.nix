self: super: with self; {
  boost_ratio = stdenv.mkDerivation rec {
    pname = "boost_ratio";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "ratio";
      rev = "boost-${version}";
      hash = "sha256-qaNRgB9tuxUzgUYXBwXo2Wyz8LdNffuiMVAqdaQGZ6c=";
    };
  };
}
