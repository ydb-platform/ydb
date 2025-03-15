self: super: with self; {
  boost_xpressive = stdenv.mkDerivation rec {
    pname = "boost_xpressive";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "xpressive";
      rev = "boost-${version}";
      hash = "sha256-w9iw5hFB0m1GifDU7bGQ710UwvBfDXYaFDF3REH/ghI=";
    };
  };
}
