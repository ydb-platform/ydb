self: super: with self; {
  boost_format = stdenv.mkDerivation rec {
    pname = "boost_format";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "format";
      rev = "boost-${version}";
      hash = "sha256-p7Qy3XhapFa9umYjYDlIAyD0ESK3P237sTGFaKHiyFI=";
    };
  };
}
