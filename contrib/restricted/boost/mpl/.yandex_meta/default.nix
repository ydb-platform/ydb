self: super: with self; {
  boost_mpl = stdenv.mkDerivation rec {
    pname = "boost_mpl";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mpl";
      rev = "boost-${version}";
      hash = "sha256-m73tx+u/fb7MPg43Kn0tjW5/QjKtPJSTbQoTylF2Tdc=";
    };
  };
}
