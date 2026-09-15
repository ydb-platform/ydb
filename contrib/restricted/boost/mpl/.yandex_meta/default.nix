self: super: with self; {
  boost_mpl = stdenv.mkDerivation rec {
    pname = "boost_mpl";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mpl";
      rev = "boost-${version}";
      hash = "sha256-j1BNEtXuoPQwLnXssZmyfGP+LXWxDDU2TtyGWXY+RlQ=";
    };
  };
}
