self: super: with self; {
  boost_mpl = stdenv.mkDerivation rec {
    pname = "boost_mpl";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mpl";
      rev = "boost-${version}";
      hash = "sha256-XzPGr2dRyhcqDaz4mDQ85kqC8BZ69XmYhIJ/8nv1Nq8=";
    };
  };
}
