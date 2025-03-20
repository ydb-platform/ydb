self: super: with self; {
  boost_tti = stdenv.mkDerivation rec {
    pname = "boost_tti";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "tti";
      rev = "boost-${version}";
      hash = "sha256-o2kdpe1nEs6/GZhL8eRN+/Ec74fu6/MkH6I3Cy+Mpck=";
    };
  };
}
