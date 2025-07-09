self: super: with self; {
  boost_mp11 = stdenv.mkDerivation rec {
    pname = "boost_mp11";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mp11";
      rev = "boost-${version}";
      hash = "sha256-XtjcSv+pEfPCuOghx1EBum6n2IZMWEMBMk/hY38/Nto=";
    };
  };
}
