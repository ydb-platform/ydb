self: super: with self; {
  boost_mpl = stdenv.mkDerivation rec {
    pname = "boost_mpl";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mpl";
      rev = "boost-${version}";
      hash = "sha256-mDqz+Y/LZv2XeyjnyhxCgogdzv/m4Dcnbylk6uyW5UE=";
    };
  };
}
