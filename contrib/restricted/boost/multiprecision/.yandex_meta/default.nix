self: super: with self; {
  boost_multiprecision = stdenv.mkDerivation rec {
    pname = "boost_multiprecision";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "multiprecision";
      rev = "boost-${version}";
      hash = "sha256-zpND2FxbhYGiPrFkix+CocQU9Ty3DnQ4rCUTUAbKAjc=";
    };
  };
}
