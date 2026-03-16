self: super: with self; {
  libdivide = stdenv.mkDerivation rec {
    pname = "libdivide";
    version = "5.3.0";

    src = fetchFromGitHub {
      owner = "ridiculousfish";
      repo = "libdivide";
      rev = "v${version}";
      hash = "sha256-/wWvCSVvBD6AGHzZWzeGEVBLGCbGl9pOp6WGQheimR4=";
    };
  };
}