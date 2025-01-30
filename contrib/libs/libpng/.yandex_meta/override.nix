pkgs: attrs: with pkgs; rec {
  version = "1.6.45";

  src = fetchFromGitHub {
    owner = "pnggroup";
    repo = "libpng";
    rev = "v${version}";
    hash = "sha256-kO0f65oHhnwrdhCIe2XbyihODxKKKWsJsh+QJS2uBtc=";
  };

  # nixpkgs applies apng patch from sourceforge.net, which changes for every libpng version.
  # We apply a sligthly modified version of this patch via patches/apng.patch
  patches = [];
  postPatch = "";

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}
