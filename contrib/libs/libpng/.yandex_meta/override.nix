pkgs: attrs: with pkgs; rec {
  version = "1.6.47";

  src = fetchFromGitHub {
    owner = "pnggroup";
    repo = "libpng";
    rev = "v${version}";
    hash = "sha256-u3t3PQ6Zgg987wbkzYTU4hzgEG14m724s7Q9rqFgc4U=";
  };

  # nixpkgs applies apng patch from sourceforge.net, which changes for every libpng version.
  # We apply a sligthly modified version of this patch via patches/apng.patch
  patches = [];
  postPatch = "";

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}
