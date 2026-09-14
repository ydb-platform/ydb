pkgs: attrs: with pkgs; rec {
  version = "1.6.58";

  src = fetchFromGitHub {
    owner = "pnggroup";
    repo = "libpng";
    rev = "v${version}";
    hash = "sha256-JSdzPsfRBjnk8DB/fkOCkaiUJ9/neQ2myIK5xnQuSsM=";
  };

  # nixpkgs applies apng patch from sourceforge.net, which changes for every libpng version.
  # We apply a sligthly modified version of this patch via patches/apng.patch
  patches = [];
  postPatch = "";

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}
