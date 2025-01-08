pkgs: attrs: with pkgs; rec {
  version = "1.6.40";

  src = fetchFromGitHub {
    owner = "pnggroup";
    repo = "libpng";
    rev = "v${version}";
    hash = "sha256-Rad7Y5Z9PUCipBTQcB7LEP8fIVTG3JsnMeknUkZ/rRg=";
  };

  # nixpkgs applies apng patch from sourceforge.net, which changes for every libpng version.
  # We apply a sligthly modified version of this patch via patches/apng.patch
  patches = [];
  postPatch = "";

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}
