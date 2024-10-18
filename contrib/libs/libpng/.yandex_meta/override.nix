pkgs: attrs: with pkgs; rec {
  version = "1.6.40";

  src = fetchFromGitHub {
    owner = "pnggroup";
    repo = "libpng";
    rev = "v${version}";
    hash = "sha256-Rad7Y5Z9PUCipBTQcB7LEP8fIVTG3JsnMeknUkZ/rRg=";
  };

  # nixpkgs use a patch from libpng-apng project for getting A(nimated) PNG support.
  # While libpng-apng project patch is functionally equivalent to apng one,
  # the latter seems to provide somewhat better code.
  #
  # The sha256 checksum of the patch has to be updated upon libpng version update.
  patch_src = fetchurl {
    url = "mirror://sourceforge/apng/libpng-${version}-apng.patch.gz";
    hash = "sha256-esYjxN5hBg8Uue6AOAowulcB22U7rnQz2TjLM0+w+0w=";
  };

  postPatch = ''
    gunzip < ${patch_src} | patch -Np0
  '';
}
