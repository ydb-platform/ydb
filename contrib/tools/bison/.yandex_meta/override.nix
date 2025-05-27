pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.7.6";

  src = fetchurl {
    url = "mirror://gnu/${pname}/${pname}-${version}.tar.gz";
    sha256 = "sha256-adwLtG6o/DB9TKHgthyMNV6yB9Cwxp9PhGIyjnTXueo=";
  };

  patches = [];

  configureFlags = [
    "--disable-nls"
  ];
}
