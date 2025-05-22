pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.4.18";

  src = fetchurl {
    url = "mirror://gnu/m4/m4-${version}.tar.bz2";
    sha256 = "sha256-ZkDXawQ7xlgTnIkD4pPVl4MJvw9AgQcUZQXspwHmfPY=";
  };

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];
}
