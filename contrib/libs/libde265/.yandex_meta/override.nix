pkgs: attrs: with pkgs; rec {
  version = "1.0.16";

  src = fetchFromGitHub {
    owner = "strukturag";
    repo = "libde265";
    rev = "v${version}";
    hash = "sha256-4Y7tuVeDLoONU6/R/47QhGtzBiM9mtl4O++CN+KCUn4=";
  };

  patches = [];

  configureFlags = [
    "--disable-dec265"
    "--disable-sherlock265"
  ];

}
