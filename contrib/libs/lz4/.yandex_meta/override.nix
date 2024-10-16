self: super: with self; rec {
  version = "1.10.0";

  src = fetchFromGitHub {
    owner = "lz4";
    repo = "lz4";
    rev = "v${version}";
    hash = "sha256-/dG1n59SKBaEBg72pAWltAtVmJ2cXxlFFhP+klrkTos=";
  };

  patches = [];
}
