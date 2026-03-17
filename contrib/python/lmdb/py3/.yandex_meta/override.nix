pkgs: attrs: with pkgs; with pkgs.python310.pkgs; with attrs; rec {
  pname = "lmdb";
  version = "1.7.5";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-8GBHUXYssJcFnVQSRExAV7lfOGx+2Vg2PPY/RT5RCNo=";
  };

  patches = [];

  propogatedBuildInputs = [];
}
