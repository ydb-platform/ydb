pkgs: attrs: with pkgs; rec {
  version = "1.8.7";

  src = fetchFromGitHub {
    owner = "microsoft";
    repo = "mimalloc";
    rev = "v${version}";
    hash = "sha256-aSZFkHCfnCbrDtSRa4966N/Z+MI3xVzhF9sVbrkTvZ8=";
  };
}
