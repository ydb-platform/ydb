pkgs: attrs: with pkgs; rec {
  version = "1.1.9";

  src = fetchFromGitHub {
    owner = "WizardMac";
    repo = "ReadStat";
    rev = "v${version}";
    hash = "sha256-4lRJgZPB2gfaQ9fQKvDDpGhy1eDNT/nT1QmeZlCmCis=";
  };
}
