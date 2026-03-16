pkgs: attrs: with pkgs; with attrs; rec {
  version = "4.1.0";

  src = fetchFromGitHub {
    owner = "LLNL";
    repo = "sundials";
    rev = "v${version}";
    hash = "sha256-hwvcSd/zRSFmH0zdWKMsCOJu5+KQY//u76zj4PKSC5w";
  };
}
