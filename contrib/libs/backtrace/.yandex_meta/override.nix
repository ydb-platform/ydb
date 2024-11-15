pkgs: attrs: with pkgs; with attrs; rec {
  version = "2024-10-25";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "d48f84034ce3e53e501d10593710d025cb1121db";
    hash = "sha256-suQJqNTh+dOI2nugASiRsWfMcjjJkTAKIpS5z9RIVUo=";
  };

  patches = [];
}
