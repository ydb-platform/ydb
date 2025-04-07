pkgs: attrs: with pkgs; with attrs; rec {
  version = "2025-02-10";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "0034e33946824057b48c5e686a3aefc761b37384";
    hash = "sha256-JCut4afd8a2iXTdbjnyabdWX9JomcSh3qj/RScyVGSw=";
  };

  patches = [];
}
