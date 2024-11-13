pkgs: attrs: with pkgs; with attrs; rec {
  version = "2024-10-20";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "531aec7c52b66cd750a28a698f3c060f279b18b0";
    hash = "sha256-TNo8x/aiwZFkJfzRe8Eq6cZwnzzUBGPJEnzVWHGnWC8=";
  };

  patches = [];
}
