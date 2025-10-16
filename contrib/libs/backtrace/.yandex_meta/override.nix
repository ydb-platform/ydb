pkgs: attrs: with pkgs; with attrs; rec {
  version = "2025-09-28";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "2f67a3abfd1947ddec71fc11965a9cf9191d45ad";
    hash = "sha256-TzIsZulPlPfah/SpZb72sAIPxTPGqg3VNf6jFlf56qI=";
  };

  patches = [];
}
