pkgs: attrs: with pkgs; with attrs; rec {
  version = "2025-01-30";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "78af4ffa26e15532847c1ba854ece7b3bacc6b1a";
    hash = "sha256-yx++/urCnaBt2QQsevSXjZ1aAHfBEznk5Dq3JscPyiQ=";
  };

  patches = [];
}
