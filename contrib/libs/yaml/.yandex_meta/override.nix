pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.2.5";

  src = fetchFromGitHub {
    owner = "yaml";
    repo = "libyaml";
    rev = version;
    sha256 = "18zsnsxc53pans4a01cs4401a2cjk3qi098hi440pj4zijifgcsb";
  };

  patches = [];
}
