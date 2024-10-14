pkgs: attrs: with pkgs; rec {
  version = "0.8";

  src = fetchFromGitHub {
    owner = "pantoniou";
    repo = "libfyaml";
    rev = "v${version}";
    hash = "sha256-b/jRKe23NIVSydoczI+Ax2VjBJLfAEwF8SW61vIDTwA=";
  };

  patches = [
    ./pr0064-remove-gpl-code.patch
    ./pr0067-add-basic-win-support.patch
    ./pr0073-add-document-destroy-hook.patch
    ./pr0078-add-node-style-setter.patch
    ./xxhash-static-link.patch
    ];
}
