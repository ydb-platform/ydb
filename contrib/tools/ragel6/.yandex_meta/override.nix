pkgs: attrs: {
  postConfigure = ''
    make -C ragel version.h
  '';
  enableParallelBuilding = true;
}
